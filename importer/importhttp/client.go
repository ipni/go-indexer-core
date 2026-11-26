package importhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/klauspost/compress/zstd"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// maxErrorBytes is the most bytes of an error response's body that the
// client reads into its error
const maxErrorBytes = 4 << 10

type clientConfig struct {
	httpClient *http.Client
}

// ClientOption sets an option of NewClient
type ClientOption func(*clientConfig)

// WithHTTPClient sets the HTTP client that requests parts,
// http.DefaultClient by default. A part's response streams for as long as
// the part takes, so the client must have no Timeout.
func WithHTTPClient(c *http.Client) ClientOption {
	return func(cfg *clientConfig) {
		if c != nil {
			cfg.httpClient = c
		}
	}
}

// client is an importer.Source whose parts come from a sender over HTTP
type client struct {
	baseURL    *url.URL
	httpClient *http.Client
	token      string
}

// NewClient returns an importer.Source that reads the parts of a sender
// (see NewSender) at baseURL: each part of kind is a request to baseURL +
// PartPath + kind, carrying token as "Authorization: Bearer <token>". The
// token must not be empty. One base URL serves every kind; a proxy in front may
// route each kind's path to a different sender.
//
// The sender picks the size of open parts, so OpenStream ignores its size
// hint. Every request asks for a body compressed with zstd, and the client
// reads a plain body too. OpenStream returns once the sender has sent the
// part's headers, and the entries read the part's body as the caller ranges
// over them. The
// caller ranges over the entries once, which closes the response when the
// loop ends, early or not.
//
// A response that refuses the part is an error with the sender's message:
// permanent for a request the sender cannot serve (400, 404, 405) or a
// permanent refusal of its source (422), and transient for any other
// status. A refused token (401) is transient too: the sender's token may be
// changed back while the import retries. The entries end as the body's trailer says (see readBody).
func NewClient(baseURL, token string, opts ...ClientOption) (importer.Source, error) {
	if token == "" {
		return nil, errors.New("the import client needs a token")
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("cannot parse the import base URL: %w", err)
	}
	cfg := clientConfig{httpClient: http.DefaultClient}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &client{baseURL: u, httpClient: cfg.httpClient, token: token}, nil
}

// OpenStream implements importer.Source. It records the measures of the
// client in metrics.ImportViews: a request starts here and ends when the
// entries end, or when OpenStream fails.
func (c *client) OpenStream(
	ctx context.Context,
	kind importer.Kind,
	start, end []byte,
	_ uint64,
) (
	[]byte,
	iter.Seq2[importer.Entry, error],
	error,
) {
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.ImportKind, string(kind)))
	stats.Record(ctx, metrics.ImportClientRequestsStarted.M(1))
	part, result, err := c.openPart(ctx, kind, start, end)
	if err != nil {
		recordResult(ctx, metrics.ImportClientRequestsEnded, result)
		return nil, nil, err
	}
	return part.end, readPart(ctx, part), nil
}

// partResponse is a part's response that the client has accepted
type partResponse struct {
	// end is the part's end, nil when the part runs to the end of the data
	end        []byte
	body       io.ReadCloser
	compressed bool
}

// openPart requests a part and checks the response's headers. On an error
// it returns the request's result.
func (c *client) openPart(ctx context.Context, kind importer.Kind, start, end []byte) (partResponse, string, error) {
	body, err := json.Marshal(partRequest{Start: start, End: end})
	if err != nil {
		return partResponse{}, resultFailed, importer.PermanentError(err)
	}
	// A kind's name is a path below PartPath, such as "ds/sync"
	u := c.baseURL.JoinPath(PartPath, string(kind))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return partResponse{}, resultFailed, importer.PermanentError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept-Encoding", zstdEncoding)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return partResponse{}, failedOrAborted(ctx), err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		result := resultFailed
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			result = resultRefused
		}
		return partResponse{}, result, responseError(resp)
	}

	part := partResponse{body: resp.Body}
	switch coding := resp.Header.Get("Content-Encoding"); coding {
	case "":
	case zstdEncoding:
		part.compressed = true
	default:
		resp.Body.Close()
		return partResponse{}, resultFailed, importer.PermanentError(fmt.Errorf("the sender sent a body in content coding %q", coding))
	}
	if v, ok := resp.Header[endHeader]; ok {
		if part.end, err = base64.RawURLEncoding.DecodeString(v[0]); err != nil {
			resp.Body.Close()
			return partResponse{}, resultFailed, importer.PermanentError(fmt.Errorf("the sender sent an end that does not decode: %w", err))
		}
	}
	return part, "", nil
}

// readPart returns the entries of part's body. When they end, it closes the
// body and records the bytes and entries received and the request's
// result, with the tags of ctx.
func readPart(ctx context.Context, part partResponse) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		wire := &countingReader{r: part.body}
		plain := &countingReader{}
		var count int64
		result := resultOK
		defer func() {
			part.body.Close()
			stats.Record(ctx, metrics.ImportReceivedBytes.M(plain.n), metrics.ImportReceivedWireBytes.M(wire.n), metrics.ImportReceivedEntries.M(count))
			recordResult(ctx, metrics.ImportClientRequestsEnded, result)
		}()

		var body io.Reader = wire
		if part.compressed {
			d, err := newDecoder(body)
			if err != nil {
				result = resultFailed
				yield(importer.Entry{}, err)
				return
			}
			defer d.Close()
			body = d
		}
		plain.r = body
		for e, err := range readBody(plain) {
			var partial *importer.PartialError
			switch {
			case err == nil:
				count++
			case errors.As(err, &partial):
			case errors.Is(err, zstd.ErrWindowSizeExceeded):
				// The sender's encoder needs more memory than the client
				// gives a decoder, and does for every retry
				err = importer.PermanentError(fmt.Errorf("the sender compressed the part with a window over the client's limit of %d bytes: %w", zstdMaxWindowBytes, err))
				result = resultFailed
			default:
				result = failedOrAborted(ctx)
			}
			if !yield(e, err) {
				if err == nil {
					result = resultAborted
				}
				return
			}
		}
	}
}

// failedOrAborted returns the result of a request that failed: aborted when
// ctx is done, which fails the request, and failed otherwise
func failedOrAborted(ctx context.Context) string {
	if ctx.Err() != nil {
		return resultAborted
	}
	return resultFailed
}

// responseError returns the error of a response that refuses a part, with
// the start of its body as the message
func responseError(resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	err := fmt.Errorf("the sender answered %s: %s", resp.Status, bytes.TrimSpace(msg))
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusUnprocessableEntity:
		return importer.PermanentError(err)
	default:
		return err
	}
}
