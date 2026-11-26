package importhttp

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/klauspost/compress/zstd"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("importhttp")

// PartPath is the path of a kind's parts, followed by the kind
const PartPath = "/import/v1/kinds/"

// endHeader is the response header with the part's end, unpadded base64url.
// It is absent when the part runs to the end of the kind's data.
const endHeader = "X-End"

// partRequest is the JSON body of a part request. A nil End opens the
// part: the sender picks its end. Start and End are base64 in JSON, and an
// empty one is left out, so it reads back as nil.
type partRequest struct {
	Start []byte `json:"start,omitempty"`
	End   []byte `json:"end,omitempty"`
}

// maxRequestBytes is the most bytes of a part request's body: two cursors
const maxRequestBytes = 64 << 10

// writeBytes is about how many bytes of a body the sender buffers before it
// writes them to the response. The response's own buffer is a few KiB, so
// data reaches the reader in writes of this size without flushes.
const writeBytes = 1 << 20

type senderConfig struct {
	partBytes uint64
}

// SenderOption sets an option of NewSender
type SenderOption func(*senderConfig)

// WithPartBytes sets the size hint that the sender opens parts with: about
// how many bytes of wire keys and values an open part holds. The default,
// 0, leaves it to each source's own default.
func WithPartBytes(n uint64) SenderOption {
	return func(c *senderConfig) {
		c.partBytes = n
	}
}

// NewSender returns an http.Handler that serves the parts of sources, the
// Source of each kind it serves, at POST PartPath + kind, to requests that
// carry token as "Authorization: Bearer <token>". The token must not be
// empty. The request body
// is a JSON object with the part's start and end cursors, base64, each
// optional; without an end the part is open. The sender opens a part with
// the size hint of WithPartBytes.
//
// It answers a request to PartPath + kind with:
//   - 401 for a request without the token, before anything else;
//   - 405 for a method other than POST;
//   - 404 for a kind it does not serve;
//   - 400 for a request it cannot read;
//   - 422 with the error's message when the source refuses the part with a
//     permanent error;
//   - 503 with the error's message when opening the part fails otherwise;
//   - 200 with the part's end in the X-End header, absent when the part
//     runs to the end of the kind's data, and the part's body (see
//     writeBody).
//
// It sends the headers before any data, and then writes the body in writes
// of about writeBytes, compressed with zstd when the request's
// Accept-Encoding accepts it. A response streams for as long
// as the part takes: the sender clears any write timeout of the server for
// it. A reader that goes away cancels the request's context, which stops
// the source, and fails the next write.
//
// It records the measures of metrics.ImportViews.
func NewSender(sources map[importer.Kind]importer.Source, token string, opts ...SenderOption) (http.Handler, error) {
	if token == "" {
		return nil, errors.New("the import sender needs a token")
	}
	var cfg senderConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	want := []byte("Bearer " + token)
	mux := http.NewServeMux()
	// A kind's name is a path below PartPath, such as "ds/sync"
	mux.HandleFunc(PartPath+"{kind...}", func(w http.ResponseWriter, r *http.Request) {
		kind := importer.Kind(r.PathValue("kind"))
		src, served := sources[kind]
		// A kind that is not served is left untagged, so that requests
		// cannot add series
		ctx := r.Context()
		if served {
			ctx, _ = tag.New(ctx, tag.Upsert(metrics.ImportKind, string(kind)))
		}
		stats.Record(ctx, metrics.ImportSenderRequestsStarted.M(1))

		var result string
		switch {
		case subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1:
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "the request does not carry the import token", http.StatusUnauthorized)
			result = resultRefused
		case r.Method != http.MethodPost:
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "a part is requested with POST", http.StatusMethodNotAllowed)
			result = resultRefused
		case !served:
			http.Error(w, fmt.Sprintf("kind %q is not served here", kind), http.StatusNotFound)
			result = resultRefused
		default:
			result = servePart(ctx, w, r, cfg, kind, src)
		}
		recordResult(ctx, metrics.ImportSenderRequestsEnded, result)
	})
	return mux, nil
}

// servePart opens the part that r asks for, writes it to w and returns the
// request's result. ctx is the request's context with the kind's tag.
func servePart(ctx context.Context, w http.ResponseWriter, r *http.Request, cfg senderConfig, kind importer.Kind, src importer.Source) string {
	var req partRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes)).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("cannot read the part request: %v", err), http.StatusBadRequest)
		return resultRefused
	}

	partEnd, entries, err := src.OpenStream(r.Context(), kind, req.Start, req.End, cfg.partBytes)
	if err != nil {
		if importer.IsPermanent(err) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return resultRefused
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return resultFailed
	}

	if partEnd != nil {
		w.Header().Set(endHeader, base64.RawURLEncoding.EncodeToString(partEnd))
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Vary", "Accept-Encoding")
	compress := acceptsZstd(r.Header.Values("Accept-Encoding"))
	if compress {
		w.Header().Set("Content-Encoding", zstdEncoding)
	}
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	// The response has no deadline, however long the part takes, and the
	// headers go out before the source reads the first entry
	err = rc.SetWriteDeadline(time.Time{})
	if err == nil || errors.Is(err, http.ErrNotSupported) {
		err = rc.Flush()
	}
	var status byte
	if err == nil {
		status, err = writeResponseBody(ctx, w, entries, compress)
	}
	switch {
	case err != nil:
		log.Debugw("Import part reader is gone", "kind", kind, "err", err)
		return resultAborted
	case status == statusTransient || status == statusPermanent:
		return resultFailed
	default:
		return resultOK
	}
}

// writeResponseBody writes the body of a part with entries to w, compressed
// with zstd when compress is set, and returns its trailer's status. The
// encoder sends its blocks as they fill, so a compressed body streams as a
// plain one does. When the body ends it records the bytes and entries
// sent, with the tags of ctx.
func writeResponseBody(ctx context.Context, w io.Writer, entries iter.Seq2[importer.Entry, error], compress bool) (status byte, err error) {
	wire := &countingWriter{w: w}
	w = wire
	var enc *zstd.Encoder
	if compress {
		var err error
		if enc, err = newEncoder(w); err != nil {
			return 0, err
		}
		w = enc
	}
	plain := &countingWriter{w: w}
	bw := bufio.NewWriterSize(plain, writeBytes)
	status, count, err := writeBody(bw, entries)
	defer func() {
		stats.Record(ctx, metrics.ImportSentBytes.M(plain.n), metrics.ImportSentWireBytes.M(wire.n), metrics.ImportSentEntries.M(int64(count)))
	}()
	if err == nil {
		err = bw.Flush()
	}
	if err == nil && enc != nil {
		// Closing the encoder ends the frame
		err = enc.Close()
	}
	return status, err
}

// acceptsZstd reports whether the values of an Accept-Encoding header accept
// zstd: one of them lists it without a quality of 0
func acceptsZstd(values []string) bool {
	for _, v := range values {
		for coding := range strings.SplitSeq(v, ",") {
			name, params, _ := strings.Cut(coding, ";")
			if !strings.EqualFold(strings.TrimSpace(name), zstdEncoding) {
				continue
			}
			q, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q=")
			if !ok || strings.Trim(q, "0.") != "" {
				return true
			}
		}
	}
	return false
}
