package importhttp

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// testRegisterImportViews registers the import views for the test. Their
// data starts empty and is dropped when the test ends.
func testRegisterImportViews(t *testing.T) {
	require.NoError(t, view.Register(metrics.ImportViews...))
	t.Cleanup(func() { view.Unregister(metrics.ImportViews...) })
}

// testViewValue returns the value of the row of a view whose tags are
// tags, and 0 when there is no such row
func testViewValue(t *testing.T, m interface{ Name() string }, tags ...tag.Tag) float64 {
	t.Helper()
	rows, err := view.RetrieveData(m.Name())
	require.NoError(t, err)
	for _, row := range rows {
		if !slices.Equal(row.Tags, tags) {
			continue
		}
		switch data := row.Data.(type) {
		case *view.SumData:
			return data.Value
		default:
			require.FailNow(t, "unexpected view data", "%T", data)
		}
	}
	return 0
}

// testViewSum returns the sum of a counter's rows
func testViewSum(t *testing.T, m interface{ Name() string }) float64 {
	t.Helper()
	rows, err := view.RetrieveData(m.Name())
	require.NoError(t, err)
	var sum float64
	for _, row := range rows {
		sum += row.Data.(*view.SumData).Value
	}
	return sum
}

// testInFlight returns the requests in flight: those started less those
// ended
func testInFlight(t *testing.T, started, ended interface{ Name() string }) float64 {
	t.Helper()
	return testViewSum(t, started) - testViewSum(t, ended)
}

// testResult returns the requests of kind with result in measure; an empty
// kind is a kind that is not served
func testResult(t *testing.T, measure interface{ Name() string }, kind importer.Kind, result string) float64 {
	t.Helper()
	tags := []tag.Tag{{Key: metrics.ImportResult, Value: result}}
	if kind != "" {
		// Rows list their tags in the view's order: kind, then result
		tags = append([]tag.Tag{{Key: metrics.ImportKind, Value: string(kind)}}, tags...)
	}
	return testViewValue(t, measure, tags...)
}

func testKindTag(kind importer.Kind) tag.Tag {
	return tag.Tag{Key: metrics.ImportKind, Value: string(kind)}
}

// testFailingWriter is a ResponseWriter whose body writes fail, as when the
// reader has gone away
type testFailingWriter struct {
	*httptest.ResponseRecorder
}

func (testFailingWriter) Write([]byte) (int, error) { return 0, errors.New("reader gone") }

// testGatedSource is a Source of empty parts that each wait in OpenStream:
// it sends on started, then waits for release
type testGatedSource struct {
	started, release chan struct{}
}

func (s testGatedSource) OpenStream(context.Context, importer.Kind, []byte, []byte, uint64) ([]byte, iter.Seq2[importer.Entry, error], error) {
	s.started <- struct{}{}
	<-s.release
	return nil, func(func(importer.Entry, error) bool) {}, nil
}

func TestSenderMetrics(t *testing.T) {
	testRegisterImportViews(t)
	gated := testGatedSource{started: make(chan struct{}), release: make(chan struct{})}
	sources := map[importer.Kind]importer.Source{
		importer.KindSync:         &testSource{entries: testRecordEntries(500)},
		importer.KindRecords:      &testSource{entries: testEntries(5), endErr: errors.New("boom")},
		importer.KindAdProcessed:  &testSource{openErr: importer.PermanentError(errors.New("frozen"))},
		importer.KindAdSkipReason: &testSource{openErr: errors.New("busy")},
		importer.KindMultihashes:  gated,
	}
	h := testNewSender(t, sources)
	require.Zero(t, testInFlight(t, metrics.ImportSenderRequestsStarted, metrics.ImportSenderRequestsEnded))

	t.Run("ok", func(t *testing.T) {
		plain := testPost(t, h, importer.KindSync, partRequest{})
		require.Equal(t, http.StatusOK, plain.Code)
		require.EqualValues(t, 1, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindSync, resultOK))
		require.EqualValues(t, plain.Body.Len(), testViewValue(t, metrics.ImportSentBytes, testKindTag(importer.KindSync)))
		require.EqualValues(t, plain.Body.Len(), testViewValue(t, metrics.ImportSentWireBytes, testKindTag(importer.KindSync)))
		require.EqualValues(t, 500, testViewValue(t, metrics.ImportSentEntries, testKindTag(importer.KindSync)))

		// A compressed body counts its plain bytes and its compressed bytes
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, PartPath+string(importer.KindSync), bytes.NewReader([]byte("{}")))
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set("Accept-Encoding", zstdEncoding)
		compressed := httptest.NewRecorder()
		h.ServeHTTP(compressed, r)
		require.Equal(t, zstdEncoding, compressed.Header().Get("Content-Encoding"))
		require.EqualValues(t, 2, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindSync, resultOK))
		require.EqualValues(t, 2*plain.Body.Len(), testViewValue(t, metrics.ImportSentBytes, testKindTag(importer.KindSync)))
		require.EqualValues(t, plain.Body.Len()+compressed.Body.Len(), testViewValue(t, metrics.ImportSentWireBytes, testKindTag(importer.KindSync)))
		require.EqualValues(t, 1000, testViewValue(t, metrics.ImportSentEntries, testKindTag(importer.KindSync)))
	})

	t.Run("refused", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, PartPath+string(importer.KindSync), nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusUnauthorized, w.Code)

		r = httptest.NewRequest(http.MethodGet, PartPath+string(importer.KindSync), nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
		require.Equal(t, http.MethodPost, w.Header().Get("Allow"))

		require.Equal(t, http.StatusBadRequest, testPost(t, h, importer.KindSync, "not an object").Code)
		require.EqualValues(t, 3, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindSync, resultRefused))

		require.Equal(t, http.StatusUnprocessableEntity, testPost(t, h, importer.KindAdProcessed, partRequest{}).Code)
		require.EqualValues(t, 1, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindAdProcessed, resultRefused))

		// A kind that is not served is counted without a kind
		require.Equal(t, http.StatusNotFound, testPost(t, h, "nope", partRequest{}).Code)
		require.EqualValues(t, 1, testResult(t, metrics.ImportSenderRequestsEnded, "", resultRefused))
	})

	t.Run("failed", func(t *testing.T) {
		require.Equal(t, http.StatusServiceUnavailable, testPost(t, h, importer.KindAdSkipReason, partRequest{}).Code)
		require.EqualValues(t, 1, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindAdSkipReason, resultFailed))

		require.Equal(t, http.StatusOK, testPost(t, h, importer.KindRecords, partRequest{}).Code)
		require.EqualValues(t, 1, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindRecords, resultFailed))
		require.EqualValues(t, 5, testViewValue(t, metrics.ImportSentEntries, testKindTag(importer.KindRecords)), "the entries before the error")
	})

	t.Run("aborted", func(t *testing.T) {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, PartPath+string(importer.KindSync), bytes.NewReader([]byte("{}")))
		r.Header.Set("Authorization", "Bearer "+testToken)
		h.ServeHTTP(testFailingWriter{httptest.NewRecorder()}, r)
		require.EqualValues(t, 1, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindSync, resultAborted))
	})

	t.Run("in flight", func(t *testing.T) {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, PartPath+string(importer.KindMultihashes), bytes.NewReader([]byte("{}")))
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.ServeHTTP(w, r)
		}()
		<-gated.started
		require.EqualValues(t, 1, testInFlight(t, metrics.ImportSenderRequestsStarted, metrics.ImportSenderRequestsEnded))
		close(gated.release)
		<-done
		require.Equal(t, http.StatusOK, w.Code)
		require.Zero(t, testInFlight(t, metrics.ImportSenderRequestsStarted, metrics.ImportSenderRequestsEnded))
		require.EqualValues(t, 1, testResult(t, metrics.ImportSenderRequestsEnded, importer.KindMultihashes, resultOK))
	})
}

func TestClientMetrics(t *testing.T) {
	testRegisterImportViews(t)
	entries := testRecordEntries(500)
	sources := map[importer.Kind]importer.Source{
		importer.KindSync:         &testSource{entries: entries},
		importer.KindRecords:      &testSource{entries: testEntries(5), endErr: errors.New("boom")},
		importer.KindAdSkipReason: &testSource{openErr: errors.New("busy")},
	}
	srv := httptest.NewServer(testNewSender(t, sources))
	defer srv.Close()
	c, err := NewClient(srv.URL, testToken, WithHTTPClient(srv.Client()))
	require.NoError(t, err)
	ctx := t.Context()
	require.Zero(t, testInFlight(t, metrics.ImportClientRequestsStarted, metrics.ImportClientRequestsEnded))
	clientResult := func(kind importer.Kind, result string) float64 {
		return testResult(t, metrics.ImportClientRequestsEnded, kind, result)
	}

	t.Run("ok", func(t *testing.T) {
		_, seq, err := c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.NoError(t, err)
		require.EqualValues(t, 1, testInFlight(t, metrics.ImportClientRequestsStarted, metrics.ImportClientRequestsEnded), "until the entries end")
		got, err := testReadAll(seq)
		require.NoError(t, err)
		require.Len(t, got, len(entries))
		require.Zero(t, testInFlight(t, metrics.ImportClientRequestsStarted, metrics.ImportClientRequestsEnded))
		require.EqualValues(t, 1, clientResult(importer.KindSync, resultOK))

		// The client receives what the sender sent, compressed on the wire
		kindTag := testKindTag(importer.KindSync)
		require.EqualValues(t, len(entries), testViewValue(t, metrics.ImportReceivedEntries, kindTag))
		require.Equal(t, testViewValue(t, metrics.ImportSentBytes, kindTag), testViewValue(t, metrics.ImportReceivedBytes, kindTag))
		require.Equal(t, testViewValue(t, metrics.ImportSentWireBytes, kindTag), testViewValue(t, metrics.ImportReceivedWireBytes, kindTag))
		require.Less(t, testViewValue(t, metrics.ImportReceivedWireBytes, kindTag), testViewValue(t, metrics.ImportReceivedBytes, kindTag))
	})

	t.Run("refused", func(t *testing.T) {
		_, _, err := c.OpenStream(ctx, importer.KindProviders, nil, nil, 0)
		require.Error(t, err)
		require.EqualValues(t, 1, clientResult(importer.KindProviders, resultRefused))

		wrong, err := NewClient(srv.URL, "guess", WithHTTPClient(srv.Client()))
		require.NoError(t, err)
		_, _, err = wrong.OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.Error(t, err)
		require.EqualValues(t, 1, clientResult(importer.KindSync, resultRefused))
	})

	t.Run("failed", func(t *testing.T) {
		_, _, err := c.OpenStream(ctx, importer.KindAdSkipReason, nil, nil, 0)
		require.Error(t, err)
		require.EqualValues(t, 1, clientResult(importer.KindAdSkipReason, resultFailed))

		_, seq, err := c.OpenStream(ctx, importer.KindRecords, nil, nil, 0)
		require.NoError(t, err)
		_, err = testReadAll(seq)
		require.Error(t, err)
		require.EqualValues(t, 1, clientResult(importer.KindRecords, resultFailed))
		require.EqualValues(t, 5, testViewValue(t, metrics.ImportReceivedEntries, testKindTag(importer.KindRecords)), "the entries before the error")

		gone := httptest.NewServer(http.NotFoundHandler())
		gone.Close()
		unreachable, err := NewClient(gone.URL, testToken)
		require.NoError(t, err)
		_, _, err = unreachable.OpenStream(ctx, importer.KindContextMap, nil, nil, 0)
		require.Error(t, err)
		require.EqualValues(t, 1, clientResult(importer.KindContextMap, resultFailed))
		require.Zero(t, testInFlight(t, metrics.ImportClientRequestsStarted, metrics.ImportClientRequestsEnded))
	})

	t.Run("aborted", func(t *testing.T) {
		_, seq, err := c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.NoError(t, err)
		for range seq {
			break
		}
		require.EqualValues(t, 1, clientResult(importer.KindSync, resultAborted))

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err = c.OpenStream(cancelled, importer.KindMultihashes, nil, nil, 0)
		require.Error(t, err)
		require.EqualValues(t, 1, clientResult(importer.KindMultihashes, resultAborted))
		require.Zero(t, testInFlight(t, metrics.ImportClientRequestsStarted, metrics.ImportClientRequestsEnded))
	})
}
