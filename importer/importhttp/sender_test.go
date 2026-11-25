package importhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/require"
)

// testSource is a Source of one kind's entries in memory. An open part is
// every entry from start, ending at end when it is set, and a closed part
// ends at its end. openErr fails OpenStream, and endErr ends the entries.
type testSource struct {
	entries []importer.Entry
	end     []byte
	openErr error
	endErr  error
	// sizeHint is the size hint of the last part opened
	sizeHint uint64
	// delay is how long the source waits before each entry
	delay time.Duration
	// opened receives the context of each part that is opened
	opened chan context.Context
}

func (s *testSource) OpenStream(ctx context.Context, _ importer.Kind, start, end []byte, sizeHint uint64) ([]byte, iter.Seq2[importer.Entry, error], error) {
	s.sizeHint = sizeHint
	if s.openErr != nil {
		return nil, nil, s.openErr
	}
	if s.opened != nil {
		s.opened <- ctx
	}
	if end == nil {
		end = s.end
	}
	return end, func(yield func(importer.Entry, error) bool) {
		for _, e := range s.entries {
			if bytes.Compare(e.Key, start) < 0 || (end != nil && bytes.Compare(e.Key, end) >= 0) {
				continue
			}
			time.Sleep(s.delay)
			if err := ctx.Err(); err != nil {
				yield(importer.Entry{}, err)
				return
			}
			if !yield(e, nil) {
				return
			}
		}
		if s.endErr != nil {
			yield(importer.Entry{}, s.endErr)
		}
	}, nil
}

// testToken is the token of the tests' senders and clients
const testToken = "test-token"

// testNewSender returns a sender of sources with testToken
func testNewSender(t *testing.T, sources map[importer.Kind]importer.Source, opts ...SenderOption) http.Handler {
	t.Helper()
	h, err := NewSender(sources, testToken, opts...)
	require.NoError(t, err)
	return h
}

// testPost sends a part request for kind with req to h
func testPost(t *testing.T, h http.Handler, kind importer.Kind, req any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, PartPath+string(kind), bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestSenderParts(t *testing.T) {
	entries := testEntries(50)
	src := &testSource{entries: entries}
	h := testNewSender(t, map[importer.Kind]importer.Source{importer.KindSync: src}, WithPartBytes(100))

	// An open part to the end of the data has no end header
	w := testPost(t, h, importer.KindSync, partRequest{})
	require.Equal(t, http.StatusOK, w.Code)
	_, ok := w.Result().Header[endHeader]
	require.False(t, ok)
	got, err := testReadAll(readBody(w.Body))
	require.NoError(t, err)
	require.Equal(t, entries, got)

	// An open part that the source ends
	src.end = entries[30].Key
	w = testPost(t, h, importer.KindSync, partRequest{Start: entries[10].Key})
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, base64.RawURLEncoding.EncodeToString(entries[30].Key), w.Header().Get(endHeader))
	got, err = testReadAll(readBody(w.Body))
	require.NoError(t, err)
	require.Equal(t, entries[10:30], got)
	require.EqualValues(t, 100, src.sizeHint, "the sender's part size reaches the source")

	// A closed part
	w = testPost(t, h, importer.KindSync, partRequest{Start: entries[5].Key, End: entries[8].Key})
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, base64.RawURLEncoding.EncodeToString(entries[8].Key), w.Header().Get(endHeader))
	got, err = testReadAll(readBody(w.Body))
	require.NoError(t, err)
	require.Equal(t, entries[5:8], got)
}

func TestSenderEndsInTrailer(t *testing.T) {
	entries := testEntries(5)
	for _, tc := range []struct {
		name  string
		err   error
		check func(t *testing.T, err error)
	}{
		{"partial", &importer.PartialError{Resume: []byte("r")}, func(t *testing.T, err error) {
			var pe *importer.PartialError
			require.ErrorAs(t, err, &pe)
			require.Equal(t, []byte("r"), pe.Resume)
		}},
		{"transient", errors.New("boom"), func(t *testing.T, err error) {
			require.ErrorContains(t, err, "boom")
			require.False(t, importer.IsPermanent(err))
		}},
		{"permanent", importer.PermanentError(errors.New("boom")), func(t *testing.T, err error) {
			require.True(t, importer.IsPermanent(err))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testNewSender(t, map[importer.Kind]importer.Source{importer.KindSync: &testSource{entries: entries, endErr: tc.err}})
			w := testPost(t, h, importer.KindSync, partRequest{})
			require.Equal(t, http.StatusOK, w.Code)
			got, err := testReadAll(readBody(w.Body))
			require.Equal(t, entries, got)
			tc.check(t, err)
		})
	}
}

func TestSenderRefuses(t *testing.T) {
	sources := map[importer.Kind]importer.Source{
		importer.KindSync:         &testSource{},
		importer.KindAdProcessed:  &testSource{openErr: importer.PermanentError(errors.New("frozen"))},
		importer.KindAdSkipReason: &testSource{openErr: errors.New("busy")},
	}
	h := testNewSender(t, sources)

	for _, tc := range []struct {
		name   string
		kind   importer.Kind
		req    any
		status int
		msg    string
	}{
		{"unknown kind", importer.KindRecords, partRequest{}, http.StatusNotFound, "not served"},
		{"bad request", importer.KindSync, "not an object", http.StatusBadRequest, "cannot read"},
		{"permanent", importer.KindAdProcessed, partRequest{}, http.StatusUnprocessableEntity, "frozen"},
		{"transient", importer.KindAdSkipReason, partRequest{}, http.StatusServiceUnavailable, "busy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := testPost(t, h, tc.kind, tc.req)
			require.Equal(t, tc.status, w.Code)
			require.Contains(t, w.Body.String(), tc.msg)
		})
	}

	t.Run("method", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, PartPath+string(importer.KindSync), nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
	t.Run("request too large", func(t *testing.T) {
		w := testPost(t, h, importer.KindSync, partRequest{Start: make([]byte, maxRequestBytes)})
		require.Equal(t, http.StatusBadRequest, w.Code)
	})
}

// TestSenderReaderGone checks that a reader that goes away part way
// through a part cancels the source's context
func TestSenderReaderGone(t *testing.T) {
	// Entries far larger than the client reads, so that the sender blocks
	// on writes once the reader stops reading
	entries := make([]importer.Entry, 2000)
	for i := range entries {
		entries[i] = importer.Entry{Key: []byte{byte(i >> 8), byte(i)}, Value: bytes.Repeat([]byte{1}, 4<<10)}
	}
	src := &testSource{entries: entries, opened: make(chan context.Context, 1)}
	srv := httptest.NewServer(testNewSender(t, map[importer.Kind]importer.Source{importer.KindSync: src}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+PartPath+string(importer.KindSync), strings.NewReader("{}"))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	srcCtx := <-src.opened

	// Read a little, then go away
	_, err = resp.Body.Read(make([]byte, 100))
	require.NoError(t, err)
	cancel()
	_ = resp.Body.Close()

	select {
	case <-srcCtx.Done():
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the part's context was not cancelled after the reader went away")
	}
}

// TestSenderNoWriteTimeout checks that a part streams for longer than the
// server's write timeout
func TestSenderNoWriteTimeout(t *testing.T) {
	entries := testEntries(10)
	src := &testSource{entries: entries, delay: 50 * time.Millisecond}
	srv := httptest.NewUnstartedServer(testNewSender(t, map[importer.Kind]importer.Source{importer.KindSync: src}))
	srv.Config.WriteTimeout = 100 * time.Millisecond
	srv.Start()
	defer srv.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+PartPath+string(importer.KindSync), strings.NewReader("{}"))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	got, err := testReadAll(readBody(resp.Body))
	require.NoError(t, err)
	require.Equal(t, entries, got)
}
