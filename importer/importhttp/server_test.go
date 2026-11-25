package importhttp

import (
	"crypto/rand"
	"iter"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/require"
)

// testRandomEntries returns n entries with random values of valueBytes,
// which do not compress
func testRandomEntries(n, valueBytes int) []importer.Entry {
	out := testEntries(n)
	for i := range out {
		out[i].Value = make([]byte, valueBytes)
		_, _ = rand.Read(out[i].Value)
	}
	return out
}

// testServer starts a server of a sender of entries as KindSync. returned
// is set once every request that the sender's handler took has returned.
// Each request returns 100 ms after the sender, as a store that is slow to
// release a part would, so that a Close that does not wait for it returns
// first.
func testServer(t *testing.T, entries []importer.Entry) (s *Server, c importer.Source, returned *atomic.Bool) {
	t.Helper()
	h := testNewSender(t, map[importer.Kind]importer.Source{importer.KindSync: &testSource{entries: entries}})
	var inFlight atomic.Int64
	returned = new(atomic.Bool)
	s, err := Listen("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inFlight.Add(1)
		returned.Store(false)
		h.ServeHTTP(w, r)
		time.Sleep(100 * time.Millisecond)
		returned.Store(inFlight.Add(-1) == 0)
	}))
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	t.Cleanup(func() {
		_ = s.Close()
		require.ErrorIs(t, <-served, http.ErrServerClosed)
	})
	c, err = NewClient(s.URL(), testToken)
	require.NoError(t, err)
	return s, c, returned
}

// TestServerCloseBlockedRequest checks that Close stops a request whose
// sender is blocked on a reader that does not read, and returns after it
func TestServerCloseBlockedRequest(t *testing.T) {
	// Far more data than the connection's buffers
	s, c, returned := testServer(t, testRandomEntries(4000, 8<<10))
	s.closeTimeout = 100 * time.Millisecond

	_, entries, err := c.OpenStream(t.Context(), importer.KindSync, nil, nil, 0)
	require.NoError(t, err)
	next, stop := iter.Pull2(entries)
	defer stop()
	_, err, ok := next()
	require.True(t, ok)
	require.NoError(t, err)
	// Let the sender fill the connection's buffers and block on a write
	time.Sleep(500 * time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Close did not return while a part was in flight")
	}
	require.True(t, returned.Load(), "Close returned before the request")

	for err == nil {
		_, err, ok = next()
		require.True(t, ok, "the part ended whole after Close")
	}
	require.False(t, importer.IsPermanent(err), "a part cut off by Close is retried: %v", err)
}

// TestServerCloseStopsParts checks that Close stops a part whose reader
// keeps reading before the part's end, rather than waiting for the part
func TestServerCloseStopsParts(t *testing.T) {
	s, c, returned := testServer(t, testRandomEntries(4000, 8<<10))
	_, entries, err := c.OpenStream(t.Context(), importer.KindSync, nil, nil, 0)
	require.NoError(t, err)

	// The reader takes about 4 seconds for the whole part
	started := make(chan struct{})
	ended := make(chan error, 1)
	go func() {
		var n int
		for _, err := range entries {
			if err != nil {
				ended <- err
				return
			}
			if n++; n == 1 {
				close(started)
			}
			time.Sleep(time.Millisecond)
		}
		ended <- nil
	}()
	<-started

	start := time.Now()
	require.NoError(t, s.Close())
	require.Less(t, time.Since(start), 2*time.Second)
	require.True(t, returned.Load(), "Close returned before the request")
	err = <-ended
	require.Error(t, err, "the part ended whole")
	require.False(t, importer.IsPermanent(err))
}

// TestServerRefusesWhileClosing checks that a request that reaches the
// handler once Close has started is refused with 503, which a reader
// retries
func TestServerRefusesWhileClosing(t *testing.T) {
	s, _, _ := testServer(t, testEntries(1))
	require.NoError(t, s.Close())

	w := httptest.NewRecorder()
	s.server.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, PartPath+string(importer.KindSync), nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
