package importhttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/ipni/go-indexer-core/importer/storeutil"
	"github.com/stretchr/testify/require"
)

// testSliceSource is a Source of one kind's sorted entries in memory, with
// open parts planned by storeutil.PlanPart and every part stopped by
// storeutil.StreamPart after twice the size hint, as a store's are
type testSliceSource struct {
	entries []importer.Entry
}

// from yields the entries in [start, end); a nil end is the end of the data
func (s *testSliceSource) from(start, end []byte) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		i, _ := slices.BinarySearchFunc(s.entries, start, func(e importer.Entry, k []byte) int { return bytes.Compare(e.Key, k) })
		for _, e := range s.entries[i:] {
			if end != nil && bytes.Compare(e.Key, end) >= 0 {
				return
			}
			if !yield(e, nil) {
				return
			}
		}
	}
}

func (s *testSliceSource) OpenStream(ctx context.Context, _ importer.Kind, start, end []byte, sizeHint uint64) ([]byte, iter.Seq2[importer.Entry, error], error) {
	if sizeHint == 0 {
		sizeHint = 1 << 10
	}
	if end != nil {
		return end, storeutil.StreamPart(ctx, nil, s.from(start, end), 2*sizeHint), nil
	}
	partEnd, head, next, err := storeutil.PlanPart(func(from []byte) iter.Seq2[importer.Entry, error] {
		return s.from(from, nil)
	}, start, sizeHint, func(key []byte) int { return len(key) })
	if err != nil {
		return nil, nil, err
	}
	var rest iter.Seq2[importer.Entry, error]
	if next != nil {
		rest = s.from(next, partEnd)
	}
	return partEnd, storeutil.StreamPart(ctx, head, rest, 2*sizeHint), nil
}

// testClient returns a client of a sender of sources
func testClient(t *testing.T, sources map[importer.Kind]importer.Source, opts ...SenderOption) importer.Source {
	t.Helper()
	srv := httptest.NewServer(testNewSender(t, sources, opts...))
	t.Cleanup(srv.Close)
	c, err := NewClient(srv.URL, testToken, WithHTTPClient(srv.Client()))
	require.NoError(t, err)
	return c
}

func TestClientReadsParts(t *testing.T) {
	entries := testEntries(3000)
	src := &testSliceSource{entries: entries}
	c := testClient(t, map[importer.Kind]importer.Source{importer.KindSync: src}, WithPartBytes(8<<10))

	rng := rand.New(rand.NewPCG(1, 2))
	parts := importertest.ReadParts(t, rng, c, importer.KindSync, 0)
	got := importertest.Entries(parts)
	require.Len(t, got, len(entries))
	for i := range entries {
		require.Equal(t, entries[i].Key, got[i].Key)
		require.True(t, bytes.Equal(entries[i].Value, got[i].Value), "value of %s", entries[i].Key)
	}
	require.Greater(t, len(parts), 10, "the sender's part size makes many parts")
}

func TestClientErrors(t *testing.T) {
	sources := map[importer.Kind]importer.Source{
		importer.KindAdProcessed:  &testSource{openErr: importer.PermanentError(errors.New("frozen"))},
		importer.KindAdSkipReason: &testSource{openErr: errors.New("busy")},
		importer.KindSync:         &testSource{endErr: importer.PermanentError(errors.New("broken"))},
	}
	c := testClient(t, sources)
	ctx := t.Context()

	for _, tc := range []struct {
		name      string
		kind      importer.Kind
		permanent bool
		msg       string
	}{
		{"not served", importer.KindRecords, true, "404"},
		{"permanent refusal", importer.KindAdProcessed, true, "frozen"},
		{"transient refusal", importer.KindAdSkipReason, false, "busy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := c.OpenStream(ctx, tc.kind, nil, nil, 0)
			require.ErrorContains(t, err, tc.msg)
			require.Equal(t, tc.permanent, importer.IsPermanent(err), "%v", err)
		})
	}

	t.Run("error in the trailer", func(t *testing.T) {
		_, entries, err := c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.NoError(t, err)
		_, err = testReadAll(entries)
		require.ErrorContains(t, err, "broken")
		require.True(t, importer.IsPermanent(err))
	})

	t.Run("sender unreachable", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		srv.Close()
		c, err := NewClient(srv.URL, testToken)
		require.NoError(t, err)
		_, _, err = c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.Error(t, err)
		require.False(t, importer.IsPermanent(err))
	})

	t.Run("end that does not decode", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(endHeader, "not base64!")
		}))
		defer srv.Close()
		c, err := NewClient(srv.URL, testToken)
		require.NoError(t, err)
		_, _, err = c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.True(t, importer.IsPermanent(err), "%v", err)
	})
}

// testCloseCounter is a RoundTripper that counts the response bodies
// closed
type testCloseCounter struct {
	rt     http.RoundTripper
	closed *atomic.Int64
}

func (c testCloseCounter) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.rt.RoundTrip(r)
	if err == nil {
		resp.Body = testCountedBody{ReadCloser: resp.Body, closed: c.closed}
	}
	return resp, err
}

type testCountedBody struct {
	io.ReadCloser
	closed *atomic.Int64
}

func (b testCountedBody) Close() error {
	b.closed.Add(1)
	return b.ReadCloser.Close()
}

// TestClientClosesResponses checks that every response is closed: after a
// part read whole, after a loop that breaks early, and after a refusal
func TestClientClosesResponses(t *testing.T) {
	sources := map[importer.Kind]importer.Source{
		importer.KindSync:        &testSource{entries: testEntries(100)},
		importer.KindAdProcessed: &testSource{openErr: errors.New("busy")},
	}
	srv := httptest.NewServer(testNewSender(t, sources))
	defer srv.Close()
	var closed atomic.Int64
	httpClient := &http.Client{Transport: testCloseCounter{rt: srv.Client().Transport, closed: &closed}}
	c, err := NewClient(srv.URL, testToken, WithHTTPClient(httpClient))
	require.NoError(t, err)
	ctx := t.Context()

	_, entries, err := c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
	require.NoError(t, err)
	_, err = testReadAll(entries)
	require.NoError(t, err)
	require.EqualValues(t, 1, closed.Load(), "after a whole part")

	_, entries, err = c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
	require.NoError(t, err)
	for range entries {
		break
	}
	require.EqualValues(t, 2, closed.Load(), "after a loop that breaks")

	_, _, err = c.OpenStream(ctx, importer.KindAdProcessed, nil, nil, 0)
	require.Error(t, err)
	require.EqualValues(t, 3, closed.Load(), "after a refusal")
}

func TestNewClientBadURL(t *testing.T) {
	_, err := NewClient(fmt.Sprintf("http://%c", 0x7f), testToken)
	require.Error(t, err)
}

func TestToken(t *testing.T) {
	entries := testEntries(10)
	srv := httptest.NewServer(testNewSender(t, map[importer.Kind]importer.Source{importer.KindSync: &testSource{entries: entries}}))
	defer srv.Close()
	ctx := t.Context()

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"wrong token", "guess"},
		{"token prefix", testToken[:len(testToken)-1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewClient(srv.URL, tc.token)
			require.NoError(t, err)
			_, _, err = c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
			require.ErrorContains(t, err, "401")
			// A token may be changed back while the import retries
			require.False(t, importer.IsPermanent(err))
			// A kind that is not served is refused the same way
			_, _, err = c.OpenStream(ctx, importer.KindRecords, nil, nil, 0)
			require.ErrorContains(t, err, "401")
		})
	}

	t.Run("no token", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+PartPath+string(importer.KindSync), nil)
		require.NoError(t, err)
		resp, err := srv.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("empty token is refused", func(t *testing.T) {
		_, err := NewSender(nil, "")
		require.Error(t, err)
		_, err = NewClient(srv.URL, "")
		require.Error(t, err)
	})

	c, err := NewClient(srv.URL, testToken)
	require.NoError(t, err)
	_, seq, err := c.OpenStream(ctx, importer.KindSync, nil, nil, 0)
	require.NoError(t, err)
	got, err := testReadAll(seq)
	require.NoError(t, err)
	require.Equal(t, entries, got)
}

// TestClientPath checks the path of a kind's part request: the base URL's
// path, PartPath, then the kind's name with its group as a path segment
func TestClientPath(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.EscapedPath()
		http.NotFound(w, r)
	}))
	defer srv.Close()
	for base, want := range map[string]string{
		srv.URL:             "/import/v1/kinds/ds/sync",
		srv.URL + "/":       "/import/v1/kinds/ds/sync",
		srv.URL + "/prefix": "/prefix/import/v1/kinds/ds/sync",
	} {
		c, err := NewClient(base, testToken)
		require.NoError(t, err)
		_, _, err = c.OpenStream(t.Context(), importer.KindSync, nil, nil, 0)
		require.Error(t, err)
		require.Equal(t, want, path, base)
	}
}
