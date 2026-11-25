package importhttp

import (
	"bytes"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestAcceptsZstd(t *testing.T) {
	for _, tc := range []struct {
		values []string
		want   bool
	}{
		{nil, false},
		{[]string{"gzip"}, false},
		{[]string{"zstd"}, true},
		{[]string{"gzip, ZSTD"}, true},
		{[]string{"gzip", "br, zstd;q=0.5"}, true},
		{[]string{"zstd;q=0"}, false},
		{[]string{"zstd; q=0.000"}, false},
		{[]string{"zstd;q=1"}, true},
		{[]string{"zstdx"}, false},
	} {
		require.Equal(t, tc.want, acceptsZstd(tc.values), "%q", tc.values)
	}
}

// testRecordEntries returns n entries shaped like a part of multihashes:
// random digests, and values that repeat a few record IDs, as compressed
// data does
func testRecordEntries(n int) []importer.Entry {
	ids := make([][]byte, 16)
	for i := range ids {
		ids[i] = make([]byte, 20)
		_, _ = rand.Read(ids[i])
	}
	out := make([]importer.Entry, n)
	for i := range out {
		key := append([]byte{0x12, 0x20}, make([]byte, 32)...)
		_, _ = rand.Read(key[2:])
		out[i] = importer.Entry{Key: key, Value: append(bytes.Clone(ids[i%16]), ids[(i/16)%16]...)}
	}
	return out
}

func TestSenderCompresses(t *testing.T) {
	entries := testRecordEntries(2000)
	h := testNewSender(t, map[importer.Kind]importer.Source{importer.KindMultihashes: &testSource{entries: entries}})

	post := func(acceptEncoding string) *httptest.ResponseRecorder {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, PartPath+string(importer.KindMultihashes), bytes.NewReader([]byte("{}")))
		r.Header.Set("Authorization", "Bearer "+testToken)
		if acceptEncoding != "" {
			r.Header.Set("Accept-Encoding", acceptEncoding)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code)
		return w
	}

	plain := post("")
	plainLen := plain.Body.Len()
	require.Empty(t, plain.Header().Get("Content-Encoding"))
	got, err := testReadAll(readBody(plain.Body))
	require.NoError(t, err)
	require.Equal(t, entries, got)

	compressed := post("gzip, zstd")
	require.Equal(t, zstdEncoding, compressed.Header().Get("Content-Encoding"))
	require.Less(t, compressed.Body.Len(), plainLen*9/10, "the repeated record IDs compress")
	d, err := newDecoder(compressed.Body)
	require.NoError(t, err)
	defer d.Close()
	got, err = testReadAll(readBody(d))
	require.NoError(t, err)
	require.Equal(t, entries, got)
}

// testEncodingRecorder is a RoundTripper that records the Content-Encoding
// of each response
type testEncodingRecorder struct {
	rt        http.RoundTripper
	encodings []string
}

func (r *testEncodingRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.rt.RoundTrip(req)
	if err == nil {
		r.encodings = append(r.encodings, resp.Header.Get("Content-Encoding"))
	}
	return resp, err
}

func TestClientCompressed(t *testing.T) {
	entries := testRecordEntries(500)
	srv := httptest.NewServer(testNewSender(t, map[importer.Kind]importer.Source{importer.KindMultihashes: &testSource{entries: entries}}))
	defer srv.Close()
	rec := &testEncodingRecorder{rt: srv.Client().Transport}
	c, err := NewClient(srv.URL, testToken, WithHTTPClient(&http.Client{Transport: rec}))
	require.NoError(t, err)

	_, seq, err := c.OpenStream(t.Context(), importer.KindMultihashes, nil, nil, 0)
	require.NoError(t, err)
	got, err := testReadAll(seq)
	require.NoError(t, err)
	require.Equal(t, entries, got)
	require.Equal(t, []string{zstdEncoding}, rec.encodings, "the client asks for zstd")
}

// TestClientBodies checks how the client reads bodies that a sender other
// than NewSender may send
func TestClientBodies(t *testing.T) {
	entries := testEntries(100)
	var plain bytes.Buffer
	_, _, err := writeBody(&plain, testSeq(entries, nil))
	require.NoError(t, err)
	var compressed bytes.Buffer
	enc, err := newEncoder(&compressed)
	require.NoError(t, err)
	_, err = enc.Write(plain.Bytes())
	require.NoError(t, err)
	require.NoError(t, enc.Close())

	serve := func(coding string, body []byte) importer.Source {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if coding != "" {
				w.Header().Set("Content-Encoding", coding)
			}
			_, _ = w.Write(body)
		}))
		t.Cleanup(srv.Close)
		c, err := NewClient(srv.URL, testToken)
		require.NoError(t, err)
		return c
	}
	ctx := t.Context()

	t.Run("plain", func(t *testing.T) {
		_, seq, err := serve("", plain.Bytes()).OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.NoError(t, err)
		got, err := testReadAll(seq)
		require.NoError(t, err)
		require.Equal(t, entries, got)
	})

	t.Run("compressed and cut off", func(t *testing.T) {
		_, seq, err := serve(zstdEncoding, compressed.Bytes()[:compressed.Len()-10]).OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.NoError(t, err)
		_, err = testReadAll(seq)
		require.Error(t, err)
		require.False(t, importer.IsPermanent(err))
	})

	t.Run("window over the limit", func(t *testing.T) {
		var wide bytes.Buffer
		enc, err := zstd.NewWriter(&wide, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(2*zstdMaxWindowBytes))
		require.NoError(t, err)
		// A frame of more than one block declares the encoder's window; a
		// smaller frame declares its own size
		_, err = enc.Write(append(bytes.Clone(plain.Bytes()), make([]byte, 1<<20)...))
		require.NoError(t, err)
		require.NoError(t, enc.Close())

		_, seq, err := serve(zstdEncoding, wide.Bytes()).OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.NoError(t, err)
		_, err = testReadAll(seq)
		require.ErrorIs(t, err, zstd.ErrWindowSizeExceeded)
		require.True(t, importer.IsPermanent(err), "%v", err)
	})

	t.Run("unknown coding", func(t *testing.T) {
		_, _, err := serve("br", plain.Bytes()).OpenStream(ctx, importer.KindSync, nil, nil, 0)
		require.True(t, importer.IsPermanent(err), "%v", err)
	})
}
