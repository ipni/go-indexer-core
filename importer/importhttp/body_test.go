package importhttp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"testing"
	"testing/iotest"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/require"
)

// testEntries returns n entries with keys and values of different lengths,
// some empty values among them. An empty value is nil, as testReadAll
// returns it.
func testEntries(n int) []importer.Entry {
	out := make([]importer.Entry, n)
	for i := range out {
		out[i] = importer.Entry{Key: fmt.Appendf(nil, "key-%04d", i)}
		if i%7 != 0 {
			out[i].Value = bytes.Repeat([]byte{byte(i)}, i%7*40)
		}
	}
	return out
}

// testSeq yields entries, then err when it is not nil
func testSeq(entries []importer.Entry, err error) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		for _, e := range entries {
			if !yield(e, nil) {
				return
			}
		}
		if err != nil {
			yield(importer.Entry{}, err)
		}
	}
}

// testReadAll returns copies of the entries that seq yields and the error
// that ends it. An empty value is nil, and no entries is a nil slice.
func testReadAll(seq iter.Seq2[importer.Entry, error]) ([]importer.Entry, error) {
	var out []importer.Entry
	for e, err := range seq {
		if err != nil {
			return out, err
		}
		var value []byte
		if len(e.Value) > 0 {
			value = bytes.Clone(e.Value)
		}
		out = append(out, importer.Entry{Key: bytes.Clone(e.Key), Value: value})
	}
	return out, nil
}

// testBody returns the body that writeBody writes for entries and err
func testBody(t *testing.T, entries []importer.Entry, err error) []byte {
	t.Helper()
	var b bytes.Buffer
	_, _, writeErr := writeBody(&b, testSeq(entries, err))
	require.NoError(t, writeErr)
	return b.Bytes()
}

func TestBodyRoundTrip(t *testing.T) {
	entries := testEntries(300)
	boom := errors.New("boom")

	for _, tc := range []struct {
		name  string
		err   error
		check func(t *testing.T, err error)
	}{
		{"complete", nil, func(t *testing.T, err error) { require.NoError(t, err) }},
		{"partial", &importer.PartialError{Resume: []byte("key-0299\x00")}, func(t *testing.T, err error) {
			var pe *importer.PartialError
			require.ErrorAs(t, err, &pe)
			require.Equal(t, []byte("key-0299\x00"), pe.Resume)
		}},
		{"transient", boom, func(t *testing.T, err error) {
			require.ErrorContains(t, err, "boom")
			require.False(t, importer.IsPermanent(err))
		}},
		{"permanent", importer.PermanentError(boom), func(t *testing.T, err error) {
			require.ErrorContains(t, err, "boom")
			require.True(t, importer.IsPermanent(err))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, n := range []int{0, 1, len(entries)} {
				body := testBody(t, entries[:n], tc.err)
				// One byte per read, so that every field is read across reads
				got, err := testReadAll(readBody(iotest.OneByteReader(bytes.NewReader(body))))
				require.Equal(t, len(entries[:n]), len(got))
				if n > 0 {
					require.Equal(t, entries[:n], got)
				}
				tc.check(t, err)
			}
		})
	}
}

func TestWriteBodyStopsAtError(t *testing.T) {
	read := 0
	entries := func(yield func(importer.Entry, error) bool) {
		read++
		if !yield(importer.Entry{}, errors.New("boom")) {
			return
		}
		read++
		yield(importer.Entry{Key: []byte("k")}, nil)
	}
	var b bytes.Buffer
	_, _, err := writeBody(&b, entries)
	require.NoError(t, err)
	require.Equal(t, 1, read, "the entries went on after an error")
}

func TestWriteBodyEmptyKey(t *testing.T) {
	entries := []importer.Entry{{Key: []byte("a")}, {Key: nil, Value: []byte("v")}}
	got, err := testReadAll(readBody(bytes.NewReader(testBody(t, entries, nil))))
	require.Equal(t, entries[:1], got)
	require.True(t, importer.IsPermanent(err), "%v", err)
}

func TestWriteBodyWriteError(t *testing.T) {
	boom := errors.New("boom")
	_, _, err := writeBody(errWriter{boom}, testSeq(testEntries(3), nil))
	require.ErrorIs(t, err, boom)
}

func TestWriteBodyStatus(t *testing.T) {
	entries := testEntries(3)
	for _, tc := range []struct {
		name    string
		entries []importer.Entry
		err     error
		want    byte
		count   uint64
	}{
		{"complete", entries, nil, statusComplete, 3},
		{"partial", entries, &importer.PartialError{Resume: []byte("r")}, statusPartial, 3},
		{"transient", entries, errors.New("boom"), statusTransient, 3},
		{"permanent", entries, importer.PermanentError(errors.New("boom")), statusPermanent, 3},
		{"empty key", []importer.Entry{entries[0], {Key: nil}, entries[1]}, nil, statusPermanent, 1},
		{"resume over the limit", entries, &importer.PartialError{Resume: make([]byte, maxTrailerFieldBytes+1)}, statusPermanent, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, count, err := writeBody(io.Discard, testSeq(tc.entries, tc.err))
			require.NoError(t, err)
			require.Equal(t, tc.want, status)
			require.Equal(t, tc.count, count)
		})
	}
}

type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

// TestReadBodyCutOff checks that every body cut short ends with a transient
// error, for each way a body can end
func TestReadBodyCutOff(t *testing.T) {
	entries := testEntries(20)
	for _, end := range []error{nil, &importer.PartialError{Resume: []byte("r")}, errors.New("boom"), importer.PermanentError(errors.New("boom"))} {
		body := testBody(t, entries, end)
		for n := range len(body) {
			_, err := testReadAll(readBody(bytes.NewReader(body[:n])))
			require.ErrorIs(t, err, errCutOff, "body cut at %d of %d", n, len(body))
			require.False(t, importer.IsPermanent(err))
		}
	}
}

func TestReadBodyCorrupt(t *testing.T) {
	entries := testEntries(3)
	body := testBody(t, entries, nil)
	trailer := len(body) - 3 // the key length 0, the status and the count 3

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"data after the trailer", append(bytes.Clone(body), 0)},
		{"count too high", append(bytes.Clone(body[:trailer]), 0, statusComplete, 4)},
		{"count too low", append(bytes.Clone(body[:trailer]), 0, statusComplete, 2)},
		{"unknown status", append(bytes.Clone(body[:trailer]), 0, 9, 3)},
		{"key over the limit", binary.AppendUvarint(nil, maxKeyBytes+1)},
		{"value over the limit", binary.AppendUvarint(append(binary.AppendUvarint(nil, 1), 'k'), maxValueBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := testReadAll(readBody(bytes.NewReader(tc.body)))
			require.ErrorIs(t, err, errCutOff)
			require.False(t, importer.IsPermanent(err))
		})
	}

	t.Run("read error", func(t *testing.T) {
		_, err := testReadAll(readBody(iotest.ErrReader(iotest.ErrTimeout)))
		require.ErrorIs(t, err, iotest.ErrTimeout)
		require.False(t, importer.IsPermanent(err))
	})
}

func TestReadBodyStop(t *testing.T) {
	body := testBody(t, testEntries(10), nil)
	n := 0
	for _, err := range readBody(bytes.NewReader(body)) {
		require.NoError(t, err)
		if n++; n == 3 {
			break
		}
	}
	require.Equal(t, 3, n)
}

func TestBodyLargeValue(t *testing.T) {
	// A value larger than the reader's buffer, between two small entries
	large := make([]byte, 5<<20+123)
	for i := range large {
		large[i] = byte(i % 251)
	}
	entries := []importer.Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: large},
		{Key: []byte("c"), Value: []byte("3")},
	}
	got, err := testReadAll(readBody(bytes.NewReader(testBody(t, entries, nil))))
	require.NoError(t, err)
	require.Equal(t, entries, got)
}

func TestWriteBodyOverLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry importer.Entry
	}{
		{"key", importer.Entry{Key: make([]byte, maxKeyBytes+1)}},
		{"value", importer.Entry{Key: []byte("k"), Value: make([]byte, maxValueBytes+1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := []importer.Entry{{Key: []byte("a")}, tc.entry}
			got, err := testReadAll(readBody(bytes.NewReader(testBody(t, entries, nil))))
			require.Equal(t, entries[:1], got)
			require.True(t, importer.IsPermanent(err), "%v", err)
		})
	}

	// Entries at the limits are sent
	entries := []importer.Entry{{Key: bytes.Repeat([]byte{1}, maxKeyBytes), Value: bytes.Repeat([]byte{2}, maxValueBytes)}}
	got, err := testReadAll(readBody(bytes.NewReader(testBody(t, entries, nil))))
	require.NoError(t, err)
	require.Equal(t, entries, got)
}

func TestBodyTrailerLimits(t *testing.T) {
	entries := testEntries(2)

	// A long message is cut to the limit
	long := errors.New(string(bytes.Repeat([]byte("m"), 2*maxTrailerFieldBytes)))
	_, err := testReadAll(readBody(bytes.NewReader(testBody(t, entries, long))))
	require.ErrorContains(t, err, string(bytes.Repeat([]byte("m"), maxTrailerFieldBytes)))
	require.NotContains(t, err.Error(), string(bytes.Repeat([]byte("m"), maxTrailerFieldBytes+1)))
	require.False(t, importer.IsPermanent(err))

	// A Resume over the limit is the source's bug, a permanent error
	resume := &importer.PartialError{Resume: make([]byte, maxTrailerFieldBytes+1)}
	_, err = testReadAll(readBody(bytes.NewReader(testBody(t, entries, resume))))
	require.True(t, importer.IsPermanent(err), "%v", err)

	// The reader refuses a trailer field over the limit
	// (no entries: the key length 0, the status, the count 0, the field length)
	body := binary.AppendUvarint([]byte{0, statusTransient, 0}, maxTrailerFieldBytes+1)
	_, err = testReadAll(readBody(bytes.NewReader(body)))
	require.ErrorIs(t, err, errCutOff)
	require.ErrorContains(t, err, "over the limit")
}
