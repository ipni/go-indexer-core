package storeutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"iter"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/require"
)

// testSliceReader reads sorted entries. It skips the entries for which skip
// returns true, but counts their bytes, as a store counts the keys it does
// not send. It counts its reads.
type testSliceReader struct {
	sorted []importer.Entry
	skip   func(importer.Entry) bool
	reads  int
}

func newTestSliceReader(entries []importer.Entry) *testSliceReader {
	slices.SortFunc(entries, func(a, b importer.Entry) int { return bytes.Compare(a.Key, b.Key) })
	entries = slices.CompactFunc(entries, func(a, b importer.Entry) bool { return bytes.Equal(a.Key, b.Key) })
	return &testSliceReader{sorted: entries, skip: func(importer.Entry) bool { return false }}
}

// entries yields the entries from the key from that the reader does not
// skip. It counts its calls in reads.
func (r *testSliceReader) entries(from []byte) iter.Seq2[importer.Entry, error] {
	r.reads++
	return func(yield func(importer.Entry, error) bool) {
		i, _ := slices.BinarySearchFunc(r.sorted, from, testCmpKey)
		for _, e := range r.sorted[i:] {
			if !r.skip(e) && !yield(e, nil) {
				return
			}
		}
	}
}

// sent returns the entries the reader does not skip in [from, to), to the
// end when to is nil.
func (r *testSliceReader) sent(from, to []byte) []importer.Entry {
	i, _ := slices.BinarySearchFunc(r.sorted, from, testCmpKey)
	j := len(r.sorted)
	if to != nil {
		j, _ = slices.BinarySearchFunc(r.sorted, to, testCmpKey)
	}
	var out []importer.Entry
	for _, e := range r.sorted[i:max(i, j)] {
		if !r.skip(e) {
			out = append(out, e)
		}
	}
	return out
}

func testCmpKey(e importer.Entry, key []byte) int { return bytes.Compare(e.Key, key) }

// testEntriesOf returns an entry for each key, with a value that brings the
// entry to n bytes.
func testEntriesOf(keys [][]byte, n int) []importer.Entry {
	out := make([]importer.Entry, len(keys))
	for i, k := range keys {
		out[i] = importer.Entry{Key: k, Value: make([]byte, max(n-len(k), 1))}
	}
	return out
}

// testPosKey returns prefix followed by p as 8 big-endian bytes
func testPosKey(prefix []byte, p uint64) []byte {
	return binary.BigEndian.AppendUint64(bytes.Clone(prefix), p)
}

// testPosKeys returns a key at each position that pos gives for 0 to n-1, which
// must not decrease. A key at the same position as the one before it ends
// with 8 more 0x00 bytes and i. Such keys are distinct, but they share a
// position on the number line of a sample whose first and last keys have no
// suffix.
func testPosKeys(prefix []byte, n int, pos func(i int) uint64) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = testPosKey(prefix, pos(i))
		if i > 0 && pos(i) == pos(i-1) {
			keys[i] = append(keys[i], 0, 0, 0, 0, 0, 0, 0, 0, byte(i))
		}
	}
	return keys
}

// testPart is a part as PlanPart planned it, with the entries an open read
// of it yields.
type testPart struct {
	start, end []byte
	// sampled is true when the part is its sample alone
	sampled bool
	entries []importer.Entry
}

func (p testPart) bytes() int { return testEntryBytes(p.entries) }

// testPlanAll cuts all of r's entries into parts with PlanPart and checks every
// part: its end is after its start and after its sample, the ends ascend,
// and an open read of each part, the sample then the rest up to its end,
// yields what a closed read of [start, end) yields. It checks that the
// parts hold every entry once.
func testPlanAll(t *testing.T, r *testSliceReader, size uint64) []testPart {
	t.Helper()
	return testPlanAllClustered(t, r, size, testOneCluster)
}

// testPlanAllClustered is testPlanAll for keys that clusterLen names the
// clusters of
func testPlanAllClustered(t *testing.T, r *testSliceReader, size uint64, clusterLen ClusterLenFunc) []testPart {
	t.Helper()
	var parts []testPart
	var start []byte
	for {
		end, head, next, err := PlanPart(r.entries, start, size, clusterLen)
		require.NoError(t, err)
		if end != nil {
			require.Positive(t, bytes.Compare(end, start), "end %x start %x", end, start)
			if len(head) > 0 {
				require.Positive(t, bytes.Compare(end, head[len(head)-1].Key), "end %x is not after the sample", end)
			}
		}
		require.True(t, next == nil || end == nil || bytes.Compare(next, end) < 0, "the rest of the part starts at %x, after its end %x", next, end)
		entries := slices.Clone(head)
		if next != nil {
			entries = append(entries, r.sent(next, end)...)
		}
		require.Equal(t, r.sent(start, end), entries, "the open read of a part differs from the closed one")
		parts = append(parts, testPart{start: start, end: end, sampled: next == nil, entries: entries})
		if end == nil {
			break
		}
		start = end
		require.Less(t, len(parts), 100_000)
	}
	var all []importer.Entry
	for _, p := range parts {
		all = append(all, p.entries...)
	}
	require.Equal(t, r.sent(nil, nil), all)
	return parts
}

func testRandomKeys(rng *rand.Rand, prefix []byte, n, length int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		k := bytes.Clone(prefix)
		for range length {
			k = append(k, byte(rng.Uint32()))
		}
		keys[i] = k
	}
	return keys
}

func TestPlanPartUniform(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	// 50 MB of entries of 1000 bytes, more than a sample of 1 MiB holds
	r := newTestSliceReader(testEntriesOf(testRandomKeys(rng, []byte{0x01}, 50_000, 32), 1000))

	t.Run("estimated", func(t *testing.T) {
		const size = 4 << 20
		parts := testPlanAll(t, r, size)
		require.InDelta(t, 50_000*1000/size, len(parts), 3)
		// The last parts hold what is left of the data
		for _, p := range parts[:len(parts)-2] {
			require.False(t, p.sampled, "a uniform part was cut at its sample")
			require.InDelta(t, size, p.bytes(), size/4)
		}
	})
	t.Run("size within the sample", func(t *testing.T) {
		const size = 5000
		parts := testPlanAll(t, r, size)
		for _, p := range parts[:len(parts)-1] {
			require.True(t, p.sampled)
			require.GreaterOrEqual(t, p.bytes(), size)
			require.Less(t, p.bytes(), size+1000)
		}
	})
}

// testBigEntry is the entry size of the estimate tests: 16 entries are
// exactly the sample of 1 MiB
const testBigEntry = 1 << 16

// TestPlanPartEstimate covers the end that PlanPart estimates from the
// sample. The sample holds 16 entries of 64 KiB, 1 MiB, and a part of 2.5 MiB
// extrapolates from the sample's first and last keys: first + (last - first)
// × 2.5. Keys are 9 bytes: a prefix and a position as 8 big-endian bytes.
func TestPlanPartEstimate(t *testing.T) {
	const size = 5 << 19
	prefix := []byte{0x01}
	for _, tc := range []struct {
		name string
		pos  func(i int) uint64
		// last is the position of the sample's last key
		last uint64
	}{
		{"uniform", func(i int) uint64 { return uint64(i) * 1024 }, 15 * 1024},
		{"dense then sparse", func(i int) uint64 {
			if i < 10 {
				return uint64(i) * 10
			}
			return uint64(i-9)*10_000 + 90
		}, 60_090},
		{"sparse then dense", func(i int) uint64 {
			if i < 10 {
				return uint64(i) * 10_000
			}
			return uint64(i-9)*10 + 90_000
		}, 90_060},
		{"few positions", func(i int) uint64 { return uint64((i+1)/4) * 1024 }, 4 * 1024},
		{"many keys at one position", func(i int) uint64 {
			return uint64(max(i-10, 0)) * 1024
		}, 5 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestSliceReader(testEntriesOf(testPosKeys(prefix, 200, tc.pos), testBigEntry))
			end, head, next, err := PlanPart(r.entries, nil, size, testOneCluster)
			require.NoError(t, err)
			require.Len(t, head, 16)
			require.Equal(t, append(bytes.Clone(head[15].Key), 0), next)
			require.Equal(t, testPosKey(prefix, tc.last*5/2), end)
		})
	}

	t.Run("estimate at the last key", func(t *testing.T) {
		// 1 byte more than the sample: the estimate rounds down to the last
		// key, so the part is the sample
		r := newTestSliceReader(testEntriesOf(testPosKeys(prefix, 200, func(i int) uint64 { return uint64(i) }), testBigEntry))
		end, head, next, err := PlanPart(r.entries, nil, 1<<20+1, testOneCluster)
		require.NoError(t, err)
		require.Len(t, head, 16)
		require.Equal(t, append(bytes.Clone(head[15].Key), 0), end)
		require.Nil(t, next)
	})
	t.Run("one key sample", func(t *testing.T) {
		r := newTestSliceReader([]importer.Entry{
			{Key: []byte{0x01}, Value: make([]byte, 1<<20)},
			{Key: []byte{0x02}, Value: []byte{1}},
		})
		end, head, next, err := PlanPart(r.entries, nil, 2<<20, testOneCluster)
		require.NoError(t, err)
		require.Len(t, head, 1)
		require.Equal(t, []byte{0x01, 0x00}, end)
		require.Nil(t, next)
	})
}

// testPrefixCluster names the first n bytes of a key as its cluster
func testPrefixCluster(n int) ClusterLenFunc {
	return func([]byte) int { return n }
}

// testClusterEntries returns 100 entries of testBigEntry bytes under a
// prefix, at positions (i+1)<<57: the first sample key is at 0x02 and the
// 16th at 0x20 in the byte after the prefix
func testClusterEntries(prefix []byte) []importer.Entry {
	keys := testPosKeys(prefix, 100, func(i int) uint64 { return uint64(i+1) << 57 })
	return testEntriesOf(keys, testBigEntry)
}

// TestPlanPartClusters checks how the estimate meets the cluster's end. A
// part of 20 MiB extrapolates 30 × 20 = 0x258 from the sample's first to
// its last key in the byte after the prefix, which carries 2 into the
// prefix's last byte.
func TestPlanPartClusters(t *testing.T) {
	const size = 20 << 20
	t.Run("carry without a cluster end", func(t *testing.T) {
		entries := append(testClusterEntries([]byte{0x01, 0x05}), testClusterEntries([]byte{0x01, 0x07})...)
		r := newTestSliceReader(entries)
		end, head, next, err := PlanPart(r.entries, nil, size, testOneCluster)
		require.NoError(t, err)
		require.Len(t, head, 16)
		require.Equal(t, []byte{0x01, 0x07, 0x5a, 0, 0, 0, 0, 0, 0, 0}, end)
		require.Equal(t, append(bytes.Clone(head[15].Key), 0), next)
		testPlanAll(t, r, size)
	})
	t.Run("estimate is clamped to the cluster end", func(t *testing.T) {
		entries := append(testClusterEntries([]byte{0x01, 0x05}), testClusterEntries([]byte{0x01, 0x07})...)
		r := newTestSliceReader(entries)
		end, head, next, err := PlanPart(r.entries, nil, size, testPrefixCluster(2))
		require.NoError(t, err)
		require.Len(t, head, 16)
		require.Equal(t, []byte{0x01, 0x06}, end)
		require.Equal(t, append(bytes.Clone(head[15].Key), 0), next)
		testPlanAllClustered(t, r, size, testPrefixCluster(2))
	})
	t.Run("estimate before the cluster end", func(t *testing.T) {
		// 4 MiB: first + 30 × 4 / 1 = 0x7a in the byte after the prefix
		entries := append(testClusterEntries([]byte{0x01, 0x05}), testClusterEntries([]byte{0x01, 0x07})...)
		r := newTestSliceReader(entries)
		end, _, next, err := PlanPart(r.entries, nil, 4<<20, testPrefixCluster(2))
		require.NoError(t, err)
		require.Equal(t, []byte{0x01, 0x05, 0x7a, 0, 0, 0, 0, 0, 0, 0}, end)
		require.NotNil(t, next)
	})
	t.Run("sample crosses clusters", func(t *testing.T) {
		// The sample starts 10 entries before the end of the first cluster.
		// The part is the whole sample, not an estimate.
		first := testClusterEntries([]byte{0x01, 0x05})[:10]
		entries := append(first, testClusterEntries([]byte{0x01, 0x07})...)
		r := newTestSliceReader(entries)
		end, head, next, err := PlanPart(r.entries, nil, size, testPrefixCluster(2))
		require.NoError(t, err)
		require.Len(t, head, 16)
		require.Equal(t, append(bytes.Clone(head[15].Key), 0), end)
		require.Nil(t, next)
		testPlanAllClustered(t, r, size, testPrefixCluster(2))
	})
	t.Run("past the end of the line", func(t *testing.T) {
		// The cluster is all 0xff bytes: it ends with the kind
		r := newTestSliceReader(testClusterEntries([]byte{0xff, 0xff}))
		for _, clusterLen := range []ClusterLenFunc{testOneCluster, testPrefixCluster(2)} {
			end, head, next, err := PlanPart(r.entries, nil, size, clusterLen)
			require.NoError(t, err)
			require.Len(t, head, 16)
			require.Nil(t, end)
			require.Equal(t, append(bytes.Clone(head[15].Key), 0), next)
			testPlanAllClustered(t, r, size, clusterLen)
		}
	})
}

func TestPlanPartKindEnds(t *testing.T) {
	t.Run("no entries", func(t *testing.T) {
		r := newTestSliceReader(nil)
		end, head, next, err := PlanPart(r.entries, nil, 1<<20, testOneCluster)
		require.NoError(t, err)
		require.Nil(t, end)
		require.Empty(t, head)
		require.Nil(t, next)
	})
	t.Run("inside the sample", func(t *testing.T) {
		entries := testEntriesOf(testPosKeys([]byte{0x01}, 5, func(i int) uint64 { return uint64(i) }), 100)
		r := newTestSliceReader(entries)
		end, head, next, err := PlanPart(r.entries, []byte{0x01}, 1<<20, testOneCluster)
		require.NoError(t, err)
		require.Nil(t, end)
		require.Equal(t, entries, head)
		require.Nil(t, next)
	})
	t.Run("only skipped keys", func(t *testing.T) {
		r := newTestSliceReader(testEntriesOf(testPosKeys([]byte{0x01}, 1000, func(i int) uint64 { return uint64(i) }), 100))
		r.skip = func(importer.Entry) bool { return true }
		end, head, next, err := PlanPart(r.entries, nil, 4000, testOneCluster)
		require.NoError(t, err)
		require.Nil(t, end)
		require.Empty(t, head)
		require.Nil(t, next)
	})
}

func TestPlanPartHugeEntry(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	entries := testEntriesOf(testRandomKeys(rng, []byte{0x01}, 2000, 32), 100)
	huge := importer.Entry{Key: []byte{0x01}, Value: make([]byte, 1<<20)}
	r := newTestSliceReader(append(entries, huge))
	end, head, next, err := PlanPart(r.entries, nil, 1<<20, testOneCluster)
	require.NoError(t, err)
	require.Equal(t, []importer.Entry{huge}, head)
	require.Equal(t, []byte{0x01, 0x00}, end)
	require.Nil(t, next)
	testPlanAll(t, r, 1<<20)
}

func TestPlanPartSkippedKeys(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	r := newTestSliceReader(testEntriesOf(testRandomKeys(rng, []byte{0x01}, 20_000, 32), 100))
	// Skip 7 of every 8 keys, as a store that sends few of the keys it reads
	r.skip = func(e importer.Entry) bool { return e.Key[1]%8 != 0 }
	const size = 64 << 10
	_, head, _, err := PlanPart(r.entries, nil, size, testOneCluster)
	require.NoError(t, err)
	require.GreaterOrEqual(t, testEntryBytes(head), size)
	// One read covers the sample, however many keys it skips
	require.Equal(t, 1, r.reads)
	// The reader sends about 250 KB
	parts := testPlanAll(t, r, size)
	require.InDelta(t, 4, len(parts), 1)
}

// TestPlanPartClustered cuts keyspaces whose keys cluster under shared
// prefixes and checks that no part is far above its size.
func TestPlanPartClustered(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	for _, tc := range []struct {
		name       string
		keys       [][]byte
		clusterLen int
	}{
		{"multihash codes", testClusteredMultihashKeys(rng, 20_000), 2},
		{"providers", testProviderClusteredKeys(rng, 20_000), 10},
		{"text CIDs", testCIDTextKeys(rng, 20_000), 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 20 MB of entries in parts of 2 MiB, more than a sample holds
			r := newTestSliceReader(testEntriesOf(tc.keys, 1000))
			const size = 2 << 20
			parts := testPlanAllClustered(t, r, size, testPrefixCluster(tc.clusterLen))
			require.Greater(t, len(parts), 20_000*1000/size)
			for _, p := range parts {
				require.LessOrEqual(t, p.bytes(), 2*size)
			}
		})
	}
}

// testClusteredMultihashKeys returns n keys under a few multihash codes, the
// way real multihash keys cluster.
func testClusteredMultihashKeys(rng *rand.Rand, n int) [][]byte {
	headers := [][]byte{{0x12, 0x20}, {0x16, 0x20}, {0xb2, 0x20}, {0x00, 0x05}}
	keys := make([][]byte, 0, n)
	for i := range n {
		h := headers[0]
		switch i % 10 {
		case 7:
			h = headers[1]
		case 8:
			h = headers[2]
		case 9:
			h = headers[3]
		}
		keys = append(keys, testRandomKeys(rng, h, 1, 32)[0])
	}
	return keys
}

// testProviderClusteredKeys returns record IDs of three providers whose hashes
// differ only in the last byte, each with many contexts.
func testProviderClusteredKeys(rng *rand.Rand, n int) [][]byte {
	keys := make([][]byte, 0, n)
	for i := range n {
		prefix := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, byte(i % 3)}
		keys = append(keys, testRandomKeys(rng, prefix, 1, 10)[0])
	}
	return keys
}

// testCIDTextKeys returns text CIDs, as the datastore's ad markers have
func testCIDTextKeys(rng *rand.Rand, n int) [][]byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	keys := make([][]byte, 0, n)
	for range n {
		k := []byte("bafy")
		for range 55 {
			k = append(k, alphabet[rng.IntN(len(alphabet))])
		}
		keys = append(keys, k)
	}
	return keys
}

func TestPlanPartRandomKeys(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10))
	for range 300 {
		var entries []importer.Entry
		for range rng.IntN(500) {
			k := make([]byte, 1+rng.IntN(14))
			for i := range k {
				// A small alphabet makes long common prefixes likely
				k[i] = []byte{0x00, 0x01, 0x7f, 0xfe, 0xff}[rng.IntN(5)]
			}
			entries = append(entries, importer.Entry{Key: k, Value: make([]byte, rng.IntN(300))})
		}
		r := newTestSliceReader(entries)
		testPlanAll(t, r, uint64(rng.IntN(20_000)))
	}
}

func TestPlanPartReadError(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	r := newTestSliceReader(testEntriesOf(testRandomKeys(rng, []byte{0x01}, 1000, 32), 100))
	boom := errors.New("boom")
	// The sample is the only read. The error comes first or after some
	// entries.
	for _, after := range []int{0, 5} {
		entries := func(from []byte) iter.Seq2[importer.Entry, error] {
			return func(yield func(importer.Entry, error) bool) {
				n := 0
				for e, err := range r.entries(from) {
					if n++; n > after || !yield(e, err) {
						break
					}
				}
				yield(importer.Entry{}, boom)
			}
		}
		_, _, _, err := PlanPart(entries, nil, 20_000, testOneCluster)
		require.ErrorIs(t, err, boom)
	}
}

// testEntryBytes returns the bytes of the entries' keys and values
func testEntryBytes(entries []importer.Entry) int {
	n := 0
	for _, e := range entries {
		n += len(e.Key) + len(e.Value)
	}
	return n
}

// testOneCluster puts every key in one cluster
func testOneCluster([]byte) int { return 0 }
