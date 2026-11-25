package pebble

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// TestKinds checks that Kinds lists each kind that kindPrefix knows, among
// all the importer's kinds, once, each with a prefix of its own
func TestKinds(t *testing.T) {
	prefixes := make(map[keyPrefix]importer.Kind)
	for _, kind := range importer.AllKinds {
		prefix, err := kindPrefix(kind)
		require.Equal(t, err == nil, slices.Contains(Kinds, kind), "kindPrefix and Kinds differ on %s", kind)
		if err != nil {
			require.True(t, importer.IsPermanent(err), kind)
			continue
		}
		other, ok := prefixes[prefix]
		require.False(t, ok, "%s and %s share a prefix", kind, other)
		prefixes[prefix] = kind
	}
	require.Len(t, Kinds, len(slices.Compact(slices.Sorted(slices.Values(Kinds)))), "a kind repeats")
}

// testStreamFixture is a source store with the multihashes it was given and a
// few multihashes whose layout the test checks by name
type testStreamFixture struct {
	mhs []multihash.Multihash
	// emptied are multihashes whose every slot was removed, so they hold
	// an empty value
	emptied []multihash.Multihash
	// withDeleteSlot holds a live slot for live and a merge-delete slot
	withDeleteSlot multihash.Multihash
	live           indexer.Value
}

// fillTestStreamStore writes providers with token and legacy contexts, each
// over many multihashes, then removes contexts, a provider and slots. Part
// of the data is flushed and part stays in the memtable.
func fillTestStreamStore(t *testing.T, rng *rand.Rand, s *store) testStreamFixture {
	var f testStreamFixture
	f.mhs = testMixedMultihashes(t, rng, 3000)
	pids := random.Peers(5)
	value := func(p, c int) indexer.Value {
		return indexer.Value{
			ProviderID:    pids[p],
			ContextID:     fmt.Appendf(nil, "ctx-%d", c),
			MetadataBytes: random.Bytes(int64(100 + rng.IntN(400))),
		}
	}
	put := func(p int) {
		for c := range 30 {
			v := value(p, c)
			sub := make([]multihash.Multihash, 100)
			for i := range sub {
				sub[i] = f.mhs[rng.IntN(len(f.mhs))]
			}
			if c%4 == 0 {
				seedLegacyValue(t, s, v, sub...)
			} else {
				require.NoError(t, s.Put(v, sub...))
			}
		}
	}

	for p := range 3 {
		put(p)
	}
	f.live = value(0, 2)
	require.NoError(t, s.Put(f.live, f.mhs[0]))
	f.emptied = random.Multihashes(10)
	emptiedValue := value(1, 100)
	require.NoError(t, s.Put(emptiedValue, f.emptied...))
	require.NoError(t, s.Flush())

	// Merge-delete operands over flushed values, and removed records
	require.NoError(t, s.Remove(emptiedValue, f.emptied...))
	require.NoError(t, s.Remove(value(2, 3), f.mhs[:500]...))
	require.NoError(t, s.RemoveProviderContext(pids[0], []byte("ctx-1")))
	require.NoError(t, s.RemoveProviderContext(pids[0], []byte("ctx-4")))
	put(3)
	put(4)
	require.NoError(t, s.RemoveProvider(t.Context(), pids[4]))

	// A multihash value whose merge kept a merge-delete slot, as a
	// compaction without the key's base value writes it
	f.withDeleteSlot = random.Multihashes(1)[0]
	liveKey := valueKeyForContext(t, s, f.live.ProviderID, f.live.ContextID)
	deleted := append([]byte{byte(mergeDeleteValueKeyPrefix)}, random.Bytes(recordIDLength)...)
	v, closer, err := s.vcodec.marshalValueKeys([][]byte{liveKey, deleted})
	require.NoError(t, err)
	require.NoError(t, s.db.Set(append([]byte{byte(multihashKeyPrefix)}, f.withDeleteSlot...), v, pebble.NoSync))
	_ = closer.Close()
	f.mhs = append(f.mhs, f.withDeleteSlot)
	f.mhs = append(f.mhs, f.emptied...)
	return f
}

// testMixedMultihashes returns n distinct multihashes of several families, so
// that keys cluster under a few leading bytes and differ in length:
// sha2-256, sha2-512, and identity multihashes of 1 to 64 bytes
func testMixedMultihashes(t *testing.T, rng *rand.Rand, n int) []multihash.Multihash {
	t.Helper()
	seen := make(map[string]bool, n)
	mhs := make([]multihash.Multihash, 0, n)
	for len(mhs) < n {
		var (
			mh  multihash.Multihash
			err error
		)
		switch rng.IntN(5) {
		case 0:
			mh, err = multihash.Sum(random.Bytes(int64(1+rng.IntN(64))), multihash.IDENTITY, -1)
		case 1:
			mh, err = multihash.Sum(random.Bytes(32), multihash.SHA2_512, -1)
		default:
			mh, err = multihash.Sum(random.Bytes(32), multihash.SHA2_256, -1)
		}
		require.NoError(t, err)
		if !seen[string(mh)] {
			seen[string(mh)] = true
			mhs = append(mhs, mh)
		}
	}
	return mhs
}

// testRawEntries returns the store's keys with prefix p and their values
func testRawEntries(t *testing.T, s *store, p keyPrefix) []importer.Entry {
	t.Helper()
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{byte(p)}, UpperBound: []byte{byte(p) + 1}})
	require.NoError(t, err)
	var out []importer.Entry
	for valid := it.First(); valid; valid = it.Next() {
		out = append(out, importer.Entry{Key: bytes.Clone(it.Key()), Value: bytes.Clone(it.Value())})
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	return out
}

// testExpectedWire returns the wire entries of a kind, from the store's raw
// keys decoded with the store's codec: keys without the prefix, no empty
// values, and multihash values as the record IDs of the slots that Get reads
func testExpectedWire(t *testing.T, s *store, kind importer.Kind) []importer.Entry {
	t.Helper()
	p, err := kindPrefix(kind)
	require.NoError(t, err)
	var out []importer.Entry
	for _, e := range testRawEntries(t, s, p) {
		if len(e.Value) == 0 {
			continue
		}
		value := e.Value
		if p == multihashKeyPrefix {
			vks, err := s.vcodec.unmarshalValueKeys(e.Value)
			require.NoError(t, err)
			value = nil
			for _, vk := range vks.keys {
				if vk.prefix() == valueKeyPrefix {
					value = append(value, vk.buf[1:]...)
				}
			}
			_ = vks.Close()
			if len(value) == 0 {
				continue
			}
		}
		out = append(out, importer.Entry{Key: e.Key[1:], Value: value})
	}
	return out
}

// requireTestPartSizes checks that no part opened with a size hint holds more
// than twice the hint, plus its largest entry, by which a read overshoots.
func requireTestPartSizes(t *testing.T, parts []importertest.Part) {
	t.Helper()
	for _, p := range parts {
		if p.Hint == 0 {
			continue
		}
		var largest uint64
		for _, e := range p.Entries {
			largest = max(largest, uint64(len(e.Key)+len(e.Value)))
		}
		require.LessOrEqual(t, p.Bytes(), 2*p.Hint+largest, "a part far above its size hint")
	}
}

// TestStreamUnflushed checks that data still in the memtable is cut in parts
func TestStreamUnflushed(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	s := openPebbleStore(t)
	value := indexer.Value{ProviderID: random.Peers(1)[0], ContextID: []byte("c"), MetadataBytes: []byte("m")}
	require.NoError(t, s.Put(value, testMixedMultihashes(t, rng, 5000)...))
	usage, err := s.db.EstimateDiskUsage([]byte{byte(multihashKeyPrefix)}, []byte{byte(multihashKeyPrefix) + 1})
	require.NoError(t, err)
	require.Zero(t, usage, "the multihashes are flushed")

	source, ok := any(s).(importer.Source)
	require.True(t, ok)
	parts := importertest.ReadParts(t, rng, source, importer.KindMultihashes, 32<<10)
	require.Greater(t, len(parts), 5)
	require.Equal(t, testExpectedWire(t, s, importer.KindMultihashes), importertest.Entries(parts))
	requireTestPartSizes(t, parts)
}

func TestStreamSourceRefuses(t *testing.T) {
	s := openPebbleStore(t)
	source, ok := any(s).(importer.Source)
	require.True(t, ok)
	ctx := t.Context()
	for _, tc := range []struct {
		name       string
		kind       importer.Kind
		start, end []byte
	}{
		{"unknown kind", "adProcessed", nil, nil},
		{"end equals start", importer.KindRecords, []byte{1}, []byte{1}},
		{"end before start", importer.KindRecords, []byte{2}, []byte{1}},
		{"empty end", importer.KindRecords, nil, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := source.OpenStream(ctx, tc.kind, tc.start, tc.end, 0)
			require.True(t, importer.IsPermanent(err), "%v", err)
		})
	}
}

func TestStreamSourceCancelled(t *testing.T) {
	s := openPebbleStore(t)
	require.NoError(t, s.Put(indexer.Value{ProviderID: random.Peers(1)[0], ContextID: []byte("c"), MetadataBytes: []byte("m")}, random.Multihashes(3)...))
	source, ok := any(s).(importer.Source)
	require.True(t, ok)

	t.Run("before an open part", func(t *testing.T) {
		// The sample checks the context for each entry it reads
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, _, err := source.OpenStream(ctx, importer.KindMultihashes, nil, nil, 0)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, importer.IsPermanent(err))
	})
	t.Run("after the sample", func(t *testing.T) {
		// The open part's first entries come from the sample's memory
		ctx, cancel := context.WithCancel(t.Context())
		_, entries, err := source.OpenStream(ctx, importer.KindMultihashes, nil, nil, 0)
		require.NoError(t, err)
		cancel()
		n := 0
		for _, err := range entries {
			require.ErrorIs(t, err, context.Canceled)
			n++
			break
		}
		require.Equal(t, 1, n)
	})
	t.Run("during the store read", func(t *testing.T) {
		// A closed part reads the store alone
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		_, entries, err := source.OpenStream(ctx, importer.KindMultihashes, nil, []byte{0xff}, 0)
		require.NoError(t, err)
		n := 0
		for e, err := range entries {
			if n++; n == 1 {
				require.NoError(t, err)
				require.NotEmpty(t, e.Key)
				cancel()
				continue
			}
			require.ErrorIs(t, err, context.Canceled)
			break
		}
		require.Equal(t, 2, n)
	})
}

// TestStreamCancelledOverSkippedKeys checks that a part notices a cancelled
// context while it walks keys that it does not send
func TestStreamCancelledOverSkippedKeys(t *testing.T) {
	s := openPebbleStore(t)
	b := s.db.NewBatch()
	for i := range 5000 {
		// A multihash key with an empty value is skipped
		key := append([]byte{byte(multihashKeyPrefix), 0x12, 0x20}, fmt.Appendf(nil, "%08d", i)...)
		require.NoError(t, b.Set(key, nil, nil))
	}
	require.NoError(t, b.Commit(nil))
	source := any(s).(importer.Source)

	ctx, cancel := context.WithCancel(t.Context())
	_, entries, err := source.OpenStream(ctx, importer.KindMultihashes, nil, []byte{0xff}, 0)
	require.NoError(t, err)
	cancel()
	n := 0
	for _, err := range entries {
		n++
		require.ErrorIs(t, err, context.Canceled)
		break
	}
	require.Equal(t, 1, n, "the part did not yield the cancellation")
}

// TestStreamResumeNotAtPartEnd checks that a part cut at its only entry does
// not resume at its own end. With a size hint of 1 the sample is one entry and
// the part end is that entry's key followed by 0x00; the entry is above twice
// the hint, but no entry follows it, so the part ends without a PartialError.
func TestStreamResumeNotAtPartEnd(t *testing.T) {
	s := openPebbleStore(t)
	require.NoError(t, s.Put(indexer.Value{ProviderID: random.Peers(1)[0], ContextID: []byte("c"), MetadataBytes: []byte("m")}, random.Multihashes(5)...))
	source := any(s).(importer.Source)

	end, entries, err := source.OpenStream(t.Context(), importer.KindMultihashes, nil, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, end)
	var partial *importer.PartialError
	for _, err := range entries {
		if err != nil {
			require.ErrorAs(t, err, &partial)
			break
		}
	}
	if partial != nil {
		// Resume must be before the part's end, as importertest checks
		require.Negative(t, bytes.Compare(partial.Resume, end), "resume %x is not before the end %x", partial.Resume, end)
		_, _, err = source.OpenStream(t.Context(), importer.KindMultihashes, partial.Resume, end, 1)
		require.NoError(t, err)
	}
}

// testCountEntries returns the number of entries, which must not yield errors
func testCountEntries(t *testing.T, entries iter.Seq2[importer.Entry, error]) int {
	t.Helper()
	n := 0
	for _, err := range entries {
		require.NoError(t, err)
		n++
	}
	return n
}

// TestStreamSourceEmpty checks the kinds of a store that holds none of their
// data
func TestStreamSourceEmpty(t *testing.T) {
	s := openPebbleStore(t)
	source, ok := any(s).(importer.Source)
	require.True(t, ok)
	for _, kind := range Kinds {
		end, entries, err := source.OpenStream(t.Context(), kind, nil, nil, 0)
		require.NoError(t, err)
		require.Nil(t, end)
		require.Zero(t, testCountEntries(t, entries))

		end, entries, err = source.OpenStream(t.Context(), kind, nil, []byte{0xff}, 0)
		require.NoError(t, err)
		require.Equal(t, []byte{0xff}, end)
		require.Zero(t, testCountEntries(t, entries))
	}
}

// TestStreamPartialStops reads a closed part with a small size hint. Each
// request stops with a PartialError after about twice the hint, and the
// requests resumed from its cursor together yield every entry once.
func TestStreamPartialStops(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	src := openPebbleStore(t)
	fillTestStreamStore(t, rng, src)
	source, ok := any(src).(importer.Source)
	require.True(t, ok)
	ctx := t.Context()
	const hint = 1000

	for _, kind := range Kinds {
		want := testExpectedWire(t, src, kind)
		end := bytes.Repeat([]byte{0xff}, 64)
		var got []importer.Entry
		var start []byte
		requests := 0
		for {
			requests++
			partEnd, entries, err := source.OpenStream(ctx, kind, start, end, hint)
			require.NoError(t, err)
			require.Equal(t, end, partEnd)
			var partial *importer.PartialError
			var bytesSent, n int
			var last []byte
			for e, err := range entries {
				if err != nil {
					require.ErrorAs(t, err, &partial)
					break
				}
				got = append(got, importer.Entry{Key: bytes.Clone(e.Key), Value: bytes.Clone(e.Value)})
				bytesSent += len(e.Key) + len(e.Value)
				// e.Key is valid only until the next iteration
				last = got[len(got)-1].Key
				n++
			}
			if partial == nil {
				break
			}
			require.Positive(t, n)
			require.GreaterOrEqual(t, bytesSent, 2*hint)
			require.Equal(t, append(bytes.Clone(last), 0), partial.Resume)
			start = partial.Resume
		}
		require.Greater(t, requests, 1, kind)
		require.Equal(t, want, got, kind)
	}
}

// TestStreamSourceReadError checks that a failed read of the store fails an
// open part when it is opened, and a closed part when it is read, with a
// transient error.
func TestStreamSourceReadError(t *testing.T) {
	// Reads of the tables fail once failing is on
	failing := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		if strings.HasSuffix(op.Path, ".sst") && op.Kind.ReadOrWrite() == errorfs.OpIsRead {
			return errorfs.ErrInjected
		}
		return nil
	})}
	fs := errorfs.Wrap(vfs.NewMem(), failing)
	vs, err := New("/db", &pebble.Options{FS: fs})
	require.NoError(t, err)
	value := indexer.Value{ProviderID: random.Peers(1)[0], ContextID: []byte("c"), MetadataBytes: []byte("m")}
	require.NoError(t, vs.Put(value, random.Multihashes(10)...))
	require.NoError(t, vs.Flush())
	require.NoError(t, vs.Close())

	vs, err = New("/db", &pebble.Options{FS: fs})
	require.NoError(t, err)
	defer func() { _ = vs.Close() }()
	source, ok := any(vs).(importer.Source)
	require.True(t, ok)
	failing.On()
	ctx := t.Context()

	_, _, err = source.OpenStream(ctx, importer.KindMultihashes, nil, nil, 0)
	require.ErrorIs(t, err, errorfs.ErrInjected)
	require.False(t, importer.IsPermanent(err))

	_, entries, err := source.OpenStream(ctx, importer.KindMultihashes, nil, []byte{0xff}, 0)
	require.NoError(t, err)
	for _, err = range entries {
		if err != nil {
			break
		}
	}
	require.ErrorIs(t, err, errorfs.ErrInjected)
	require.False(t, importer.IsPermanent(err))
}

func TestToWireEntry(t *testing.T) {
	id := func(b byte) []byte { return bytes.Repeat([]byte{b}, recordIDLength) }
	slot := func(p keyPrefix, b byte) []byte {
		return append([]byte{marshalledValueKeyLength - 1, byte(p)}, id(b)...)
	}
	mhKey := []byte{byte(multihashKeyPrefix), 1, 2}
	value := slices.Concat(slot(valueKeyPrefix, 1), slot(mergeDeleteValueKeyPrefix, 2), slot(contextMapKeyPrefix, 3), slot(valueKeyPrefix, 4))

	// Only the live slots are sent, as their record IDs
	e, _, ok := toWireEntry(mhKey, value, nil)
	require.True(t, ok)
	require.Equal(t, importer.Entry{Key: []byte{1, 2}, Value: slices.Concat(id(1), id(4))}, e)

	// A record is sent as stored, without its prefix byte
	e, _, ok = toWireEntry([]byte{byte(valueKeyPrefix), 7}, []byte{1}, nil)
	require.True(t, ok)
	require.Equal(t, importer.Entry{Key: []byte{7}, Value: []byte{1}}, e)

	// A multihash whose value does not decode is skipped, and so is one
	// with only a merge-delete slot, an empty value, or the prefix alone
	_, _, ok = toWireEntry(mhKey, value[1:], nil)
	require.False(t, ok)
	_, _, ok = toWireEntry(mhKey, slot(mergeDeleteValueKeyPrefix, 2), nil)
	require.False(t, ok)
	_, _, ok = toWireEntry(mhKey, nil, nil)
	require.False(t, ok)
	_, _, ok = toWireEntry(mhKey[:1], value, nil)
	require.False(t, ok)
	_, _, ok = toWireEntry([]byte{byte(valueKeyPrefix), 2}, nil, nil)
	require.False(t, ok)
}
