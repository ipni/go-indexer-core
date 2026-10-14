package pebble

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// FuzzValueKeysMergerAssociativity checks that arbitrary contiguous merges of
// a value-key operation sequence produce the same live set as applying every
// operation in order. Intermediate Finish(includesBase=false) results must
// retain delete marks so later merges still see them.
func FuzzValueKeysMergerAssociativity(f *testing.F) {
	// Seeds that exercise add / remove / re-add and multi-step reduction.
	f.Add([]byte{0, 1})                                              // add0, del0 — empty live set, DeletableFinish must request delete
	f.Add([]byte{0, 1, 0, 2, 1, 1, 0, 1})                            // add0, add1, add0, add2, del1, add1, add0, add1
	f.Add([]byte{0, 1, 0, 1, 0})                                     // add, del, add (re-add after remove)
	f.Add([]byte{1, 0})                                              // remove missing, then add
	f.Add([]byte{0, 0, 0, 1, 1, 0})                                  // duplicate adds then deletes
	f.Add([]byte{0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1})                   // longer mix
	f.Add([]byte{0, 1, 0, 2, 1, 1, 0, 3, 1, 0, 2, 0, 1, 2, 3, 1, 0}) // mix of values plus RNG tail for pivots

	cdc, liveKeys, deleteKeys, mhKey, merger := initFuzzValueKeysMergerAssociativity(f)

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) == 0 {
			t.Skip("empty seed")
		}

		// Up to 16 leading bytes are merge ops; any leftover bytes drive which
		// contiguous ranges to collapse.
		nOps := min(len(raw), 16)
		ops := make([][]byte, nOps)
		for i := range nOps {
			b := raw[i]
			if b&1 == 0 { // add op
				ops[i] = liveKeys[int(b>>1)%len(liveKeys)]
			} else { // delete op
				ops[i] = deleteKeys[int(b>>1)%len(deleteKeys)]
			}
		}
		rng := raw[nOps:]

		var want [][]byte
		for _, op := range ops {
			if op[0] == byte(valueKeyPrefix) {
				if !slices.ContainsFunc(
					want,
					func(vk []byte) bool { return bytes.Equal(vk, op) },
				) {
					want = append(want, op)
				}
			} else {
				want = slices.DeleteFunc(
					want,
					func(vk []byte) bool { return bytes.Equal(vk[1:], op[1:]) },
				)
			}
		}

		var needDelete bool

		if len(ops) == 1 {
			ops[0], needDelete = testRangeMerge(t, merger, mhKey, ops, true, true)
		}

		for len(ops) > 1 {
			if len(rng) < 4 {
				// Deterministically finish: merge everything left-to-right.
				var merged []byte
				merged, needDelete = testRangeMerge(t, merger, mhKey, ops, true, true)
				ops = [][]byte{merged}
				break
			}

			lo := int(binary.LittleEndian.Uint16(rng[0:2])) % len(ops)
			hi := lo + 2 + int(rng[2])
			if hi > len(ops) {
				t.Skip("invalid merge range")
			}

			merged, deleted := testRangeMerge(t, merger, mhKey, ops[lo:hi], rng[3]&1 != 1, lo == 0)

			ops = slices.Concat(ops[:lo], [][]byte{merged}, ops[hi:])
			needDelete = deleted
			rng = rng[4:]
		}

		require.Len(t, ops, 1)
		require.Equal(t, len(want) == 0, needDelete, "DeletableFinish delete flag")
		var gotKeys [][]byte
		if got := ops[0]; len(got) > 0 {
			kl, err := cdc.unmarshalValueKeys(got)
			require.NoError(t, err)
			if kl != nil {
				defer kl.Close()
				for _, k := range kl.keys {
					require.Equal(t, valueKeyPrefix, k.prefix(), "delete marks must not survive includesBase finish")
					gotKeys = append(gotKeys, bytes.Clone(k.buf))
				}
			}
		}
		require.ElementsMatch(t, want, gotKeys)
	})
}

func initFuzzValueKeysMergerAssociativity(f *testing.F) (*codec, [][]byte, [][]byte, []byte, *pebble.Merger) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	defer bk.Close()

	values := []*indexer.Value{
		{ProviderID: "fuzz-a", ContextID: []byte{1}, MetadataBytes: []byte{1}},
		{ProviderID: "fuzz-b", ContextID: []byte{2}, MetadataBytes: []byte{2}},
		{ProviderID: "fuzz-c", ContextID: []byte{3}, MetadataBytes: []byte{3}},
		{ProviderID: "fuzz-d", ContextID: []byte{4}, MetadataBytes: []byte{4}},
	}
	liveKeys := make([][]byte, len(values))
	deleteKeys := make([][]byte, len(values))
	for i, v := range values {
		vk, err := bk.valueKey(v, false)
		if err != nil {
			f.Fatal(err)
		}
		liveKeys[i] = bytes.Clone(vk.buf)
		_ = vk.Close()

		dk, err := bk.valueKey(v, true)
		if err != nil {
			f.Fatal(err)
		}
		deleteKeys[i] = bytes.Clone(dk.buf)
		_ = dk.Close()
	}

	mk, err := bk.multihashKey(multihash.Multihash("fuzz-mh"))
	if err != nil {
		f.Fatal(err)
	}
	mhKey := bytes.Clone(mk.buf)
	_ = mk.Close()
	merger := newValueKeysMerger(cdc)

	return cdc, liveKeys, deleteKeys, mhKey, merger
}

func testRangeMerge(
	t *testing.T,
	merger *pebble.Merger,
	mhKey []byte,
	parts [][]byte,
	newer, includesBase bool,
) ([]byte, bool) {
	t.Helper()
	require.NotEmpty(t, parts)

	var (
		m   pebble.ValueMerger
		err error
	)

	if newer {
		m, err = merger.Merge(mhKey, parts[0])
		require.NoError(t, err)
		for _, part := range parts[1:] {
			require.NoError(t, m.MergeNewer(part))
		}
	} else {
		m, err = merger.Merge(mhKey, parts[len(parts)-1])
		require.NoError(t, err)
		for i := len(parts) - 2; i >= 0; i-- {
			require.NoError(t, m.MergeOlder(parts[i]))
		}
	}

	dm, ok := m.(pebble.DeletableValueMerger)
	require.True(t, ok)

	out, needDelete, closer, err := dm.DeletableFinish(includesBase)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.NoError(t, err)

	return bytes.Clone(out), needDelete
}
