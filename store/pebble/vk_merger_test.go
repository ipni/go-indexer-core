package pebble

import (
	"bytes"
	"slices"
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var (
	value1 = &indexer.Value{ProviderID: "fish", ContextID: []byte("1"), MetadataBytes: []byte{}}
	value2 = &indexer.Value{ProviderID: "in", ContextID: []byte("2"), MetadataBytes: []byte("lulu")}
	value3 = &indexer.Value{ProviderID: "dasea", ContextID: []byte("3"), MetadataBytes: []byte{141}}
)

func unpackMergeSlots(t *testing.T, cdc *codec, got []byte) [][]byte {
	t.Helper()
	if len(got) == 0 {
		return nil
	}
	kl, err := cdc.unmarshalValueKeys(got)
	require.NoError(t, err)
	defer kl.Close()
	out := make([][]byte, 0, len(kl.keys))
	for _, k := range kl.keys {
		out = append(out, bytes.Clone(k.buf))
	}
	slices.SortFunc(out, bytes.Compare)
	return out
}

func requireMergeSlotsEqual(t *testing.T, cdc *codec, got []byte, want ...[]byte) {
	t.Helper()
	wantSorted := make([][]byte, len(want))
	for i, w := range want {
		wantSorted[i] = bytes.Clone(w)
	}
	slices.SortFunc(wantSorted, bytes.Compare)
	require.Equal(t, wantSorted, unpackMergeSlots(t, cdc, got))
}

func TestValueKeysMerger_IsAssociative(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	k, err := bk.multihashKey(multihash.Multihash("fish"))
	require.NoError(t, err)
	a, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	b, err := bk.valueKey(value2, false)
	require.NoError(t, err)
	c, err := bk.valueKey(value3, false)
	require.NoError(t, err)

	subject := newValueKeysMerger(cdc)
	oneMerge, err := subject.Merge(k.buf, a.buf)
	require.NoError(t, err)
	require.NoError(t, oneMerge.MergeOlder(b.buf))
	require.NoError(t, oneMerge.MergeOlder(c.buf))
	gotOne, _, err := oneMerge.Finish(true)
	require.NoError(t, err)

	anotherMerge, err := subject.Merge(k.buf, c.buf)
	require.NoError(t, err)
	require.NoError(t, anotherMerge.MergeNewer(b.buf))
	require.NoError(t, anotherMerge.MergeNewer(a.buf))
	gotAnother, _, err := anotherMerge.Finish(true)
	require.NoError(t, err)

	require.Equal(t, gotOne, gotAnother)
}

func TestValueKeysValueMerger_DeleteKeyRemovesValueKeys(t *testing.T) {
	mh := multihash.Multihash("lobster")
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()

	vk1, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	vk2, err := bk.valueKey(value2, false)
	require.NoError(t, err)
	dvk2, err := bk.valueKey(value2, true)
	require.NoError(t, err)
	vk3, err := bk.valueKey(value3, false)
	require.NoError(t, err)

	subject := newValueKeysMerger(cdc)
	mk, err := bk.multihashKey(mh)
	require.NoError(t, err)
	oneMerge, err := subject.Merge(mk.buf, vk1.buf)
	require.NoError(t, err)
	require.NoError(t, oneMerge.MergeNewer(vk2.buf))
	require.NoError(t, oneMerge.MergeNewer(vk3.buf))
	require.NoError(t, oneMerge.MergeNewer(dvk2.buf))

	gotVKs, _, err := oneMerge.Finish(true)
	require.NoError(t, err)
	requireMergeSlotsEqual(t, cdc, gotVKs, vk1.buf, vk3.buf)
}

func TestValueKeysMerger_FinishWithoutBaseKeepsDeletes(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	mk, err := bk.multihashKey(multihash.Multihash("lobster"))
	require.NoError(t, err)
	vk1, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	vk2, err := bk.valueKey(value2, false)
	require.NoError(t, err)
	dvk2, err := bk.valueKey(value2, true)
	require.NoError(t, err)

	subject := newValueKeysMerger(cdc)
	m, err := subject.Merge(mk.buf, vk1.buf)
	require.NoError(t, err)
	require.NoError(t, m.MergeNewer(vk2.buf))
	require.NoError(t, m.MergeNewer(dvk2.buf))

	got, _, err := m.Finish(false)
	require.NoError(t, err)
	requireMergeSlotsEqual(t, cdc, got, vk1.buf, dvk2.buf)
}

func TestValueKeysMerger_FinishWithoutBaseDropsSupersededDeletes(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	mk, err := bk.multihashKey(multihash.Multihash("lobster"))
	require.NoError(t, err)
	vk1, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	dvk1, err := bk.valueKey(value1, true)
	require.NoError(t, err)

	subject := newValueKeysMerger(cdc)
	m, err := subject.Merge(mk.buf, dvk1.buf)
	require.NoError(t, err)
	require.NoError(t, m.MergeNewer(vk1.buf))

	got, _, err := m.Finish(false)
	require.NoError(t, err)
	requireMergeSlotsEqual(t, cdc, got, vk1.buf)
}

func TestValueKeysMerger_OldDeleteOperand(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	mk, err := bk.multihashKey(multihash.Multihash("lobster"))
	require.NoError(t, err)
	vk1, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	vk2, err := bk.valueKey(value2, false)
	require.NoError(t, err)
	vk3, err := bk.valueKey(value3, false)
	require.NoError(t, err)

	// Legacy delete operands prepended legacyMergeDeleteKeyPrefix to the value key.
	oldDelete := append([]byte{byte(legacyMergeDeleteKeyPrefix)}, vk2.buf...)

	subject := newValueKeysMerger(cdc)
	m, err := subject.Merge(mk.buf, vk1.buf)
	require.NoError(t, err)
	require.NoError(t, m.MergeNewer(vk2.buf))
	require.NoError(t, m.MergeNewer(vk3.buf))
	require.NoError(t, m.MergeNewer(oldDelete))

	got, _, err := m.Finish(true)
	require.NoError(t, err)
	requireMergeSlotsEqual(t, cdc, got, vk1.buf, vk3.buf)
}

func TestValueKeysMerger_DropsShortDeleteOperand(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	mk, err := bk.multihashKey(multihash.Multihash("lobster"))
	require.NoError(t, err)
	vk1, err := bk.valueKey(value1, false)
	require.NoError(t, err)

	subject := newValueKeysMerger(cdc)
	m, err := subject.Merge(mk.buf, vk1.buf)
	require.NoError(t, err)
	require.NoError(t, m.MergeNewer([]byte{byte(mergeDeleteValueKeyPrefix), 1, 2, 3}))

	got, _, err := m.Finish(true)
	require.NoError(t, err)
	requireMergeSlotsEqual(t, cdc, got, vk1.buf)
}

func TestValueKeysMerger_ShortDeleteDoesNotMatchZeroPaddedSuffix(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	mk, err := bk.multihashKey(multihash.Multihash("lobster"))
	require.NoError(t, err)

	shortDelete := []byte{byte(mergeDeleteValueKeyPrefix), 1, 2, 3}

	// A full-length value key that equals what zero-padding the short delete
	// would produce. Apply the delete first so a padded reconstruct would make
	// exists() treat the later add as already deleted.
	padded := [1 + providerHashLen*2]byte{byte(valueKeyPrefix)}
	copy(padded[1:], shortDelete[1:])

	subject := newValueKeysMerger(cdc)
	m, err := subject.Merge(mk.buf, padded[:])
	require.NoError(t, err)
	require.NoError(t, m.MergeNewer(shortDelete))

	got, _, err := m.Finish(true)
	require.NoError(t, err)
	want, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{padded[:]})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestValueKeysValueMerger_RepeatedlyMarshalledValueKeys(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	mh := multihash.Multihash("lobster")
	k, err := bk.multihashKey(mh)
	require.NoError(t, err)

	vk1, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	vk2, err := bk.valueKey(value2, false)
	require.NoError(t, err)
	vk3, err := bk.valueKey(value3, false)
	require.NoError(t, err)

	wantSlots := [][]byte{vk1.buf, vk2.buf, vk3.buf}
	want, err := indexer.BinaryValueCodec{}.MarshalValueKeys(wantSlots)
	require.NoError(t, err)
	mvk2, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{want})
	require.NoError(t, err)
	mvk3, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{mvk2})
	require.NoError(t, err)
	mvk4, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{mvk3})
	require.NoError(t, err)

	t.Run("four nested initial", func(t *testing.T) {
		subject := newValueKeysMerger(cdc)
		m, err := subject.Merge(k.buf, mvk4)
		require.NoError(t, err)
		got, _, err := m.Finish(true)
		require.NoError(t, err)
		requireMergeSlotsEqual(t, cdc, got, wantSlots...)
	})
	t.Run("mix nested newer", func(t *testing.T) {
		subject := newValueKeysMerger(cdc)
		m, err := subject.Merge(k.buf, vk1.buf)
		require.NoError(t, err)
		require.NoError(t, m.MergeNewer(vk2.buf))
		require.NoError(t, m.MergeNewer(mvk3))
		got, _, err := m.Finish(true)
		require.NoError(t, err)
		requireMergeSlotsEqual(t, cdc, got, wantSlots...)
	})

	reverse, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk3.buf, vk2.buf, vk1.buf})
	require.NoError(t, err)
	rmvk2, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{reverse})
	require.NoError(t, err)

	t.Run("mix nested older", func(t *testing.T) {
		subject := newValueKeysMerger(cdc)
		m, err := subject.Merge(k.buf, vk3.buf)
		require.NoError(t, err)
		require.NoError(t, m.MergeOlder(rmvk2))
		require.NoError(t, m.MergeOlder(mvk3))
		require.NoError(t, m.MergeOlder(vk1.buf))
		require.NoError(t, m.MergeOlder(vk2.buf))
		got, _, err := m.Finish(true)
		require.NoError(t, err)
		requireMergeSlotsEqual(t, cdc, got, wantSlots...)
	})
}
