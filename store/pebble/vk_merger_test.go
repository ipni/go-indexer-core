package pebble

import (
	"bytes"
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

func TestValueKeysMerger_IsAssociative(t *testing.T) {
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()
	k, err := bk.multihashKey(multihash.Multihash("fish"))
	if err != nil {
		t.Fatal()
	}
	a, err := bk.valueKey(value1, false)
	if err != nil {
		t.Fatal()
	}
	b, err := bk.valueKey(value2, false)
	if err != nil {
		t.Fatal()
	}
	c, err := bk.valueKey(value3, false)
	if err != nil {
		t.Fatal()
	}

	subject := newValueKeysMerger(cdc)
	oneMerge, err := subject.Merge(k.buf, a.buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeOlder(b.buf); err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeOlder(c.buf); err != nil {
		t.Fatal(err)
	}
	gotOne, _, err := oneMerge.Finish(false)
	if err != nil {
		t.Fatal(err)
	}

	anotherMerge, err := subject.Merge(k.buf, c.buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := anotherMerge.MergeNewer(b.buf); err != nil {
		t.Fatal(err)
	}
	if err := anotherMerge.MergeNewer(a.buf); err != nil {
		t.Fatal(err)
	}
	gotAnother, _, err := anotherMerge.Finish(false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotOne, gotAnother) {
		t.Fatalf("merge is not associative. %v != %v", gotOne, gotAnother)
	}
}

func TestValueKeysValueMerger_DeleteKeyRemovesValueKeys(t *testing.T) {
	mh := multihash.Multihash("lobster")
	p := newPool()
	cdc := &codec{p: p}
	bk := p.leaseBlake3Keyer()

	vk1, err := bk.valueKey(value1, false)
	if err != nil {
		t.Fatal(err)
	}
	vk2, err := bk.valueKey(value2, false)
	if err != nil {
		t.Fatal(err)
	}
	dvk2, err := bk.valueKey(value2, true)
	if err != nil {
		t.Fatal(err)
	}
	vk3, err := bk.valueKey(value3, false)
	if err != nil {
		t.Fatal(err)
	}

	subject := newValueKeysMerger(cdc)
	mk, err := bk.multihashKey(mh)
	if err != nil {
		t.Fatal()
	}
	oneMerge, err := subject.Merge(mk.buf, vk1.buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeNewer(vk2.buf); err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeNewer(vk3.buf); err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeNewer(dvk2.buf); err != nil {
		t.Fatal(err)
	}

	// Assert that vk2 is not present since its delete key, dvk2, is also merged.
	gotVKs, _, err := oneMerge.Finish(false)
	if err != nil {
		t.Fatal(err)
	}

	wantVKs, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk1.buf, vk3.buf})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(wantVKs, gotVKs) {
		t.Fatalf("expected %v but got %v", wantVKs, gotVKs)
	}
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

	// Older delete operands prepended mergeDeleteKeyPrefix to the value key.
	oldDelete := append([]byte{byte(legacyMergeDeleteKeyPrefix)}, vk2.buf...)

	subject := newValueKeysMerger(cdc)
	m, err := subject.Merge(mk.buf, vk1.buf)
	require.NoError(t, err)
	require.NoError(t, m.MergeNewer(vk2.buf))
	require.NoError(t, m.MergeNewer(vk3.buf))
	require.NoError(t, m.MergeNewer(oldDelete))

	got, _, err := m.Finish(false)
	require.NoError(t, err)
	want, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk1.buf, vk3.buf})
	require.NoError(t, err)
	require.Equal(t, want, got)
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

	got, _, err := m.Finish(false)
	require.NoError(t, err)
	want, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk1.buf})
	require.NoError(t, err)
	require.Equal(t, want, got)
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
	if err != nil {
		t.Fatal()
	}

	vk1, err := bk.valueKey(value1, false)
	if err != nil {
		t.Fatal(err)
	}
	vk2, err := bk.valueKey(value2, false)
	if err != nil {
		t.Fatal(err)
	}
	vk3, err := bk.valueKey(value3, false)
	if err != nil {
		t.Fatal(err)
	}

	// Repeatedly marshall the marshalled value
	want, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk1.buf, vk2.buf, vk3.buf})
	if err != nil {
		t.Fatal(err)
	}
	mvk2, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{want})
	if err != nil {
		t.Fatal(err)
	}
	mvk3, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{mvk2})
	if err != nil {
		t.Fatal(err)
	}
	mvk4, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{mvk3})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("four nested initial", func(t *testing.T) {
		subject := newValueKeysMerger(cdc)
		m, err := subject.Merge(k.buf, mvk4)
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := m.Finish(false)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatal()
		}
	})
	t.Run("mix nested newer", func(t *testing.T) {
		subject := newValueKeysMerger(cdc)
		m, err := subject.Merge(k.buf, vk1.buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.MergeNewer(vk2.buf); err != nil {
			t.Fatal(err)
		}
		if err := m.MergeNewer(mvk3); err != nil {
			t.Fatal(err)
		}
		got, _, err := m.Finish(false)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatal()
		}
	})

	reverse, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk3.buf, vk2.buf, vk1.buf})
	if err != nil {
		t.Fatal(err)
	}
	rmvk2, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{reverse})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("mix nested older", func(t *testing.T) {
		subject := newValueKeysMerger(cdc)
		m, err := subject.Merge(k.buf, vk3.buf)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(rmvk2); err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(mvk3); err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(vk1.buf); err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(vk2.buf); err != nil {
			t.Fatal(err)
		}
		got, _, err := m.Finish(false)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatal()
		}
	})
}
