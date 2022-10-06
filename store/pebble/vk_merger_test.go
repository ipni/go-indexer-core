//go:build !386

package pebble

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/multiformats/go-multihash"
)

var (
	value1 = indexer.Value{ProviderID: "fish", ContextID: []byte("1")}
	value2 = indexer.Value{ProviderID: "in", ContextID: []byte("2")}
	value3 = indexer.Value{ProviderID: "dasea", ContextID: []byte("3")}
)

func TestValueKeysMerger_IsAssociative(t *testing.T) {
	bk := newBlake3Keyer(10)
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

	subject := newValueKeysMerger()
	oneMerge, err := subject.Merge(k, a)
	if err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeOlder(b); err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeOlder(c); err != nil {
		t.Fatal(err)
	}
	gotOne, _, err := oneMerge.Finish(false)
	if err != nil {
		t.Fatal(err)
	}

	anotherMerge, err := subject.Merge(k, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := anotherMerge.MergeNewer(b); err != nil {
		t.Fatal(err)
	}
	if err := anotherMerge.MergeNewer(a); err != nil {
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
	bk := newBlake3Keyer(10)

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

	subject := newValueKeysMerger()
	mk, err := bk.multihashKey(mh)
	if err != nil {
		t.Fatal()
	}
	oneMerge, err := subject.Merge(mk, vk1)
	if err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeNewer(vk2); err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeNewer(vk3); err != nil {
		t.Fatal(err)
	}
	if err := oneMerge.MergeNewer(dvk2); err != nil {
		t.Fatal(err)
	}

	// Assert that vk2 is not present since its delete key, dvk2, is also merged.
	gotVKs, _, err := oneMerge.Finish(false)
	if err != nil {
		t.Fatal(err)
	}

	wantVKs, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk1, vk3})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(wantVKs, gotVKs) {
		t.Fatalf("expected %v but got %v", wantVKs, gotVKs)
	}
}

func TestValueKeysValueMerger_RepeatedlyMarshalledValueKeys(t *testing.T) {

	bk := newBlake3Keyer(10)
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
	want, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk1, vk2, vk3})
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
		subject := newValueKeysMerger()
		m, err := subject.Merge(k, mvk4)
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
		subject := newValueKeysMerger()
		m, err := subject.Merge(k, vk1)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.MergeNewer(vk2); err != nil {
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

	reverse, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{vk3, vk2, vk1})
	if err != nil {
		t.Fatal(err)
	}
	rmvk2, err := indexer.BinaryValueCodec{}.MarshalValueKeys([][]byte{reverse})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("mix nested older", func(t *testing.T) {
		subject := newValueKeysMerger()
		m, err := subject.Merge(k, vk3)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(rmvk2); err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(mvk3); err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(vk1); err != nil {
			t.Fatal(err)
		}
		if err := m.MergeOlder(vk2); err != nil {
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
