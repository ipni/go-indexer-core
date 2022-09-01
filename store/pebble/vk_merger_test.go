//go:build !386

package pebble

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
)

func TestValueKeysMerger_IsAssociative(t *testing.T) {
	key := []byte("fish")
	a := []byte("A")
	b := []byte("B")
	c := []byte("C")

	subject := newValueKeysMerger()
	oneMerge, err := subject.Merge(key, a)
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

	anotherMerge, err := subject.Merge(key, c)
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
	mh := []byte("lobster")
	bk := newBlake3Keyer(10)
	value1 := indexer.Value{ProviderID: "fish", ContextID: []byte("1")}
	value2 := indexer.Value{ProviderID: "in", ContextID: []byte("2")}
	value3 := indexer.Value{ProviderID: "dasea", ContextID: []byte("3")}

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
	oneMerge, err := subject.Merge(mh, vk1)
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
