package pebble

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/multiformats/go-multihash"
)

func Test_blake3Keyer(t *testing.T) {
	const l = 4
	var vk key
	v := indexer.Value{
		ProviderID: "fish",
		ContextID:  []byte("lobster"),
	}

	subject := newBlake3Keyer(l)

	t.Run("multihashKey", func(t *testing.T) {
		wantMh, err := multihash.Sum([]byte("fish"), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}

		gotMhKey, err := subject.multihashKey(wantMh)
		if err != nil {
			t.Fatal(err)
		}
		if gotMhKey.prefix() != multihashKeyPrefix {
			t.Fatal()
		}
		gotMh, err := subject.keyToMultihash(gotMhKey)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wantMh, gotMh) {
			t.Fatal()
		}
	})

	t.Run("multihashesKeyRange", func(t *testing.T) {
		start, end, err := subject.multihashesKeyRange()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(start, []byte{byte(multihashKeyPrefix)}) {
			t.Fatal()
		}
		if !bytes.Equal(end, start.next()) {
			t.Fatal()
		}
		if start.prefix() != multihashKeyPrefix {
			t.Fatal()
		}
	})

	t.Run("valueKey", func(t *testing.T) {
		var err error
		vk, err = subject.valueKey(v, false)
		if err != nil {
			t.Fatal(err)
		}
		if vk.prefix() != valueKeyPrefix {
			t.Fatal()
		}
		p, vvk := vk.stripMergeDelete()
		if p != valueKeyPrefix {
			t.Fatal()
		}
		if !bytes.Equal(vk, vvk) {
			t.Fatal()
		}
	})

	t.Run("valuesByProviderKeyRange", func(t *testing.T) {
		start, end, err := subject.valuesByProviderKeyRange(v.ProviderID)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(vk, start) {
			t.Fatal()
		}
		if !bytes.Equal(end, start.next()) {
			t.Fatal()
		}
		if start.prefix() != valueKeyPrefix {
			t.Fatal()
		}
		if end.prefix() != valueKeyPrefix {
			t.Fatal()
		}
	})

	t.Run("valueKeyMergeDelete", func(t *testing.T) {
		dvk, err := subject.valueKey(v, true)
		if err != nil {
			t.Fatal(err)
		}
		if dvk.prefix() != mergeDeleteKeyPrefix {
			t.Fatal()
		}
		p, vk2 := dvk.stripMergeDelete()
		if p != mergeDeleteKeyPrefix {
			t.Fatal()
		}
		if !bytes.Equal(vk, vk2) {
			t.Fatal()
		}
	})
}

func Test_key_next(t *testing.T) {
	var k key
	t.Run("increment", func(t *testing.T) {
		k = []byte{1, 2, 3, 4}
		if !bytes.Equal([]byte{1, 2, 3, 5}, k.next()) {
			t.Fatal()
		}
	})
	t.Run("incrementWith0xff", func(t *testing.T) {
		k = []byte{1, 2, 3, 0xff}
		if !bytes.Equal([]byte{1, 2, 4}, k.next()) {
			t.Fatal()
		}
	})
	t.Run("0xff", func(t *testing.T) {
		k = []byte{0xff}
		if !bytes.Equal(nil, k.next()) {
			t.Fatal()
		}
	})
	t.Run("empty", func(t *testing.T) {
		k = []byte{}
		if !bytes.Equal(nil, k.next()) {
			t.Fatal()
		}
	})
}
