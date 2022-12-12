package pebble

import (
	"bytes"
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/multiformats/go-multihash"
)

func Test_blake3Keyer(t *testing.T) {
	var vk *key

	p := newPool()
	subject := p.leaseBlake3Keyer()

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
		if !bytes.Equal(start.buf, []byte{byte(multihashKeyPrefix)}) {
			t.Fatal()
		}
		if !bytes.Equal(end.buf, start.next().buf) {
			t.Fatal()
		}
		if start.prefix() != multihashKeyPrefix {
			t.Fatal()
		}
	})

	t.Run("valueKeyHashKey", func(t *testing.T) {
		var err error
		keyer := indexer.NewKeyer()
		k, err := keyer.Key(value2)
		if err != nil {
			t.Fatal(err)
		}

		vk = subject.valueKeyHashKey(k, false)

		if vk.prefix() != valueKeyHashPrefix {
			t.Fatal()
		}
	})

	t.Run("valueKeyMergeDelete", func(t *testing.T) {
		keyer := indexer.NewKeyer()
		k, err := keyer.Key(value2)
		if err != nil {
			t.Fatal(err)
		}

		dvk := subject.valueKeyHashKey(k, true)
		if dvk.prefix() != mergeDeleteKeyPrefix {
			t.Fatal()
		}
		if !bytes.Equal(vk.buf, dvk.buf[1:]) {
			t.Fatal()
		}
	})
}

func Test_key_next(t *testing.T) {
	k := newPool().leaseKey()
	t.Run("increment", func(t *testing.T) {
		k.buf = []byte{1, 2, 3, 4}
		if !bytes.Equal([]byte{1, 2, 3, 5}, k.next().buf) {
			t.Fatal()
		}
	})
	t.Run("incrementWith0xff", func(t *testing.T) {
		k.buf = []byte{1, 2, 3, 0xff}
		next := k.next()
		if !bytes.Equal([]byte{1, 2, 4}, next.buf) {
			t.Fatal()
		}
	})
	t.Run("0xff", func(t *testing.T) {
		k.buf = []byte{0xff}
		if k.next() != nil {
			t.Fatal()
		}
	})
	t.Run("empty", func(t *testing.T) {
		k.buf = []byte{}
		if k.next() != nil {
			t.Fatal()
		}
	})
}
