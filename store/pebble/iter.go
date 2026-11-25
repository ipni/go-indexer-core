package pebble

import (
	"context"
	"iter"

	"github.com/cockroachdb/pebble/v2"
)

// pebbleKV is a key and a value in Pebble's on-disk layout: the key starts
// with its prefix byte, and a multihash's value is the store's slot list.
type pebbleKV struct {
	key, value []byte
}

// pebbleKVs yields the keys and values that an iterator over r with opts
// reads, in key order, with one iterator closed when the loop ends. Nil opts
// read none. It checks ctx before every key, and ends with an error when ctx
// is done (its cause) or the read fails. A yielded key and value are the
// iterator's own buffers: they are valid until the next iteration, and the
// caller must not alter their bytes.
func pebbleKVs(ctx context.Context, r pebble.Reader, opts *pebble.IterOptions) iter.Seq2[pebbleKV, error] {
	return func(yield func(pebbleKV, error) bool) {
		if opts == nil {
			return
		}
		it, err := r.NewIter(opts)
		if err != nil {
			yield(pebbleKV{}, err)
			return
		}
		defer func() { _ = it.Close() }()

		for valid := it.First(); valid; valid = it.Next() {
			if err := context.Cause(ctx); err != nil {
				yield(pebbleKV{}, err)
				return
			}
			value, err := it.ValueAndErr()
			if err != nil {
				yield(pebbleKV{}, err)
				return
			}
			if !yield(pebbleKV{it.Key(), value}, nil) {
				return
			}
		}
		if err := it.Error(); err != nil {
			yield(pebbleKV{}, err)
		}
	}
}
