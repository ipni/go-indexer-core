//go:build !386

package pebble

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/filecoin-project/go-indexer-core"
)

const valueKeysMergerName = "indexer.v1.binary.valueKeysMerger"

var (
	_ pebble.ValueMerger          = (*valueKeysValueMerger)(nil)
	_ pebble.DeletableValueMerger = (*valueKeysValueMerger)(nil)
)

type valueKeysValueMerger struct {
	merge   [][]byte
	delete  [][]byte
	reverse bool
	codec   indexer.BinaryValueCodec
}

func newValueKeysMerger() *pebble.Merger {
	return &pebble.Merger{
		Merge: func(k, value []byte) (pebble.ValueMerger, error) {
			// Fall back on default merger if the key is not of type multihash, i.e. the only key
			// type that corresponds to value-keys.
			switch key(k).prefix() {
			case multihashKeyPrefix:
				v := &valueKeysValueMerger{}
				return v, v.MergeNewer(value)
			default:
				return pebble.DefaultMerger.Merge(k, value)
			}
		},
		Name: valueKeysMergerName,
	}
}

func (v *valueKeysValueMerger) MergeNewer(value []byte) error {
	prefix, dvk := key(value).stripMergeDelete()
	switch prefix {
	case mergeDeleteKeyPrefix:
		v.delete = append(v.delete, dvk)
	case valueKeyPrefix:
		if !v.exists(value) {
			v.merge = append(v.merge, value)
		}
	default:
		// The given value is marshalled value-keys; decode it and populate merge values.
		vks, err := v.codec.UnmarshalValueKeys(value)
		if err != nil {
			return err
		}

		// Grow v.merge capacity if it is less than the upper bound for new length.
		// This is to reduce footprint of append called in a loop.
		maxLen := len(v.merge) + len(vks)
		if cap(v.merge) < maxLen {
			v.merge = append(make([][]byte, 0, maxLen), v.merge...)
		}
		for _, vk := range vks {
			if !v.exists(vk) {
				v.merge = append(v.merge, vk)
			}
		}
	}
	return nil
}

func (v *valueKeysValueMerger) MergeOlder(value []byte) error {
	v.reverse = true
	return v.MergeNewer(value)
}

func (v *valueKeysValueMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	for _, dvk := range v.delete {
		v.deleteValueKey(dvk)
	}
	if len(v.merge) == 0 {
		return nil, nil, nil
	}
	if v.reverse {
		for one, other := 0, len(v.merge)-1; one < other; one, other = one+1, other-1 {
			v.merge[one], v.merge[other] = v.merge[other], v.merge[one]
		}
	}
	b, err := v.codec.MarshalValueKeys(v.merge)
	return b, nil, err
}

func (v *valueKeysValueMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	b, c, err := v.Finish(includesBase)
	return b, len(b) == 0, c, err
}

func (v *valueKeysValueMerger) deleteValueKey(value []byte) {
	b := v.merge[:0]
	for _, x := range v.merge {
		if !bytes.Equal(x, value) {
			b = append(b, x)
		}
	}
	v.merge = b
}

func (v *valueKeysValueMerger) exists(value []byte) bool {
	for _, x := range v.merge {
		if bytes.Equal(x, value) {
			return true
		}
	}
	return false
}
