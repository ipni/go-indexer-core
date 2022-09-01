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
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			v := &valueKeysValueMerger{}
			return v, v.MergeNewer(value)
		},
		Name: valueKeysMergerName,
	}
}

func (v *valueKeysValueMerger) MergeNewer(value []byte) error {
	dvk, ok := key(value).stripMergeDelete()
	switch {
	case ok:
		v.delete = append(v.delete, dvk)
	case !v.exists(value):
		v.merge = append(v.merge, value)
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
