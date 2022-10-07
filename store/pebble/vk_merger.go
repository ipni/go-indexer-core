package pebble

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble"
)

const valueKeysMergerName = "indexer.v1.binary.valueKeysMerger"

var (
	_ pebble.ValueMerger          = (*valueKeysValueMerger)(nil)
	_ pebble.DeletableValueMerger = (*valueKeysValueMerger)(nil)
)

type valueKeysValueMerger struct {
	merges  [][]byte
	deletes map[string]struct{}
	reverse bool
	codec   zeroCopyBinaryValueCodec
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
		v.addToDeletes(dvk)
	case valueKeyPrefix:
		v.addToMerges(value)
	default:
		return v.mergeMarshalled(value)
	}
	return nil
}

// mergeMarshalled extracts value-keys by unmarshalling the given value and adds them to merges.
// This function recursively unmarshalls value-keys to gracefully correct previous behaviour
// of this merger where in certain scenarios already marshalled value-keys may have been
// re-marshalled. This assures that any such records are opportunistically unmarshalled and
// de-duplicated whenever they are read or changed.
//
// See: https://github.com/filecoin-project/go-indexer-core/issues/94
func (v *valueKeysValueMerger) mergeMarshalled(value []byte) error {
	// The given value is marshalled value-keys; decode it and populate merge values.
	vks, err := v.codec.UnmarshalValueKeys(value)
	if err != nil {
		return err
	}
	// Attempt to grow the capacity of v.merge to reduce append footprint loop below.
	v.merges = maybeGrow(v.merges, len(vks))
	for _, vk := range vks {
		// Recursively merge the value key to accommodate previous behaviour of the value-key
		// merger, where the value-keys may have been marshalled multiple times.
		// Recursion here will gracefully and opportunistically correct any such cases.
		if err := v.MergeNewer(vk); err != nil {
			return err
		}
	}
	return nil
}

func (v *valueKeysValueMerger) MergeOlder(value []byte) error {
	v.reverse = true
	return v.MergeNewer(value)
}

func (v *valueKeysValueMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	v.prune()
	if len(v.merges) == 0 {
		return nil, nil, nil
	}
	if v.reverse {
		for one, other := 0, len(v.merges)-1; one < other; one, other = one+1, other-1 {
			v.merges[one], v.merges[other] = v.merges[other], v.merges[one]
		}
	}
	b, err := v.codec.MarshalValueKeys(v.merges)
	return b, nil, err
}

func (v *valueKeysValueMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	b, c, err := v.Finish(includesBase)
	return b, len(b) == 0, c, err
}

// prune removes value-keys that are present in deletes from merges
func (v *valueKeysValueMerger) prune() {
	pruned := v.merges[:0]
	for _, x := range v.merges {
		if _, ok := v.deletes[string(x)]; !ok {
			pruned = append(pruned, x)
		}
	}
	v.merges = pruned
}

// addToMerges checks whether the given value exists and if not adds it to the list of merges.
func (v *valueKeysValueMerger) addToMerges(value []byte) {
	if !v.exists(value) {
		dst := make([]byte, len(value))
		copy(dst, value)
		v.merges = append(v.merges, dst)
	}
}

// exists checks whether the given value is already present, either pending merge or deletion.
func (v *valueKeysValueMerger) exists(value []byte) bool {
	if _, pendingDelete := v.deletes[string(value)]; pendingDelete {
		return true
	}
	for _, x := range v.merges {
		if bytes.Equal(x, value) {
			return true
		}
	}
	return false
}

// addToMerges checks whether the given value exists and if not adds it to the list of deletes.
func (v *valueKeysValueMerger) addToDeletes(value []byte) {
	if v.deletes == nil {
		// Lazily instantiate the deletes map since deletions are far less common than merges.
		v.deletes = make(map[string]struct{})
	}
	v.deletes[string(value)] = struct{}{}
}

// maybeGrow grows the capacity of the given slice if necessary, such that it can fit n more
// elements and returns the resulting slice.
func maybeGrow(s [][]byte, n int) [][]byte {
	const growthFactor = 2
	l := len(s)
	switch {
	case n <= cap(s)-l:
		return s
	case l == 0:
		return make([][]byte, 0, n*growthFactor)
	default:
		return append(make([][]byte, 0, (l+n)*growthFactor), s...)
	}
}
