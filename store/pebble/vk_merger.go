package pebble

import (
	"bytes"
	"errors"
	"io"

	"github.com/cockroachdb/pebble/v2"
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
	c       *codec
}

func newValueKeysMerger(c *codec) *pebble.Merger {
	return &pebble.Merger{
		Merge: func(k, value []byte) (pebble.ValueMerger, error) {
			// Use specialized merger for multihash keys.
			if keyPrefix(k[0]) == multihashKeyPrefix {
				v := &valueKeysValueMerger{c: c}
				return v, v.MergeNewer(value)
			}
			// Use default merger for non-multihash type keys, i.e. the
			// only key type that corresponds to value-keys.
			return pebble.DefaultMerger.Merge(k, value)
		},
		Name: valueKeysMergerName,
	}
}

func (v *valueKeysValueMerger) MergeNewer(value []byte) error {
	if len(value) == 0 {
		return nil
	}
	// Look at value prefix to determine if this value is being added to or
	// removed from the set of values the multihash key maps to.
	switch keyPrefix(value[0]) {
	case mergeDeleteKeyPrefix:
		v.addToDeletes(value[1:])
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
// See: https://github.com/ipni/go-indexer-core/issues/94
func (v *valueKeysValueMerger) mergeMarshalled(value []byte) error {
	offset := len(value) % marshalledValueKeyLength
	if offset < 0 {
		return errors.New("invalid marshalled value key")
	}

	// The given value is marshalled value-keys; decode it and populate merge values.
	vks, err := v.c.unmarshalValueKeys(value[offset:])
	if err != nil {
		return err
	}
	defer vks.Close()
	// Attempt to grow the capacity of v.merge to reduce append footprint loop below.
	v.merges = maybeGrow(v.merges, len(vks.keys))
	for _, vk := range vks.keys {
		// Recursively merge the value key to accommodate previous behaviour of the value-key
		// merger, where the value-keys may have been marshalled multiple times.
		// Recursion here will gracefully and opportunistically correct any such cases.
		v.addToMerges(vk.buf)
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
	return v.c.marshalValueKeys(v.merges)
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
