package pebble

import (
	"bytes"
	"io"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

const valueKeysMergerName = "indexer.v1.binary.valueKeysMerger"

var (
	_ pebble.ValueMerger          = (*valueKeysValueMerger)(nil)
	_ pebble.DeletableValueMerger = (*valueKeysValueMerger)(nil)
)

type valueKeysValueMerger struct {
	merges  map[string]struct{}
	deletes map[string]struct{}
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
	switch keyPrefix(value[0]) {
	case legacyMergeDeleteKeyPrefix:
		v.mergeNewerDelete(string(value[1:]))
	case mergeDeleteValueKeyPrefix:
		v.mergeNewerDelete(reconstructValueKey(value))
	case valueKeyPrefix:
		v.mergeNewerAdd(string(value))
	default:
		return v.mergeMarshalledNewer(value)
	}
	return nil
}

func (v *valueKeysValueMerger) MergeOlder(value []byte) error {
	if len(value) == 0 {
		return nil
	}
	switch keyPrefix(value[0]) {
	case legacyMergeDeleteKeyPrefix:
		v.mergeOlderDelete(string(value[1:]))
	case mergeDeleteValueKeyPrefix:
		v.mergeOlderDelete(reconstructValueKey(value))
	case valueKeyPrefix:
		v.mergeOlderAdd(string(value))
	default:
		return v.mergeMarshalledOlder(value)
	}
	return nil
}

func (v *valueKeysValueMerger) mergeNewerDelete(vk string) {
	// If delete is newer, it removes existing live slots.
	v.removeFromMerges(vk)
	v.addToDeletes(vk)
}

func (v *valueKeysValueMerger) mergeOlderDelete(vk string) {
	// If delete is older, it adds to the delete set, existing slots are unaffected
	// as those are chronologically after the delete operation.
	v.addToDeletes(vk)
}

// mergeMarshalledNewer unpacks a previous merge result and applies MergeNewer
// rules to each slot: live slots are added, delete slots remove a live slot
// and join the delete set.
func (v *valueKeysValueMerger) mergeMarshalledNewer(value []byte) error {
	vks, err := v.unpackSlots(value)
	if err != nil {
		return err
	}
	defer vks.Close()

	for _, vk := range vks.keys {
		if err := v.MergeNewer(vk.buf); err != nil {
			return err
		}
	}

	return nil
}

// mergeMarshalledOlder unpacks a previous merge result. Live slots that are not
// already deleted are added first. Delete slots then extend the delete set.
// Packed results store deletes after live slots, so that order matches.
func (v *valueKeysValueMerger) mergeMarshalledOlder(value []byte) error {
	vks, err := v.unpackSlots(value)
	if err != nil {
		return err
	}
	defer vks.Close()

	for _, vk := range vks.keys {
		if err := v.MergeOlder(vk.buf); err != nil {
			return err
		}
	}

	return nil
}

func (v *valueKeysValueMerger) unpackSlots(value []byte) (*keyList, error) {
	offset := len(value) % marshalledValueKeyLength
	return v.c.unmarshalValueKeys(value[offset:])
}

func (v *valueKeysValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	out := make([][]byte, 0, len(v.merges)+len(v.deletes))
	for merge := range v.merges {
		out = append(out, []byte(merge))
	}
	// Order among live keys is not significant; sort for a stable encoding.
	slices.SortFunc(out, bytes.Compare)
	if !includesBase {
		// Only emit delete marks for keys that are not live. A key in both
		// merges and deletes was re-added after a delete; the add is newer, so
		// serializing the delete would make a later MergeNewer of this blob
		// remove the value again.
		dks := make([][]byte, 0, len(v.deletes))
		for deleted := range v.deletes {
			if _, live := v.merges[deleted]; live {
				continue
			}
			dk := make([]byte, len(deleted))
			dk[0] = byte(mergeDeleteValueKeyPrefix)
			copy(dk[1:], deleted[1:])
			dks = append(dks, dk)
		}
		slices.SortFunc(dks, bytes.Compare)
		out = append(out, dks...)
	}
	if len(out) == 0 {
		return nil, nil, nil
	}
	return v.c.marshalValueKeys(out)
}

func (v *valueKeysValueMerger) DeletableFinish(includesBase bool) ([]byte, bool, io.Closer, error) {
	b, c, err := v.Finish(includesBase)
	return b, len(b) == 0, c, err
}

func (v *valueKeysValueMerger) mergeNewerAdd(value string) {
	v.addToMerges(value)
}

func (v *valueKeysValueMerger) mergeOlderAdd(value string) {
	if !v.isDeleted(value) {
		v.addToMerges(value)
	}
}

func (v *valueKeysValueMerger) addToMerges(value string) {
	if v.merges == nil {
		v.merges = make(map[string]struct{})
	}
	v.merges[value] = struct{}{}
}

func (v *valueKeysValueMerger) removeFromMerges(vk string) {
	delete(v.merges, vk)
}

func (v *valueKeysValueMerger) isDeleted(value string) bool {
	_, ok := v.deletes[value]
	return ok
}

func (v *valueKeysValueMerger) addToDeletes(value string) {
	if v.deletes == nil {
		// Lazily instantiate the deletes map since deletions are far less common than merges.
		v.deletes = make(map[string]struct{})
	}
	v.deletes[value] = struct{}{}
}

// reconstructValueKey builds the live value-key string for a compact delete
// operand by replacing mergeDeleteValueKeyPrefix with valueKeyPrefix.
func reconstructValueKey(value []byte) string {
	b := strings.Builder{}
	b.Grow(len(value))
	b.WriteByte(byte(valueKeyPrefix))
	b.Write(value[1:])
	return b.String()
}
