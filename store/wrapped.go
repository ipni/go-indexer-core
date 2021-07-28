package store

import (
	"bytes"
	"encoding/json"

	"github.com/filecoin-project/go-indexer-core/entry"
	mh "github.com/multiformats/go-multihash"
)

const (
	hashAlg  = mh.SHA2_256
	hashLen  = 32
	mhashLen = 32 + 2
)

// Entry representation for the entry store.
type WrappedValue struct {
	Value entry.Value
	RefC  uint64
}

// Marshal StoreEntry for storage and compute the key
func Marshal(li *WrappedValue) ([]byte, error) {
	return json.Marshal(li)
}

// Unmarshal StoreEntry from storage
func Unmarshal(b []byte) (*WrappedValue, error) {
	li := &WrappedValue{}
	err := json.Unmarshal(b, li)
	return li, err
}

// JoinKs joins a list of entry keys into the same byte array
func JoinKs(b [][]byte) []byte {
	return bytes.Join(b, nil)
}

// SplitKs splits entry keys from a byte array
func SplitKs(b []byte) [][]byte {
	// NOTE: We could consider using bytes.Split here
	// but this requires adding a separator.
	// We can save ourselves from using separators as
	// we know in advance the size multihashes will have.
	out := [][]byte{}
	for i := 0; i < len(b); i += mhashLen {
		t := b[i:(i + mhashLen)]
		out = append(out, t)
	}
	return out
}

// EntryKey computes the key for an entry pointer
func EntryKey(in entry.Value) ([]byte, error) {
	b, err := entry.Marshal(in)
	if err != nil {
		return nil, err
	}
	h, err := mh.Sum(b, hashAlg, hashLen)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// DuplicateEntry checks if the key for the entry is already there.
func DuplicateEntry(k []byte, old [][]byte) bool {
	for i := range old {
		if bytes.Equal(old[i], k) {
			return true
		}
	}
	return false
}
