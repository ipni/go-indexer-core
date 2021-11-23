package indexer

import (
	"bytes"
	"encoding/json"

	"github.com/libp2p/go-libp2p-core/peer"
)

// Value is the value of an index entry that is stored for each multihash in
// the indexer.
type Value struct {
	// PrividerID is the peer ID of the provider of the multihash.
	ProviderID peer.ID `json:"p"`
	// ContextID identifies the metadata that is part of this value.
	ContextID []byte `json:"c"`
	// MetadataBytes is serialized metadata.  The is kept serialized, because
	// the indexer only uses the serialized form of this data.
	MetadataBytes []byte `json:"m,omitempty"`
}

// Match return true if both values have the same ProviderID and ContextID.
func (v Value) Match(other Value) bool {
	return v.ProviderID == other.ProviderID && bytes.Equal(v.ContextID, other.ContextID)
}

// Equal returns true if two Value instances are identical
func (v Value) Equal(other Value) bool {
	return v.Match(other) && bytes.Equal(v.MetadataBytes, other.MetadataBytes)
}

// MatchEqual returns true for the first bool if both values have the same
// ProviderID and ContextID, and returns true for the second value if the
// metadata is also equal.
func (v Value) MatchEqual(other Value) (isMatch bool, isEqual bool) {
	if v.Match(other) {
		isMatch = true
		isEqual = bytes.Equal(v.MetadataBytes, other.MetadataBytes)
	}
	return
}

// MarshalValue serializes a single value
func MarshalValue(value Value) ([]byte, error) {
	return json.Marshal(&value)
}

func UnmarshalValue(b []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(b, &value)
	return value, err
}

// MarshalValues serializes a Value list for storage.
//
// TODO: Switch from JSON to a more efficient serialization
// format once we figure out the right data structure?
func MarshalValueKeys(valKeys [][]byte) ([]byte, error) {
	return json.Marshal(&valKeys)
}

// Unmarshal serialized value keys list
func UnmarshalValueKeys(b []byte) ([][]byte, error) {
	var valKeys [][]byte
	err := json.Unmarshal(b, &valKeys)
	return valKeys, err
}
