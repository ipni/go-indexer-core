package indexer

import (
	"bytes"
	"encoding/json"

	peer "github.com/libp2p/go-libp2p-core/peer"
	varint "github.com/multiformats/go-varint"
)

// Value is the value of an index entry that is stored for each CID in the indexer.
type Value struct {
	// PrividerID is the peer ID of the provider of the CID
	ProviderID peer.ID
	// ContextID identifies the metadata that is part of this value
	ContextID []byte
	// Metadata is serialized data that provides information about retrieving
	// data, for the indexed CID, from the identified provider.
	Metadata []byte `json:",omitempty"`
}

func MakeValue(providerID peer.ID, contextID []byte, protocol uint64, data []byte) Value {
	return Value{
		ProviderID: providerID,
		ContextID:  contextID,
		Metadata:   encodeMetadata(protocol, data),
	}
}

// PutData writes the protocol ID and the encoded data to Value.Metadata
func (v *Value) PutData(protocol uint64, data []byte) {
	v.Metadata = encodeMetadata(protocol, data)
}

// GetData returns the protocol ID and the encoded data from the Value.Metadata
func (v *Value) GetData() (uint64, []byte, error) {
	protocol, len, err := varint.FromUvarint(v.Metadata)
	if err != nil {
		return 0, nil, err
	}
	return protocol, v.Metadata[len:], nil
}

// Equal returns true if two Value instances are identical
func (v Value) Equal(other Value) bool {
	return v.ProviderID == other.ProviderID &&
		bytes.Equal(v.ContextID, other.ContextID) &&
		bytes.Equal(v.Metadata, other.Metadata)
}

// Match return true if both values have the same ProviderID and ContextID.
func (v Value) Match(other Value) bool {
	return v.ProviderID == other.ProviderID && bytes.Equal(v.ContextID, other.ContextID)
}

// MatchEqual returns true for the first bool if both values have the same
// ProviderID and ContextID, and returns true for the second value if the
// metadata is also equal.
func (v Value) MatchEqual(other Value) (isMatch bool, isEqual bool) {
	if v.Match(other) {
		isMatch = true
		isEqual = bytes.Equal(v.Metadata, other.Metadata)
	}
	return
}

func encodeMetadata(protocol uint64, data []byte) []byte {
	varintSize := varint.UvarintSize(protocol)
	buf := make([]byte, varintSize+len(data))
	varint.PutUvarint(buf, protocol)
	if len(data) != 0 {
		copy(buf[varintSize:], data)
	}
	return buf
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
func MarshalValues(vals []Value) ([]byte, error) {
	return json.Marshal(&vals)
}

// Unmarshal serialized Value list
func UnmarshalValues(b []byte) ([]Value, error) {
	vals := []Value{}
	err := json.Unmarshal(b, &vals)
	return vals, err
}
