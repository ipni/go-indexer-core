package indexer

import (
	"bytes"
	"encoding/json"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multicodec"
)

// Value is the value of an index entry that is stored for each CID in the indexer.
type Value struct {
	// PrividerID is the peer ID of the provider of the CID
	ProviderID peer.ID
	// ContextID identifies the metadata that is part of this value
	ContextID []byte
	// MetadataBytes is serialized metadata.  The is kept serialized, because
	// the indexer never serializes this data.
	MetadataBytes []byte `json:",omitempty"`
}

func MakeValue(providerID peer.ID, contextID []byte, protocol multicodec.Code, data []byte) Value {
	return Value{
		ProviderID: providerID,
		ContextID:  contextID,
		MetadataBytes: EncodeMetadata(Metadata{
			ProtocolID: protocol,
			Data:       data,
		}),
	}
}

// PutData writes the protocol ID and the encoded data to Value.Metadata
func (v *Value) PutData(protocol multicodec.Code, data []byte) {
	m := Metadata{
		ProtocolID: protocol,
		Data:       data,
	}
	v.MetadataBytes = EncodeMetadata(m)
}

// GetData returns the protocol ID and the encoded data from the Value.Metadata
func (v *Value) GetData() (multicodec.Code, []byte, error) {
	metadata, err := DecodeMetadata(v.MetadataBytes)
	if err != nil {
		return 0, nil, err
	}

	return metadata.ProtocolID, metadata.Data, nil
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
func MarshalValues(vals []Value) ([]byte, error) {
	return json.Marshal(&vals)
}

// Unmarshal serialized Value list
func UnmarshalValues(b []byte) ([]Value, error) {
	vals := []Value{}
	err := json.Unmarshal(b, &vals)
	return vals, err
}
