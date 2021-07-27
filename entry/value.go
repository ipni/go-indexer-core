package entry

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
	// Metadata is serialized data that provides information about retrieving
	// data, for the indexed CID, from the identified provider.
	Metadata []byte
}

func MakeValue(providerID peer.ID, protocol uint64, data []byte) Value {
	return Value{
		ProviderID: providerID,
		Metadata:   encodeMetadata(protocol, data),
	}
}

// PutData writes the protocol ID and the encoded data to Value.Metadata
func (ie *Value) PutData(protocol uint64, data []byte) {
	ie.Metadata = encodeMetadata(protocol, data)
}

// GetData returns the protocol ID and the encoded data from the Value.Metadata
func (ie *Value) GetData() (uint64, []byte, error) {
	protocol, len, err := varint.FromUvarint(ie.Metadata)
	if err != nil {
		return 0, nil, err
	}
	return protocol, ie.Metadata[len:], nil
}

// Equal returns true if two Value instances are identical
func (ie Value) Equal(other Value) bool {
	return ie.ProviderID == other.ProviderID && bytes.Equal(ie.Metadata, other.Metadata)
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

// Marshal serializes a Value list for storage
// TODO: Switch from JSON to a more efficient serialization
// format once we figure out the right data structure?
func Marshal(li Value) ([]byte, error) {
	return json.Marshal(&li)
}

// Unmarshal serialized Value list
func Unmarshal(b []byte) (Value, error) {
	li := Value{}
	err := json.Unmarshal(b, &li)
	return li, err
}
