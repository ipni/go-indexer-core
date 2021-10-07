package indexer

import (
	"bytes"

	"github.com/multiformats/go-varint"
)

// Metadata is data that provides information about retrieving
// data for an index, from a particular provider.
type Metadata struct {
	// ProtocolID defines the protocol used for data retrieval.
	ProtocolID uint64
	// Data is specific to the identified protocol, and provides data, or a
	// link to data, necessary for retrieval.
	Data []byte
}

// Equal determines if two Metadata values are equal.
func (m Metadata) Equal(other Metadata) bool {
	return m.ProtocolID == other.ProtocolID && bytes.Equal(m.Data, other.Data)
}

// EncodeMetadata serializes Metadata to []byte.
func EncodeMetadata(m Metadata) []byte {
	varintSize := varint.UvarintSize(m.ProtocolID)
	buf := make([]byte, varintSize+len(m.Data))
	varint.PutUvarint(buf, m.ProtocolID)
	if len(m.Data) != 0 {
		copy(buf[varintSize:], m.Data)
	}
	return buf
}

// DecodeMetadata deserializes []byte into Metadata.
func DecodeMetadata(data []byte) (Metadata, error) {
	protocol, len, err := varint.FromUvarint(data)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{
		ProtocolID: protocol,
		Data:       data[len:],
	}, nil
}
