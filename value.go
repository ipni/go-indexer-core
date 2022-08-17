package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-varint"
)

var (
	_ ValueSerde = (*JsonValueSerde)(nil)
	_ ValueSerde = (*BinaryValueSerde)(nil)

	// ErrSerdeOverflow signals that unexpected size was encountered while
	// unmarshalling bytes to Value.
	ErrSerdeOverflow = errors.New("overflow")
)

type (
	// Value is the value of an index entry that is stored for each multihash in
	// the indexer.
	Value struct {
		// ProviderID is the peer ID of the provider of the multihash.
		ProviderID peer.ID `json:"p"`
		// ContextID identifies the metadata that is part of this value.
		ContextID []byte `json:"c"`
		// MetadataBytes is serialized metadata. The is kept serialized, because
		// the indexer only uses the serialized form of this data.
		MetadataBytes []byte `json:"m,omitempty"`
	}

	// ValueSerde represents Value serializer and deserializer to/from bytes.
	ValueSerde interface {
		// MarshalValue serializes a single value.
		MarshalValue(Value) ([]byte, error)
		// UnmarshalValue deserializes a single value.
		UnmarshalValue(b []byte) (Value, error)
		// MarshalValueKeys serializes a Value list for storage.
		MarshalValueKeys([][]byte) ([]byte, error)
		// UnmarshalValueKeys deserializes value keys list.
		UnmarshalValueKeys([]byte) ([][]byte, error)
	}

	// JsonValueSerde serializes and deserializes Value as JSON.
	// See: json.Marshal, json.Unmarshal
	JsonValueSerde struct{}

	// BinaryValueSerde serializes and deserializes Value as binary sections
	// prepended with byte length as varint.
	BinaryValueSerde struct{}
)

// Match return true if both values have the same ProviderID and ContextID.
func (v Value) Match(other Value) bool {
	return v.ProviderID == other.ProviderID && bytes.Equal(v.ContextID, other.ContextID)
}

// Equal returns true if two Value instances are identical.
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

func (JsonValueSerde) MarshalValue(v Value) ([]byte, error) {
	return json.Marshal(&v)
}

func (JsonValueSerde) UnmarshalValue(b []byte) (v Value, err error) {
	err = json.Unmarshal(b, &v)
	return
}

func (JsonValueSerde) MarshalValueKeys(vk [][]byte) ([]byte, error) {
	return json.Marshal(&vk)
}

func (JsonValueSerde) UnmarshalValueKeys(b []byte) (vk [][]byte, err error) {
	err = json.Unmarshal(b, &vk)
	return
}

func (BinaryValueSerde) MarshalValue(v Value) ([]byte, error) {
	pid := []byte(v.ProviderID)
	pl := len(pid)
	upl := uint64(pl)
	cl := len(v.ContextID)
	ucl := uint64(cl)
	ml := len(v.MetadataBytes)
	uml := uint64(ml)
	size := varint.UvarintSize(upl) + pl +
		varint.UvarintSize(ucl) + cl +
		varint.UvarintSize(uml) + ml

	var buf bytes.Buffer
	buf.Grow(size)
	buf.Write(varint.ToUvarint(upl))
	buf.Write(pid)
	buf.Write(varint.ToUvarint(ucl))
	buf.Write(v.ContextID)
	buf.Write(varint.ToUvarint(uml))
	buf.Write(v.MetadataBytes)
	return buf.Bytes(), nil
}

// UnmarshalValue deserializes a single value.
//
// If a failure occurs during serialization an error is returned along with
// the partially deserialized value keys. Only nil error means complete and
// successful deserialization.
func (BinaryValueSerde) UnmarshalValue(b []byte) (Value, error) {
	var v Value
	buf := bytes.NewBuffer(b)

	// Decode provider ID.
	usize, err := varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size := int(usize)
	if size < 0 || size > buf.Len() {
		return Value{}, ErrSerdeOverflow
	}
	v.ProviderID = peer.ID(buf.Next(size))

	// Decode context ID.
	usize, err = varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size = int(usize)
	if size < 0 || size > buf.Len() {
		return v, ErrSerdeOverflow
	}
	v.ContextID = buf.Next(size)

	// Decode metadata.
	usize, err = varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size = int(usize)
	if size < 0 || size > buf.Len() {
		return v, ErrSerdeOverflow
	}
	v.MetadataBytes = buf.Next(size)
	if buf.Len() != 0 {
		return v, fmt.Errorf("too many bytes; %d remain unread", buf.Len())
	}
	return v, nil
}

func (BinaryValueSerde) MarshalValueKeys(vk [][]byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(varint.ToUvarint(uint64(len(vk))))
	for _, v := range vk {
		vl := len(v)
		uvl := uint64(vl)
		buf.Grow(varint.UvarintSize(uvl) + vl)
		buf.Write(varint.ToUvarint(uvl))
		buf.Write(v)
	}
	return buf.Bytes(), nil
}

// UnmarshalValueKeys deserializes value keys.
//
// If a failure occurs during serialization an error is returned along with
// the partially deserialized value keys. Only nil error means complete and
// successful deserialization.
func (BinaryValueSerde) UnmarshalValueKeys(b []byte) ([][]byte, error) {
	var vk [][]byte
	buf := bytes.NewBuffer(b)

	// Decode vk length.
	ulen, err := varint.ReadUvarint(buf)
	if err != nil {
		return vk, err
	}
	if ulen > math.MaxUint32 {
		return vk, ErrSerdeOverflow
	}
	l := int(ulen)
	// There should at least be l number of bytes since length of each inner byte slice length
	// should have been written, even if it was zero, and minimum size of a uvarint is a byte.
	if l < 0 || l > buf.Len() {
		return vk, ErrSerdeOverflow
	}

	// Decode each value key.
	for i := 0; i < l; i++ {
		usize, err := varint.ReadUvarint(buf)
		if err != nil {
			return vk, err
		}
		size := int(usize)
		if size < 0 || size > buf.Len() {
			return vk, ErrSerdeOverflow
		}
		vk = append(vk, buf.Next(size))
	}
	if buf.Len() != 0 {
		return vk, fmt.Errorf("too many bytes; %d remain unread", buf.Len())
	}
	return vk, nil
}
