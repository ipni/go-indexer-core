package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
)

var (
	_ ValueCodec = (*JsonValueCodec)(nil)
	_ ValueCodec = (*BinaryValueCodec)(nil)
	_ ValueCodec = (*BinaryWithJsonFallbackCodec)(nil)

	// ErrCodecOverflow signals that unexpected size was encountered while
	// unmarshalling bytes to Value.
	ErrCodecOverflow = errors.New("overflow")
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

	// ValueCodec represents Value serializer and deserializer to/from bytes.
	ValueCodec interface {
		// MarshalValue serializes a single value.
		MarshalValue(Value) ([]byte, error)
		// UnmarshalValue deserializes a single value.
		UnmarshalValue(b []byte) (Value, error)
		// MarshalValueKeys serializes a Value list for storage.
		MarshalValueKeys([][]byte) ([]byte, error)
		// UnmarshalValueKeys deserializes value keys list.
		UnmarshalValueKeys([]byte) ([][]byte, error)
	}

	// JsonValueCodec serializes and deserializes Value as JSON.
	// See: json.Marshal, json.Unmarshal
	JsonValueCodec struct{}

	// BinaryValueCodec serializes and deserializes Value as binary sections
	// prepended with byte length as varint.
	BinaryValueCodec struct{}

	// BinaryWithJsonFallbackCodec always serialises values as binary but deserializes
	// both from binary and JSON, which gracefully and opportunistically migrates codec
	// from JSON to the more efficient binary format.
	BinaryWithJsonFallbackCodec struct {
		BinaryValueCodec
		JsonValueCodec
	}
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

func (JsonValueCodec) MarshalValue(v Value) ([]byte, error) {
	return json.Marshal(&v)
}

func (JsonValueCodec) UnmarshalValue(b []byte) (v Value, err error) {
	err = json.Unmarshal(b, &v)
	return
}

func (JsonValueCodec) MarshalValueKeys(vk [][]byte) ([]byte, error) {
	return json.Marshal(&vk)
}

func (JsonValueCodec) UnmarshalValueKeys(b []byte) (vk [][]byte, err error) {
	err = json.Unmarshal(b, &vk)
	return
}

func (BinaryValueCodec) MarshalValue(v Value) ([]byte, error) {
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
func (BinaryValueCodec) UnmarshalValue(b []byte) (Value, error) {
	var v Value
	buf := bytes.NewBuffer(b)

	// Decode provider ID.
	usize, err := varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size := int(usize)
	if size < 0 || size > buf.Len() {
		return Value{}, ErrCodecOverflow
	}
	v.ProviderID = peer.ID(buf.Next(size))

	// Decode context ID.
	usize, err = varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size = int(usize)
	if size < 0 || size > buf.Len() {
		return v, ErrCodecOverflow
	}
	v.ContextID = make([]byte, size)
	buf.Read(v.ContextID)

	// Decode metadata.
	usize, err = varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size = int(usize)
	if size < 0 || size > buf.Len() {
		return v, ErrCodecOverflow
	}
	v.MetadataBytes = make([]byte, size)
	buf.Read(v.MetadataBytes)
	if buf.Len() != 0 {
		return v, fmt.Errorf("too many bytes; %d remain unread", buf.Len())
	}
	return v, nil
}

func (BinaryValueCodec) MarshalValueKeys(vk [][]byte) ([]byte, error) {
	var buf bytes.Buffer
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
func (BinaryValueCodec) UnmarshalValueKeys(b []byte) ([][]byte, error) {
	var vk [][]byte
	buf := bytes.NewBuffer(b)
	// Decode each value key.
	for buf.Len() != 0 {
		usize, err := varint.ReadUvarint(buf)
		if err != nil {
			return vk, err
		}
		size := int(usize)
		if size < 0 || size > buf.Len() {
			return vk, ErrCodecOverflow
		}
		vkData := make([]byte, size)
		buf.Read(vkData)
		vk = append(vk, vkData)
	}
	return vk, nil
}

func (bjc BinaryWithJsonFallbackCodec) MarshalValue(v Value) ([]byte, error) {
	return bjc.BinaryValueCodec.MarshalValue(v)
}

func (bjc BinaryWithJsonFallbackCodec) UnmarshalValue(b []byte) (Value, error) {
	v, err := bjc.BinaryValueCodec.UnmarshalValue(b)
	if err == nil {
		return v, nil
	}
	// If b does not look like JSON, i.e. a JSON object with no whitespace at head or tail,
	//  return the binary unmarshal error.
	bl := len(b)
	if bl < 2 || b[0] != '{' || b[bl-1] != '}' {
		return Value{}, err
	}
	return bjc.JsonValueCodec.UnmarshalValue(b)
}

func (bjc BinaryWithJsonFallbackCodec) MarshalValueKeys(vk [][]byte) ([]byte, error) {
	return bjc.BinaryValueCodec.MarshalValueKeys(vk)
}

func (bjc BinaryWithJsonFallbackCodec) UnmarshalValueKeys(b []byte) ([][]byte, error) {
	v, err := bjc.BinaryValueCodec.UnmarshalValueKeys(b)
	if err == nil {
		return v, nil
	}
	// If b does not look like JSON, i.e. a JSON array with no whitespace at head or tail,
	//  return the binary unmarshal error.
	bl := len(b)
	if bl < 2 || b[0] != '[' || b[bl-1] != ']' {
		return nil, err
	}
	return bjc.JsonValueCodec.UnmarshalValueKeys(b)
}
