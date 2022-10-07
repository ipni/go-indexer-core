package pebble

import (
	"bytes"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
)

var _ indexer.ValueCodec = (*zeroCopyBinaryValueCodec)(nil)

// zeroCopyBinaryValueCodec is a fork of indexer.BinaryValueCodec which does not make a copy of
// given slices as part of unmarshalling.
type zeroCopyBinaryValueCodec struct{}

func (zeroCopyBinaryValueCodec) MarshalValue(v indexer.Value) ([]byte, error) {
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

// UnmarshalValue deserializes a single value without making any copies.
//
// If a failure occurs during serialization an error is returned along with
// the partially deserialized value keys. Only nil error means complete and
// successful deserialization.
func (zeroCopyBinaryValueCodec) UnmarshalValue(b []byte) (indexer.Value, error) {
	var v indexer.Value
	buf := bytes.NewBuffer(b)

	// Decode provider ID.
	usize, err := varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size := int(usize)
	if size < 0 || size > buf.Len() {
		return indexer.Value{}, indexer.ErrCodecOverflow
	}
	v.ProviderID = peer.ID(buf.Next(size))

	// Decode context ID.
	usize, err = varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size = int(usize)
	if size < 0 || size > buf.Len() {
		return v, indexer.ErrCodecOverflow
	}
	v.ContextID = buf.Next(size)

	// Decode metadata.
	usize, err = varint.ReadUvarint(buf)
	if err != nil {
		return v, err
	}
	size = int(usize)
	if size < 0 || size > buf.Len() {
		return v, indexer.ErrCodecOverflow
	}
	v.MetadataBytes = buf.Next(size)
	if buf.Len() != 0 {
		return v, fmt.Errorf("too many bytes; %d remain unread", buf.Len())
	}
	return v, nil
}

func (zeroCopyBinaryValueCodec) MarshalValueKeys(vk [][]byte) ([]byte, error) {
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

// UnmarshalValueKeys deserializes value keys without making any copies.
//
// If a failure occurs during serialization an error is returned along with
// the partially deserialized value keys. Only nil error means complete and
// successful deserialization.
func (zeroCopyBinaryValueCodec) UnmarshalValueKeys(b []byte) ([][]byte, error) {
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
			return vk, indexer.ErrCodecOverflow
		}
		maybeGrow(vk, 1)
		vk = append(vk, buf.Next(size))
	}
	return vk, nil
}
