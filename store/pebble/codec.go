package pebble

import (
	"fmt"
	"io"

	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	// marshalledValueKeyLength length is the default key length plus the length of the prefix i.e. 1, plus
	// the length of its varint length which is also 1.
	marshalledValueKeyLength = defaultKeyerLength + 1 + 1
)

// codec offers marshalling compatible with indexer.BinaryValueCodec format but optimised for use
// in pebble; it does not make copies when possible and returns reusable pooled sectionBuffer,
// key and keyList to reduce memory footprint where possible.
type codec struct {
	p *pool
}

func (c *codec) marshalValue(v *indexer.Value) ([]byte, io.Closer, error) {
	buf := c.p.leaseSectionBuff()
	buf.writeSection([]byte(v.ProviderID))
	buf.writeSection(v.ContextID)
	buf.writeSection(v.MetadataBytes)
	return buf.buf, buf, nil
}

func (c *codec) unmarshalValue(b []byte) (*indexer.Value, error) {
	buf := c.p.leaseSectionBuff()
	defer buf.Close()
	buf.wrap(b)

	// Decode provider ID.
	section, err := buf.copyNextSection()
	if err != nil {
		return nil, err
	}
	var v indexer.Value
	v.ProviderID = peer.ID(section)

	// Decode context ID.
	section, err = buf.copyNextSection()
	if err != nil {
		return nil, err
	}
	v.ContextID = section

	// Decode metadata.
	section, err = buf.copyNextSection()
	if err != nil {
		return nil, err
	}
	v.MetadataBytes = section
	if buf.remaining() != 0 {
		return nil, fmt.Errorf("too many bytes; %d remain unread", buf.remaining())
	}
	return &v, nil
}

func (c *codec) marshalValueKeys(vk [][]byte) ([]byte, io.Closer, error) {
	buf := c.p.leaseSectionBuff()
	buf.maybeGrow(marshalledValueKeyLength * len(vk))
	for _, v := range vk {
		buf.writeSection(v)
	}
	return buf.buf, buf, nil
}

func (c *codec) unmarshalValueKeys(b []byte) (*keyList, error) {
	l := len(b)
	if l == 0 {
		return nil, nil
	}
	vkl := l / marshalledValueKeyLength
	if vkl < 1 {
		return nil, fmt.Errorf("marshalled value-key length %d is shorter than expected minimum %d", l, marshalledValueKeyLength)
	}
	vks := c.p.leaseKeyList()
	vks.maybeGrow(vkl)
	for i := 0; i < vkl; i++ {
		vk := c.p.leaseKey()
		offset := marshalledValueKeyLength * i
		vk.append(b[offset+1 : offset+marshalledValueKeyLength]...)
		prefix := vk.prefix()
		if prefix != valueKeyPrefix {
			log.Debugf("unexpected key prefix for key: %v", vk)
			_ = vk.Close()
			continue
		}
		vks.append(vk)
	}
	if l%marshalledValueKeyLength != 0 {
		return vks, indexer.ErrCodecOverflow
	}
	return vks, nil
}
