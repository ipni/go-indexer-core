package pebble

import (
	"io"

	"github.com/ipni/go-indexer-core"
	"github.com/multiformats/go-varint"
)

var _ io.Closer = (*sectionBuffer)(nil)

// sectionBuffer offers an efficient way to write and copy byte slices prefixed with their length as
// varint, referred to as "section". This implementation uses byte slice directly to offer better
// throughput in comparison with bytes.Buffer.
type sectionBuffer struct {
	buf     []byte
	written int
	read    int
	p       *pool
}

func (bb *sectionBuffer) wrap(d []byte) {
	bb.buf = append(bb.buf[0:], d...)
	bb.written = len(d)
}

func (bb *sectionBuffer) writeSection(b []byte) {
	l := len(b)
	ul := uint64(l)
	size := varint.UvarintSize(ul)
	bb.maybeGrow(size)
	bb.buf = bb.buf[:bb.written+size]
	bb.written += varint.PutUvarint(bb.buf[bb.written:], ul)
	bb.buf = append(bb.buf, b...)
	bb.written += l
}

func (bb *sectionBuffer) copyNextSection() ([]byte, error) {
	usize, read, err := varint.FromUvarint(bb.buf[bb.read:])
	bb.read += read
	if err != nil {
		return nil, err
	}
	size := int(usize)
	if size < 0 || size > bb.remaining() {
		return nil, indexer.ErrCodecOverflow
	}

	section := make([]byte, size)
	copy(section, bb.buf[bb.read:size+bb.read])
	bb.read += size
	return section, nil
}

func (bb *sectionBuffer) maybeGrow(n int) {
	l := len(bb.buf)
	switch {
	case n <= cap(bb.buf)-l:
	case l == 0:
		bb.buf = make([]byte, 0, n*pooledSliceCapGrowthFactor)
	default:
		bb.buf = append(make([]byte, 0, (l+n)*pooledSliceCapGrowthFactor), bb.buf...)
	}
}

func (bb *sectionBuffer) Close() error {
	if cap(bb.buf) <= pooledSectionBufferMaxCap {
		bb.buf = bb.buf[:0]
		bb.written = 0
		bb.read = 0
		bb.p.sectionBufferPool.Put(bb)
	}
	return nil
}

func (bb *sectionBuffer) remaining() int {
	return len(bb.buf[bb.read:bb.written])
}
