package pebble

import (
	"sync"
)

const (
	pooledKeyMaxCap            = 32
	pooledKeyListMaxCap        = 32
	pooledSectionBufferMaxCap  = 1 << 10 // 1 KiB
	pooledSliceCapGrowthFactor = 2
)

type pool struct {
	blake3KeyerPool   sync.Pool
	keyPool           sync.Pool
	keyListPool       sync.Pool
	sectionBufferPool sync.Pool
}

func newPool() *pool {
	var p pool
	p.keyPool.New = func() any {
		return &key{
			buf: make([]byte, 0, pooledKeyMaxCap),
			p:   &p,
		}
	}
	p.keyListPool.New = func() any {
		return &keyList{
			keys: make([]*key, 0, pooledKeyListMaxCap),
			p:    &p,
		}
	}
	p.blake3KeyerPool.New = func() any {
		return newBlake3Keyer(defaultKeyerLength, &p)
	}
	p.sectionBufferPool.New = func() any {
		return &sectionBuffer{
			buf: make([]byte, 0, pooledSectionBufferMaxCap),
			p:   &p,
		}
	}
	return &p
}

func (p *pool) leaseBlake3Keyer() *blake3Keyer {
	return p.blake3KeyerPool.Get().(*blake3Keyer)
}

func (p *pool) leaseKey() *key {
	return p.keyPool.Get().(*key)
}

func (p *pool) leaseKeyList() *keyList {
	return p.keyListPool.Get().(*keyList)
}

func (p *pool) leaseSectionBuff() *sectionBuffer {
	return p.sectionBufferPool.Get().(*sectionBuffer)
}
