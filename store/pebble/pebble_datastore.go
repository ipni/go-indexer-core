package pebble

import (
	"errors"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/ipni/go-indexer-core"
	dstore "github.com/ipni/go-indexer-core/store"
	"github.com/multiformats/go-multihash"
	"lukechampine.com/blake3"
)

var (
	_ dstore.Interface = (*dhstore)(nil)
	_ indexer.Iterator = (*dhiterator)(nil)
)

type (
	dhstore struct {
		db *pebble.DB
		// Only support binary format since in pebble we need the capability to merge keys and
		// there is little reason for store values in any format other than binary for performance
		// characteristics.
		// Note, pebble is using a zero-copy variation of marshaller to allow optimizations in
		// cases where the value need not to be copied. The root level binary codec copies on
		// unmarshal every time.
		vcodec *codec
		// Two different value pools are required because of different key lengths for encrypted value keys.
		p        *pool
		closed   bool
		comparer *pebble.Comparer
		hasher   *blake3.Hasher
	}
	dhiterator struct {
		closed   bool
		snapshot *pebble.Snapshot
		it       *pebble.Iterator
		vcodec   *codec
		p        *pool
		start    *key
		end      *key
	}
)

// New instantiates a new instance of a store backed by Pebble.
// Note that any Merger value specified in the given options will be overridden.
func NewDatastore(path string, opts *pebble.Options) (dstore.Interface, error) {
	p := newPool()
	c := &codec{
		p:      p,
		prefix: valueKeyHashPrefix,
	}
	if opts == nil {
		opts = &pebble.Options{}
	}
	opts.EnsureDefaults()
	// Override Merger since the store relies on a specific implementation of it
	// to handle read-free writing of value-keys; see: valueKeysValueMerger.
	opts.Merger = newValueKeysMerger(c)
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &dhstore{
		db:       db,
		p:        p,
		vcodec:   c,
		comparer: opts.Comparer,
		hasher:   blake3.New(defaultKeyerLength, nil),
	}, nil
}

func (s *dhstore) GetValueKeys(mh multihash.Multihash) ([][]byte, bool, error) {
	keygen := s.p.leaseBlake3Keyer()
	mhk, err := keygen.multihashKey(mh)
	_ = keygen.Close()
	if err != nil {
		return nil, false, err
	}
	vkhb, vkhbClose, err := s.db.Get(mhk.buf)
	_ = mhk.Close()
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		log.Errorw("can't find multihash", "err", err)
		return nil, false, err
	}
	vkhs, err := s.vcodec.unmarshalValueKeys(vkhb)
	if err != nil {
		return nil, false, err
	}
	_ = vkhbClose.Close()
	defer vkhs.Close()

	result := make([][]byte, 0, len(vkhs.keys))
	for _, vkh := range vkhs.keys {
		vk, c, err := s.db.Get(vkh.buf)
		if err != nil {
			return nil, false, err
		}
		vkCpy := make([]byte, len(vk))
		copy(vkCpy, vk)
		_ = c.Close()

		result = append(result, vkCpy[:])
	}
	return result, len(result) != 0, nil
}

func (s *dhstore) GetValue(valKey []byte) (*indexer.Value, error) {
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()

	vk := keygen.valueKeyFromPayload(valKey)
	vs, vCloser, err := s.db.Get(vk.buf)
	_ = vk.Close()

	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		log.Errorw("can't find value", "err", err)
		return nil, err
	}
	vcpy := make([]byte, len(vs))
	copy(vcpy, vs)
	_ = vCloser.Close()

	v, err := s.vcodec.unmarshalValue(vcpy)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (s *dhstore) NewBatch() interface{} {
	return s.db.NewBatch()
}

func (s *dhstore) CommitBatch(batch interface{}) error {
	b := batch.(*pebble.Batch)
	return b.Commit(pebble.NoSync)
}

func (s *dhstore) CloseBatch(batch interface{}) error {
	b := batch.(*pebble.Batch)
	return b.Close()
}

func (s *dhstore) PutValue(valKey []byte, v indexer.Value, batch interface{}) error {
	b := batch.(*pebble.Batch)

	if len(v.MetadataBytes) == 0 {
		return errors.New("value missing metadata")
	}

	keygen := s.p.leaseBlake3Keyer()
	vk := keygen.valueKeyFromPayload(valKey)
	_ = keygen.Close()
	defer vk.Close()

	vs, c, err := s.vcodec.marshalValue(&v)
	if err != nil {
		return err
	}

	defer c.Close()
	// Don't bother checking if the value has changed, and write it anyway. Because, otherwise
	// we need to use an IndexedBatch which is generally slower than Batch, and the majority
	// of writing here is the writing of multihashes.
	//
	// TODO: experiment to see if it is indeed faster to always write the value instead of
	//       check if it's changed before writing.
	return b.Set(vk.buf, vs, pebble.NoSync)
}

func (s *dhstore) PutValueKey(mh multihash.Multihash, valKey []byte, batch interface{}) error {
	b := batch.(*pebble.Batch)

	// Generate a hash of the value key. This is needed as value keys can be of variable length but we want to convert them to fixed length instead
	vkh, err := s.valueKeyHash(valKey)
	if err != nil {
		return err
	}

	keygen := s.p.leaseBlake3Keyer()
	vkhKey := keygen.valueKeyHashKey(vkh, false)
	mhk, err := keygen.multihashKey(mh)
	_ = keygen.Close()

	if err != nil {
		return err
	}

	// Persist a mapping from the hashed value key to the original value key
	err = b.Set(vkhKey.buf, valKey, pebble.NoSync)
	if err != nil {
		return err
	}

	if err := b.Merge(mhk.buf, vkhKey.buf, pebble.NoSync); err != nil {
		return err
	}

	_ = vkhKey.Close()
	_ = mhk.Close()

	return nil
}

func (s *dhstore) RemoveValueKey(mh multihash.Multihash, valKey []byte, batch interface{}) error {
	b := batch.(*pebble.Batch)
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()
	mhk, err := keygen.multihashKey(mh)
	if err != nil {
		return err
	}
	defer mhk.Close()

	// Generate a hash of the value key
	vkh, err := s.valueKeyHash(valKey)
	if err != nil {
		return err
	}

	vkhKey := keygen.valueKeyHashKey(vkh, true)
	defer vkhKey.Close()

	// Remove value key hash from the multihash mapping
	err = b.Merge(mhk.buf, vkhKey.buf, pebble.NoSync)
	if err != nil {
		return err
	}

	// Remove a mapping from hashed value key to value key
	return b.Delete(vkhKey.buf, pebble.NoSync)
}

func (s *dhstore) RemoveValue(valKey []byte) error {
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()
	vk := keygen.valueKeyFromPayload(valKey)
	defer vk.Close()
	return s.db.Delete(vk.buf, pebble.NoSync)
}

func (s *dhstore) Size() (int64, error) {
	sizeEstimate, err := s.db.EstimateDiskUsage([]byte{0}, []byte{0xff})
	return int64(sizeEstimate), err
}

func (s *dhstore) Flush() error {
	return s.db.Flush()
}

func (s *dhstore) Close() error {
	if s.closed {
		return nil
	}
	ferr := s.db.Flush()
	cerr := s.db.Close()
	s.closed = true
	// Prioritise on returning close errors over flush errors, since it is more likely to contain
	// useful information about the failure root cause.
	if cerr != nil {
		return cerr
	}
	return ferr
}

// Stats gathers statistics about the values indexed by Pebble store.
// The statistic gathering can be expensive and may be slow to calculate.
// The numbers returned are estimated, must not be interpreted as exact and will not include
// records that are not flushed.
func (s *dhstore) Stats() (*indexer.Stats, error) {
	sst, err := s.db.SSTables(pebble.WithProperties())
	if err != nil {
		return nil, err
	}
	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()

	start, end, err := keygen.multihashesKeyRange()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = start.Close()
		_ = end.Close()
	}()
	var stats indexer.Stats
	for _, is := range sst {
		for _, info := range is {
			// SST file entries may overlap beyont start or end key. Include all such overlapping
			// SST files in the count; this effectively implies an over estimation of unique
			// multihashes. This is fine since the number of multihashes is expected to be
			// overwhelmingly larger than provider records.
			if s.comparer.Compare(start.buf, info.Smallest.UserKey) <= 0 ||
				s.comparer.Compare(end.buf, info.Largest.UserKey) <= 0 {
				stats.MultihashCount += info.Properties.NumEntries
			}
		}
	}
	return &stats, nil
}

func (s *dhstore) Iter() (indexer.Iterator, error) {
	keygen := s.p.leaseBlake3Keyer()
	start, end, err := keygen.multihashesKeyRange()
	if err != nil {
		_ = keygen.Close()
		return nil, err
	}
	_ = keygen.Close()
	snapshot := s.db.NewSnapshot()
	iter := snapshot.NewIter(&pebble.IterOptions{
		LowerBound: start.buf,
		UpperBound: end.buf,
	})
	iter.First()
	return &iterator{
		snapshot: snapshot,
		it:       iter,
		vcodec:   s.vcodec,
		start:    start,
		end:      end,
		p:        s.p,
	}, nil
}

func (i *dhiterator) Next() (multihash.Multihash, [][]byte, error) {
	switch {
	case i.it.Error() != nil:
		return nil, nil, i.it.Error()
	case !i.it.Valid():
		return nil, nil, io.EOF
	}
	keygen := i.p.leaseBlake3Keyer()
	mhk := i.p.leaseKey()
	mhk.append(i.it.Key()...)
	mh, err := keygen.keyToMultihash(mhk)

	_ = mhk.Close()
	_ = keygen.Close()
	if err != nil {
		return nil, nil, err
	}

	// We don't need to copy the value since it is only used for fetching the
	// indexer.Values, and not returned to the caller.
	vks, err := i.vcodec.unmarshalValueKeys(i.it.Value())
	if err != nil {
		return nil, nil, err
	}
	defer vks.Close()
	result := make([][]byte, len(vks.keys))
	for j, vk := range vks.keys {
		vkCopy := make([]byte, len(vk.buf)-1)
		copy(vkCopy, vk.buf[1:])
		result[j] = vkCopy
	}
	i.it.Next()
	return mh, result, err
}

func (i *dhiterator) Close() error {
	_ = i.start.Close()
	_ = i.end.Close()
	// Check if closed already and do not re-call, since pebble
	// panics if snapshot is closed more than once.
	if i.closed {
		return nil
	}
	serr := i.snapshot.Close()
	ierr := i.it.Close()
	i.closed = true
	// Prioritise returning the iterator closure error. Because, that error
	// is more likely to be meaningful to the caller.
	if ierr != nil {
		return ierr
	}
	return serr
}

// valueKeyHash calculates a blake3 hash over the original value key to be stored in the index
func (s *dhstore) valueKeyHash(valKey []byte) ([]byte, error) {
	s.hasher.Reset()
	if _, err := s.hasher.Write(valKey); err != nil {
		return nil, err
	}
	return s.hasher.Sum(nil), nil
}
