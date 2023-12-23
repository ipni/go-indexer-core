package pebble

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	// defaultKeyerLength is the length of hashes generated by the default keyer, blake3Keyer.
	defaultKeyerLength = 20

	// metricsReportingInterval is the interval at which metrics are reported
	metricsReportingInterval = 30 * time.Second
)

var (
	log = logging.Logger("store/pebble")

	_ indexer.Interface = (*store)(nil)
	_ indexer.Iterator  = (*iterator)(nil)
)

type (
	store struct {
		db *pebble.DB
		// Only support binary format since in pebble we need the capability to merge keys and
		// there is little reason for store values in any format other than binary for performance
		// characteristics.
		// Note, pebble is using a zero-copy variation of marshaller to allow optimizations in
		// cases where the value need not to be copied. The root level binary codec copies on
		// unmarshal every time.
		vcodec        *codec
		p             *pool
		closed        bool
		comparer      *pebble.Comparer
		metricsCancel context.CancelFunc
	}
	iterator struct {
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
func New(path string, opts *pebble.Options) (indexer.Interface, error) {
	p := newPool()
	c := &codec{
		p: p,
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

	metricsContext, cancelFunc := context.WithCancel(context.Background())

	st := &store{
		db:            db,
		p:             p,
		vcodec:        c,
		comparer:      opts.Comparer,
		metricsCancel: cancelFunc,
	}

	go metrics.ObservePebbleMetrics(metricsContext, metricsReportingInterval, db)

	return st, nil
}

func (s *store) Get(mh multihash.Multihash) ([]indexer.Value, bool, error) {
	keygen := s.p.leaseBlake3Keyer()
	mhk, err := keygen.multihashKey(mh)
	_ = keygen.Close()
	if err != nil {
		return nil, false, err
	}
	vkb, vkbClose, err := s.db.Get(mhk.buf)
	_ = mhk.Close()
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		log.Errorw("can't find multihash", "err", err)
		return nil, false, err
	}

	vks, err := s.vcodec.unmarshalValueKeys(vkb)
	_ = vkbClose.Close()
	if err != nil {
		return nil, false, err
	}
	defer vks.Close()

	// Optimistically set the capacity of values slice to the number of value-keys
	// in order to reduce the append footprint in the loop below.
	values := make([]indexer.Value, 0, len(vks.keys))
	for _, vk := range vks.keys {
		vs, vCloser, err := s.db.Get(vk.buf)
		if err == pebble.ErrNotFound {
			// TODO find an efficient way to opportunistically clean up
			continue
		}
		if err != nil {
			log.Errorw("can't find value", "err", err)
			return nil, false, err
		}

		v, err := s.vcodec.unmarshalValue(vs)
		_ = vCloser.Close()
		if err != nil {
			return nil, false, err
		}
		values = append(values, *v)
	}
	return values, len(values) != 0, nil
}

func (s *store) Put(v indexer.Value, mhs ...multihash.Multihash) error {
	if len(v.MetadataBytes) == 0 {
		return errors.New("value missing metadata")
	}

	// Sort multihashes before insertion to reduce cursor churn. Since a
	// multihash key is a prefix plus the multihash itself, sorting the
	// multihashes means their resulting keys will also be sorted.
	sort.Slice(mhs, func(i, j int) bool {
		return bytes.Compare(mhs[i], mhs[j]) == -1
	})

	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()
	vk, err := keygen.valueKey(&v, false)
	if err != nil {
		return err
	}
	defer vk.Close()

	b := s.db.NewBatch()
	defer b.Close()

	for _, mh := range mhs {
		mhk, err := keygen.multihashKey(mh)
		if err != nil {
			return err
		}
		err = b.Merge(mhk.buf, vk.buf, pebble.NoSync)
		_ = mhk.Close()
		if err != nil {
			return err
		}
	}

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
	if err = b.Set(vk.buf, vs, pebble.NoSync); err != nil {
		return err
	}

	return b.Commit(pebble.NoSync)
}

func (s *store) Remove(v indexer.Value, mhs ...multihash.Multihash) error {
	// Sort multihashes before insertion to reduce cursor churn. Since a
	// multihash key is a prefix plus the multihash itself, sorting the
	// multihashes means their resulting keys will also be sorted.
	sort.Slice(mhs, func(i, j int) bool {
		return bytes.Compare(mhs[i], mhs[j]) == -1
	})

	keygen := s.p.leaseBlake3Keyer()
	defer keygen.Close()
	dvk, err := keygen.valueKey(&v, true)
	if err != nil {
		return err
	}
	defer dvk.Close()

	b := s.db.NewBatch()
	defer b.Close()

	for _, mh := range mhs {
		mhk, err := keygen.multihashKey(mh)
		if err != nil {
			return err
		}
		err = b.Merge(mhk.buf, dvk.buf, pebble.NoSync)
		_ = mhk.Close()
		if err != nil {
			return err
		}
	}

	// TODO: opportunistically delete garbage key value keys by checking a list
	//       of removed providers during merge.
	return b.Commit(pebble.NoSync)
}

func (s *store) RemoveProvider(_ context.Context, pid peer.ID) error {
	keygen := s.p.leaseBlake3Keyer()
	start, end, err := keygen.valuesByProviderKeyRange(pid)
	_ = keygen.Close()
	if err != nil {
		return err
	}
	defer func() {
		_ = start.Close()
		_ = end.Close()
	}()
	return s.db.DeleteRange(start.buf, end.buf, pebble.NoSync)
}

func (s *store) RemoveProviderContext(pid peer.ID, ctxID []byte) error {
	keygen := s.p.leaseBlake3Keyer()
	vk, err := keygen.valueKey(
		&indexer.Value{
			ProviderID: pid,
			ContextID:  ctxID,
		}, false)
	_ = keygen.Close()
	if err != nil {
		return err
	}
	defer vk.Close()
	return s.db.Delete(vk.buf, pebble.NoSync)
}

func (s *store) Size() (int64, error) {
	sizeEstimate, err := s.db.EstimateDiskUsage([]byte{0}, []byte{0xff})
	return int64(sizeEstimate), err
}

func (s *store) Flush() error {
	return s.db.Flush()
}

func (s *store) Close() error {
	if s.closed {
		return nil
	}
	ferr := s.db.Flush()
	cerr := s.db.Close()
	s.metricsCancel()
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
func (s *store) Stats() (*indexer.Stats, error) {
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

func (s *store) Iter() (indexer.Iterator, error) {
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

func (i *iterator) Next() (multihash.Multihash, []indexer.Value, error) {
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
	vs := make([]indexer.Value, len(vks.keys))
	for j, vk := range vks.keys {
		bv, c, err := i.snapshot.Get(vk.buf)
		if err != nil {
			return nil, nil, err
		}

		v, err := i.vcodec.unmarshalValue(bv)
		_ = c.Close()
		if err != nil {
			return nil, nil, err
		}
		vs[j] = *v
	}
	i.it.Next()
	return mh, vs, err
}

func (i *iterator) Close() error {
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
