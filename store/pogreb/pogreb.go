// NOTE: Due to how pogreb is implemented, it is only capable of storing up to
// 4 billion records max (https://github.com/akrylysov/pogreb/issues/38).
// With our current scale I don't expect us to reach this limit, but
// noting it here just in case it becomes an issue in the future.
// Interesting link with alternatives: https://github.com/akrylysov/pogreb/issues/38#issuecomment-850852472

package pogreb

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/filecoin-project/go-indexer-core"
	"github.com/gammazero/keymutex"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

const DefaultSyncInterval = time.Second

var (
	indexKeyPrefix = []byte("idx")
	mdKeyPrefix    = []byte("md")
)

type pStorage struct {
	dir    string
	store  *pogreb.DB
	mlk    *keymutex.KeyMutex
	mdLock sync.RWMutex
}

type pogrebIter struct {
	iter *pogreb.ItemIterator
	s    *pStorage
}

// New creates a new indexer.Interface implemented by a pogreb-based value
// store.
func New(dir string) (indexer.Interface, error) {
	opts := pogreb.Options{BackgroundSyncInterval: DefaultSyncInterval}

	s, err := pogreb.Open(dir, &opts)
	if err != nil {
		return nil, err
	}
	return &pStorage{
		dir:   dir,
		store: s,
		mlk:   keymutex.New(0),
	}, nil
}

func (s *pStorage) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	return s.get(makeIndexKey(m))
}

func (s *pStorage) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	err := s.updateMetadata(value, len(mhs) != 0)
	if err != nil {
		return err
	}

	for i := range mhs {
		err = s.putIndex(mhs[i], value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *pStorage) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	for i := range mhs {
		err := s.removeIndex(mhs[i], value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *pStorage) RemoveProvider(providerID peer.ID) error {
	// NOTE: There is no straightforward way of implementing this batch
	// remove. We could use an offline process which iterates through all keys
	// removing/updating the ones belonging to provider.  Deferring to the
	// future
	panic("not implemented")
}

func (s *pStorage) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	mdKey := makeMetadataKey(indexer.Value{
		ProviderID: providerID,
		ContextID:  contextID,
	})

	s.mdLock.Lock()
	defer s.mdLock.Unlock()

	// Remove any previous value.
	return s.store.Delete(mdKey)
}

func (s *pStorage) Size() (int64, error) {
	var size int64
	err := filepath.Walk(s.dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func (s *pStorage) Flush() error {
	return s.store.Sync()
}

func (s *pStorage) Close() error {
	return s.store.Close()
}

func (s *pStorage) Iter() (indexer.Iterator, error) {
	err := s.store.Sync()
	if err != nil {
		return nil, err
	}
	return &pogrebIter{
		iter: s.store.Items(),
		s:    s,
	}, nil
}

func (it *pogrebIter) Next() (multihash.Multihash, []indexer.Value, error) {
	for {
		key, val, err := it.iter.Next()
		if err != nil {
			if err == pogreb.ErrIterationDone {
				err = io.EOF
			}
			return nil, nil, err
		}

		if bytes.HasPrefix(key, indexKeyPrefix) {
			values, err := indexer.UnmarshalValues(val)
			if err != nil {
				return nil, nil, err
			}

			// Get the metadata for each value
			values, err = it.s.populateMetadata(key, values)
			if err != nil {
				return nil, nil, err
			}
			if len(values) == 0 {
				continue
			}

			return multihash.Multihash(key[len(indexKeyPrefix):]), values, nil
		}
	}
}

func (s *pStorage) get(k []byte) ([]indexer.Value, bool, error) {
	valueData, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if valueData == nil {
		return nil, false, nil
	}

	values, err := indexer.UnmarshalValues(valueData)
	if err != nil {
		return nil, false, err
	}

	// Get the metadata for each value
	values, err = s.populateMetadata(k, values)
	if err != nil {
		return nil, false, err
	}

	if len(values) == 0 {
		return nil, false, nil
	}

	return values, true, nil
}

func (s *pStorage) putIndex(m multihash.Multihash, value indexer.Value) error {
	k := makeIndexKey(m)

	s.lock(k)
	defer s.unlock(k)

	existing, found, err := s.get(k)
	if err != nil {
		return err
	}
	if found {
		// If found it means there is already a value there.  Check if we are
		// trying to put a duplicate value.
		for j := range existing {
			if value.Match(existing[j]) {
				return nil
			}
		}
	}

	// Values are stored without metadata, and are used as a key to lookup
	// the metadata.
	value.MetadataBytes = nil
	vals := append(existing, value)

	// Store the list of value keys for the multihash.
	b, err := indexer.MarshalValues(vals)
	if err != nil {
		return err
	}

	err = s.store.Put(k, b)
	if err != nil {
		return err
	}

	return nil
}

func (s *pStorage) removeIndex(m multihash.Multihash, value indexer.Value) error {
	k := makeIndexKey(m)

	s.lock(k)
	defer s.unlock(k)

	old, found, err := s.get(k)
	if err != nil {
		return err
	}
	// If found it means there is a value for the multihash.  Check if there is
	// something to remove.
	if !found {
		return nil
	}

	return s.removeValue(k, value, old)
}

func (s *pStorage) updateMetadata(value indexer.Value, saveNew bool) error {
	// All values must have metadata, even if this only consists of the
	// protocol ID.  When retrieving values, those that have nil metadata are
	// ones that have been deleted, and this is used to remove remaining
	// mappings from a multihash to the value.
	if len(value.MetadataBytes) == 0 {
		return errors.New("value missing metadata")
	}

	mdKey := makeMetadataKey(value)

	s.mdLock.Lock()
	defer s.mdLock.Unlock()

	// See if there is a previous value.
	metadata, err := s.store.Get(mdKey)
	if err != nil {
		return err
	}
	if metadata == nil {
		if saveNew {
			// Store the new metadata
			return s.store.Put(mdKey, value.MetadataBytes)
		}
		return nil
	}

	// Found previous metadata.  If it is different, then update it.
	if !bytes.Equal(value.MetadataBytes, metadata) {
		return s.store.Put(mdKey, value.MetadataBytes)
	}

	return nil
}

func (s *pStorage) removeValue(k []byte, value indexer.Value, stored []indexer.Value) error {
	for i := range stored {
		if value.Match(stored[i]) {
			// If it is the only value, remove the value.
			if len(stored) == 1 {
				return s.store.Delete(k)
			}

			// Remove from value and put updated structure.
			stored[i] = stored[len(stored)-1]
			stored[len(stored)-1] = indexer.Value{}
			b, err := indexer.MarshalValues(stored[:len(stored)-1])
			if err != nil {
				return err
			}
			if err := s.store.Put(k, b); err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

func (s *pStorage) lock(k []byte) {
	s.mlk.LockBytes(k)
}

func (s *pStorage) unlock(k []byte) {
	s.mlk.UnlockBytes(k)
}

func (s *pStorage) populateMetadata(key []byte, values []indexer.Value) ([]indexer.Value, error) {
	s.mdLock.RLock()
	defer s.mdLock.RUnlock()

	startLen := len(values)
	for i := 0; i < len(values); {
		// Try to get metadata from previous matching value.
		var prev int
		for prev = i - 1; prev >= 0; prev-- {
			if values[i].Match(values[prev]) {
				values[i].MetadataBytes = values[prev].MetadataBytes
				break
			}
		}
		// If metadata not in previous value, fetch from datastore.
		if prev < 0 {
			md, err := s.store.Get(makeMetadataKey(values[i]))
			if err != nil {
				return nil, err
			}
			if md == nil {
				// If metadata not in datastore, this means it has been
				// deleted, and the mapping from the multihash to that value
				// should also be removed.
				values[i] = values[len(values)-1]
				values[len(values)-1] = indexer.Value{}
				values = values[:len(values)-1]
				continue
			}
			values[i].MetadataBytes = md
		}
		i++
	}
	if len(values) < startLen {
		s.lock(key)
		defer s.unlock(key)

		if len(values) == 0 {
			err := s.store.Delete(key)
			return nil, err
		}

		// Update the values this metadata maps to.
		storeVals := make([]indexer.Value, len(values))
		for i := range values {
			storeVals[i] = values[i]
			storeVals[i].MetadataBytes = nil
		}
		b, err := indexer.MarshalValues(storeVals)
		if err != nil {
			return nil, err
		}
		if err = s.store.Put(key, b); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func makeIndexKey(m multihash.Multihash) []byte {
	mhb := []byte(m)
	var b bytes.Buffer
	b.Grow(len(indexKeyPrefix) + len(mhb))
	b.Write(indexKeyPrefix)
	b.Write(mhb)
	return b.Bytes()
}

func makeMetadataKey(value indexer.Value) []byte {
	// Create a sha1 hash of the ProviderID and ContextID so that the key
	// length is fixed.  Note: a faster non-crypto hash could be used here.
	h := sha1.New()
	_, _ = io.WriteString(h, string(value.ProviderID))
	h.Write(value.ContextID)

	var b bytes.Buffer
	b.Grow(len(mdKeyPrefix) + sha1.Size)
	b.Write(mdKeyPrefix)
	b.Write(h.Sum(nil))
	return b.Bytes()
}
