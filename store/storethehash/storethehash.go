package storethehash

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/ipld/go-storethehash/store/primary"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/multiformats/go-multihash"

	"github.com/gammazero/keymutex"
	sth "github.com/ipld/go-storethehash/store"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ indexer.Interface = &sthStorage{}

// TODO: Benchmark and fine-tune for better performance.
const DefaultIndexSizeBits = uint8(24)
const DefaultBurstRate = 4 * 1024 * 1024
const DefaultSyncInterval = time.Second

var (
	indexKeyPrefix = []byte("I")
	mdKeyPrefix    = []byte("M")
)

type sthStorage struct {
	dir    string
	store  *sth.Store
	mlk    *keymutex.KeyMutex
	mdLock sync.RWMutex

	primary *mhprimary.MultihashPrimary
}

type sthIterator struct {
	iter     primary.PrimaryStorageIter
	storage  *sthStorage
	uniqKeys map[string]struct{}
}

func New(dir string) (*sthStorage, error) {
	// NOTE: Using a single file to store index and data.
	// This may change in the future, and we may choose to set
	// a max. size to files. Having several files for storage
	// increases complexity but mimizes the overhead of compaction
	// (once we have it)
	indexPath := filepath.Join(dir, "storethehash.index")
	dataPath := filepath.Join(dir, "storethehash.data")
	primary, err := mhprimary.OpenMultihashPrimary(dataPath)
	if err != nil {
		return nil, err
	}

	s, err := sth.OpenStore(indexPath, primary, DefaultIndexSizeBits, DefaultSyncInterval, DefaultBurstRate)
	if err != nil {
		return nil, err
	}
	s.Start()
	return &sthStorage{
		dir:     dir,
		store:   s,
		mlk:     keymutex.New(0),
		primary: primary,
	}, nil
}

func (s *sthStorage) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	return s.get(makeIndexKey(m))
}

func (s *sthStorage) get(k []byte) ([]indexer.Value, bool, error) {
	value, found, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	values, err := indexer.UnmarshalValues(value)
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

func (s *sthStorage) Iter() (indexer.Iterator, error) {
	s.Flush()
	iter, err := s.primary.Iter()
	if err != nil {
		return nil, err
	}
	return &sthIterator{
		iter:     iter,
		storage:  s,
		uniqKeys: map[string]struct{}{},
	}, nil
}

func (it *sthIterator) Next() (multihash.Multihash, []indexer.Value, error) {
	for {
		key, _, err := it.iter.Next()
		if err != nil {
			if err == io.EOF {
				it.uniqKeys = nil
			}
			return nil, nil, err
		}

		// Decode the key and see if it is an index key.
		dm, err := multihash.Decode(key)
		if err != nil {
			return nil, nil, err
		}
		if !bytes.HasPrefix(dm.Digest, indexKeyPrefix) {
			// Key does not have index prefix, so is not an index key.
			continue
		}

		origMultihash := multihash.Multihash(dm.Digest[len(indexKeyPrefix):])
		k := string(origMultihash)
		_, found := it.uniqKeys[k]
		if found {
			continue
		}
		it.uniqKeys[k] = struct{}{}

		valueData, found, err := it.storage.store.Get(multihash.Multihash(key))
		if err != nil {
			return nil, nil, err
		}
		if !found {
			continue
		}
		values, err := indexer.UnmarshalValues(valueData)
		if err != nil {
			return nil, nil, err
		}

		// Get the metadata for each value
		values, err = it.storage.populateMetadata(key, values)
		if err != nil {
			return nil, nil, err
		}

		if len(values) == 0 {
			continue
		}

		return origMultihash, values, nil
	}
}

func (s *sthStorage) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	err := s.updateMetadata(value, len(mhs) != 0)
	if err != nil {
		return fmt.Errorf("cannot update metadada: %w", err)
	}

	for i := range mhs {
		err = s.putIndex(mhs[i], value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *sthStorage) putIndex(m multihash.Multihash, value indexer.Value) error {
	k := makeIndexKey(m)

	s.lock(k)
	defer s.unlock(k)

	// NOTE: The implementation of Put in storethehash already
	// performs a first lookup to check the type of update that
	// needs to be done over the key. We can probably save this
	// additional get access by implementing the duplicateValue comparison
	// low-level
	existing, found, err := s.get(k)
	if err != nil {
		return err
	}
	if found {
		// If found it means there is already a value there.
		// Check if we are trying to put a duplicate value
		for j := range existing {
			if value.Match(existing[j]) {
				return nil
			}
		}
	}

	// Values are stored without metadata, and are used as a key to lookup
	// the metadata.
	value.Metadata = nil
	vals := append(existing, value)

	// store the list of value keys for the multihash
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

func (s *sthStorage) Flush() error {
	s.store.Flush()
	return s.store.Err()
}

func (s *sthStorage) Size() (int64, error) {
	// NOTE: Should we flush to commit all changes before returning the
	// size?
	size := int64(0)
	fi, err := os.Stat(filepath.Join(s.dir, "storethehash.data"))
	if err != nil {
		return size, err
	}
	size += fi.Size()
	fi, err = os.Stat(filepath.Join(s.dir, "storethehash.index"))
	if err != nil {
		return size, err
	}
	size += fi.Size()
	fi, err = os.Stat(filepath.Join(s.dir, "storethehash.index.free"))
	if err != nil {
		return size, err
	}
	size += fi.Size()
	return size, nil

}

func (s *sthStorage) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	for i := range mhs {
		k := makeIndexKey(mhs[i])
		err := s.removeIndex(k, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *sthStorage) removeIndex(k []byte, value indexer.Value) error {
	s.lock(k)
	defer s.unlock(k)

	old, found, err := s.get(k)
	if err != nil {
		return err
	}
	// If found it means there is a value for the multihash
	// check if there is something to remove.
	if !found {
		return nil
	}

	return s.removeValue(k, value, old)
}

// RemoveProvider removes all enrties for specified provider.  This is used
// when a provider is no longer indexed by the indexer.
func (s *sthStorage) RemoveProvider(providerID peer.ID) error {
	// NOTE: There is no straightforward way of implementing this
	// batch remove. We can either regenerate the index from
	// the original data, or iterate through the whole the whole primary storage
	// inspecting all values for the provider in multihashes.
	// Deferring to the future
	panic("not implemented")
}

// RemoveProviderContext removes all values for specified providerID that have
// the specified contextID.  The mappings of multihashes to these values are
// removed when they are retrieved, by detecting that the metadata of these
// values no longer exists.
func (s *sthStorage) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	mdKey := makeMetadataKey(indexer.Value{
		ProviderID: providerID,
		ContextID:  contextID,
	})

	s.mdLock.Lock()
	defer s.mdLock.Unlock()

	// Remove any previous value.
	_, err := s.store.Remove(mdKey)
	return err
}

func (s *sthStorage) updateMetadata(value indexer.Value, saveNew bool) error {
	// All values must have metadata, even if this only consists of the
	// protocol ID.  When retrieving values, those that have nil metadata are
	// ones that have been deleted, and this is used to remove remaining
	// mappings from a multihash to the value.
	if len(value.Metadata) == 0 {
		return errors.New("value missing metadata")
	}

	mdKey := makeMetadataKey(value)

	s.mdLock.Lock()
	defer s.mdLock.Unlock()

	// See if there is a previous value.
	metadata, found, err := s.store.Get(mdKey)
	if err != nil {
		return err
	}
	if !found {
		if saveNew {
			// Store the new metadata
			return s.store.Put(mdKey, value.Metadata)
		}
		return nil
	}

	// Found previous metadata.  If it is different, then update it.
	if !bytes.Equal(value.Metadata, metadata) {
		return s.store.Put(mdKey, value.Metadata)
	}

	return nil
}

func (s *sthStorage) removeValue(k []byte, value indexer.Value, stored []indexer.Value) error {
	for i := range stored {
		if value.Match(stored[i]) {
			// It is the only value, remove the value
			if len(stored) == 1 {
				_, err := s.store.Remove(k)
				return err
			}

			// else remove from value and put updated structure
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

// Close stops all storage-related routines, and flushes
// pending data
func (s *sthStorage) Close() error {
	return s.store.Close()
}

func (s *sthStorage) lock(k []byte) {
	s.mlk.LockBytes(k)
}

func (s *sthStorage) unlock(k []byte) {
	s.mlk.UnlockBytes(k)
}

func (s *sthStorage) populateMetadata(key []byte, values []indexer.Value) ([]indexer.Value, error) {
	s.mdLock.RLock()
	defer s.mdLock.RUnlock()

	startLen := len(values)
	for i := 0; i < len(values); {
		// Try to get metadata from previous matching value
		var prev int
		for prev = i - 1; prev >= 0; prev-- {
			prevVal := values[prev]
			if values[i].Match(prevVal) {
				values[i].Metadata = prevVal.Metadata
				break
			}
		}
		// If metadata not in previous value, fetch from datastore.
		if prev < 0 {
			md, found, err := s.store.Get(makeMetadataKey(values[i]))
			if err != nil {
				return nil, err
			}
			if !found {
				// If metadata not in datastore, this means it has been
				// deleted, and the mapping from the multihash to that value
				// should also be removed.
				values[i] = values[len(values)-1]
				values[len(values)-1] = indexer.Value{}
				values = values[:len(values)-1]
				continue
			}
			values[i].Metadata = md
		}
		i++
	}
	if len(values) < startLen {
		s.lock(key)
		defer s.unlock(key)

		if len(values) == 0 {
			_, err := s.store.Remove(key)
			if err != nil {
				return nil, err
			}
			return nil, nil
		}

		// Update the values this metadata maps to
		b, err := indexer.MarshalValues(values)
		if err != nil {
			return nil, err
		}
		if err = s.store.Put(key, b); err != nil {
			return nil, err
		}
	}

	return values, nil
}

func makeIndexKey(m multihash.Multihash) multihash.Multihash {
	mhb := []byte(m)
	var b bytes.Buffer
	b.Grow(len(indexKeyPrefix) + len(mhb))
	b.Write(indexKeyPrefix)
	b.Write(mhb)
	mh, _ := multihash.Encode(b.Bytes(), multihash.IDENTITY)
	return mh
}

func makeMetadataKey(value indexer.Value) multihash.Multihash {
	// Create a sha1 hash of the ProviderID and ContextID so that the key
	// lenght is fixed.  Note: a faster non-crypto hash could be used here.
	h := sha1.New()
	io.WriteString(h, string(value.ProviderID))
	h.Write(value.ContextID)

	var b bytes.Buffer
	b.Grow(len(mdKeyPrefix) + sha1.Size)
	b.Write(mdKeyPrefix)
	b.Write(h.Sum(nil))
	mh, _ := multihash.Encode(b.Bytes(), multihash.IDENTITY)
	return mh
}
