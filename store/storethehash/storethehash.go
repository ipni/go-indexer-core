package storethehash

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/multiformats/go-multihash"

	"github.com/im7mortal/kmutex"
	sth "github.com/ipld/go-storethehash/store"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Interface = &sthStorage{}

// TODO: Benchmark and fine-tune for better performance.
const DefaultIndexSizeBits = uint8(24)
const DefaultBurstRate = 4 * 1024 * 1024
const DefaultSyncInterval = time.Second

type sthStorage struct {
	dir   string
	store *sth.Store
	mlk   *kmutex.Kmutex

	primary *mhprimary.MultihashPrimary
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
		mlk:     kmutex.New(),
		primary: primary,
	}, nil
}

func (s *sthStorage) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	return s.get(m)
}

func (s *sthStorage) get(k []byte) ([]indexer.Value, bool, error) {
	value, found, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	out, err := indexer.UnmarshalValues(value)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil

}

func (s *sthStorage) ForEach(iterFunc indexer.IterFunc) error {
	s.Flush()
	iter, err := s.primary.Iter()
	if err != nil {
		return err
	}

	// Create a list of unique keys, and then call get for each unique key.
	//
	// This is necessary because the primary iterator returns all versions of
	// each index.  If a key has been updated with new values, the iterator
	// returns the key with the original values, then again with the values for
	// the next update, and so on for as many updates a there were for a key.
	uniqKeys := map[string]struct{}{}
	for {
		key, _, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		k := string(key)
		if _, found := uniqKeys[k]; found {
			continue
		}
		uniqKeys[k] = struct{}{}

		values, ok, err := s.get(key)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if iterFunc(multihash.Multihash(key), values) {
			break
		}
	}

	return nil
}

func (s *sthStorage) Put(m multihash.Multihash, value indexer.Value) (bool, error) {
	return s.put(m, value)
}

func (s *sthStorage) put(k []byte, in indexer.Value) (bool, error) {
	// Acquire lock
	s.lock(k)
	defer s.unlock(k)
	// NOTE: The implementation of Put in storethehash already
	// performs a first lookup to check the type of update that
	// needs to be done over the key. We can probably save this
	// additional get access by implementing the duplicateValue comparison
	// low-level
	old, found, err := s.get(k)
	if err != nil {
		return false, err
	}
	// If found it means there is already a value there.
	// Check if we are trying to put a duplicate value
	if found && duplicateValue(in, old) {
		return false, nil
	}

	li := append(old, in)
	b, err := indexer.MarshalValues(li)
	if err != nil {
		return false, err
	}

	err = s.store.Put(k, b)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *sthStorage) PutMany(mhs []multihash.Multihash, value indexer.Value) error {
	for i := range mhs {
		_, err := s.put(mhs[i], value)
		if err != nil {
			// TODO: Log error but don't return. Errors for a single
			// multihash shouldn't stop from putting the rest.
			continue
		}
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
func (s *sthStorage) Remove(m multihash.Multihash, value indexer.Value) (bool, error) {
	return s.remove(m, value)
}

func (s *sthStorage) remove(m multihash.Multihash, value indexer.Value) (bool, error) {
	// Acquire lock
	s.lock(m)
	defer s.unlock(m)

	old, found, err := s.get(m)
	if err != nil {
		return false, err
	}
	// If found it means there is a value for the multihash
	// check if there is something to remove.
	if found {
		return s.removeValue(m, value, old)
	}
	return false, nil
}

func (s *sthStorage) RemoveMany(mhs []multihash.Multihash, value indexer.Value) error {
	for i := range mhs {
		_, err := s.remove(mhs[i], value)
		if err != nil {
			return err
		}
	}
	return nil
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

// duplicateValue checks if the value already exists in the index entry. An
// value for the same provider but different metadata is not considered a
// duplicate value.
func duplicateValue(in indexer.Value, entry []indexer.Value) bool {
	// Iterate values in the index entry to look for a duplicate
	for i := range entry {
		if in.Equal(entry[i]) {
			return true
		}
	}
	return false
}

func (s *sthStorage) removeValue(k []byte, value indexer.Value, stored []indexer.Value) (bool, error) {
	for i := range stored {
		if value.Equal(stored[i]) {
			// It is the only value, remove the value
			if len(stored) == 1 {
				return s.store.Remove(k)
			}

			// else remove from value and put updated structure
			stored[i] = stored[len(stored)-1]
			stored[len(stored)-1] = indexer.Value{}
			b, err := indexer.MarshalValues(stored[:len(stored)-1])
			if err != nil {
				return false, err
			}
			if err := s.store.Put(k, b); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

// Close stops all storage-related routines, and flushes
// pending data
func (s *sthStorage) Close() error {
	return s.store.Close()
}

func (s *sthStorage) lock(k []byte) {
	s.mlk.Lock(string(k))
}

func (s *sthStorage) unlock(k []byte) {
	s.mlk.Unlock(string(k))
}
