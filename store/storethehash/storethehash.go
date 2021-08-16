package storethehash

import (
	"os"
	"path/filepath"
	"time"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	cidprimary "github.com/ipld/go-storethehash/store/primary/cid"

	"github.com/im7mortal/kmutex"
	"github.com/ipfs/go-cid"
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
}

func New(dir string) (*sthStorage, error) {
	// NOTE: Using a single file to store index and data.
	// This may change in the future, and we may choose to set
	// a max. size to files. Having several files for storage
	// increases complexity but mimizes the overhead of compaction
	// (once we have it)
	indexPath := filepath.Join(dir, "storethehash.index")
	dataPath := filepath.Join(dir, "storethehash.data")
	primary, err := cidprimary.OpenCIDPrimary(dataPath)
	if err != nil {
		return nil, err
	}

	s, err := sth.OpenStore(indexPath, primary, DefaultIndexSizeBits, DefaultSyncInterval, DefaultBurstRate)
	if err != nil {
		return nil, err
	}
	s.Start()
	return &sthStorage{
		dir:   dir,
		store: s,
		mlk:   kmutex.New(),
	}, nil
}

func (s *sthStorage) Get(c cid.Cid) ([]entry.Value, bool, error) {
	return s.get(c.Bytes())
}

func (s *sthStorage) get(k []byte) ([]entry.Value, bool, error) {
	value, found, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	out, err := entry.Unmarshal(value)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil

}

func (s *sthStorage) Put(c cid.Cid, entry entry.Value) (bool, error) {
	return s.put(c.Bytes(), entry)
}

func (s *sthStorage) put(k []byte, in entry.Value) (bool, error) {
	// Acquire lock
	s.lock(k)
	defer s.unlock(k)
	// NOTE: The implementation of Put in storethehash already
	// performs a first lookup to check the type of update that
	// needs to be done over the key. We can probably save this
	// additional get access by implementing the duplicateEntry comparison
	// low-level
	old, found, err := s.get(k)
	if err != nil {
		return false, err
	}
	// If found it means there is already a value there.
	// Check if we are trying to put a duplicate entry
	if found && duplicateEntry(in, old) {
		return false, nil
	}

	li := append(old, in)
	b, err := entry.Marshal(li)
	if err != nil {
		return false, err
	}

	err = s.store.Put(k, b)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *sthStorage) PutMany(cs []cid.Cid, entry entry.Value) error {
	for _, c := range cs {
		_, err := s.put(c.Bytes(), entry)
		if err != nil {
			// TODO: Log error but don't return. Errors for a single
			// CID shouldn't stop from putting the rest.
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
func (s *sthStorage) Remove(c cid.Cid, entry entry.Value) (bool, error) {
	return s.remove(c, entry)
}

func (s *sthStorage) remove(c cid.Cid, entry entry.Value) (bool, error) {
	k := c.Bytes()
	// Acquire lock
	s.lock(k)
	defer s.unlock(k)

	old, found, err := s.get(k)
	if err != nil {
		return false, err
	}
	// If found it means there is a value for the cid
	// check if there is something to remove.
	if found {
		return s.removeEntry(k, entry, old)
	}
	return false, nil
}

func (s *sthStorage) RemoveMany(cids []cid.Cid, entry entry.Value) error {
	for i := range cids {
		_, err := s.remove(cids[i], entry)
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
	// inspecting all entries for the provider in cids.
	// Deferring to the future
	panic("not implemented")
}

// DuplicateEntry checks if the entry already exists in the index. An entry
// for the same provider but different metadata is not considered
// a duplicate entry.
func duplicateEntry(in entry.Value, old []entry.Value) bool {
	for i := range old {
		if in.Equal(old[i]) {
			return true
		}
	}
	return false
}

func (s *sthStorage) removeEntry(k []byte, value entry.Value, stored []entry.Value) (bool, error) {
	for i := range stored {
		if value.Equal(stored[i]) {
			// It is the only value, remove the value
			if len(stored) == 1 {
				return s.store.Remove(k)
			}

			// else remove from value and put updated structure
			stored[i] = stored[len(stored)-1]
			stored[len(stored)-1] = entry.Value{}
			b, err := entry.Marshal(stored[:len(stored)-1])
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