// NOTE: Due to how pogreb is implemented, it is only capable of storing up to
// 4 billion records max (https://github.com/akrylysov/pogreb/issues/38).
// With our current scale I don't expect us to reach this limit, but
// noting it here just in case it becomes an issue in the future.
// Interesting link with alternatives: https://github.com/akrylysov/pogreb/issues/38#issuecomment-850852472

package pogreb

import (
	"os"
	"path/filepath"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/im7mortal/kmutex"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

var _ store.Interface = &pStorage{}

const DefaultSyncInterval = time.Second

type pStorage struct {
	dir   string
	store *pogreb.DB
	mlk   *kmutex.Kmutex
}

func New(dir string) (*pStorage, error) {
	opts := pogreb.Options{BackgroundSyncInterval: DefaultSyncInterval}

	s, err := pogreb.Open(dir, &opts)
	if err != nil {
		return nil, err
	}
	return &pStorage{
		dir:   dir,
		store: s,
		mlk:   kmutex.New(),
	}, nil
}

func (s *pStorage) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	return s.get([]byte(m))
}

func (s *pStorage) get(k []byte) ([]indexer.Value, bool, error) {
	value, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}

	out, err := indexer.UnmarshalValues(value)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil

}

func (s *pStorage) ForEach(iterFunc indexer.IterFunc) error {
	err := s.store.Sync()
	if err != nil {
		return err
	}
	it := s.store.Items()
	for {
		key, val, err := it.Next()
		if err != nil {
			if err == pogreb.ErrIterationDone {
				break
			}
			return err
		}

		values, err := indexer.UnmarshalValues(val)
		if err != nil {
			return err
		}

		if iterFunc(multihash.Multihash(key), values) {
			break
		}
	}

	return nil
}

func (s *pStorage) Put(m multihash.Multihash, value indexer.Value) (bool, error) {
	return s.put([]byte(m), value)
}

func (s *pStorage) put(k []byte, in indexer.Value) (bool, error) {
	// Acquire lock
	s.lock(k)
	defer s.unlock(k)
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

func (s *pStorage) PutMany(mhs []multihash.Multihash, value indexer.Value) error {
	for i := range mhs {
		_, err := s.put([]byte(mhs[i]), value)
		if err != nil {
			// TODO: Log error but don't return. Errors for a single
			// multihash shouldn't stop from putting the rest.
			continue
		}
	}
	return nil
}

func (s *pStorage) Flush() error {
	return s.store.Sync()
}

func (s *pStorage) Close() error {
	return s.store.Close()
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

func (s *pStorage) Remove(m multihash.Multihash, value indexer.Value) (bool, error) {
	return s.remove(m, value)
}

func (s *pStorage) remove(m multihash.Multihash, value indexer.Value) (bool, error) {
	k := []byte(m)
	// Acquire lock
	s.lock(k)
	defer s.unlock(k)
	old, found, err := s.get(k)
	if err != nil {
		return false, err
	}
	// If found it means there is a value for the multihash
	// check if there is something to remove.
	if found {
		return s.removeValue(k, value, old)
	}
	return false, nil
}

func (s *pStorage) RemoveMany(mhs []multihash.Multihash, value indexer.Value) error {
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
func (s *pStorage) RemoveProvider(providerID peer.ID) error {
	// NOTE: There is no straightforward way of implementing this
	// batch remove. We could use an offline process which
	// iterates through all keys removing/updating
	// the ones belonging to provider.
	// Deferring to the future
	panic("not implemented")
}

// DuplicateValue checks if the value already exists in the index entry. A
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

func (s *pStorage) removeValue(k []byte, value indexer.Value, stored []indexer.Value) (bool, error) {
	for i := range stored {
		if value.Equal(stored[i]) {
			// It is the only value, remove the value
			if len(stored) == 1 {
				return true, s.store.Delete(k)
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

func (s *pStorage) lock(k []byte) {
	s.mlk.Lock(string(k))
}

func (s *pStorage) unlock(k []byte) {
	s.mlk.Unlock(string(k))
}
