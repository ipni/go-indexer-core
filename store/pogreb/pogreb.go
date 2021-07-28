// NOTE: Due to how pogreb is implemented, it is only capable of storing up to
// 4 billion records max (https://github.com/akrylysov/pogreb/issues/38).
// With our current scale I don't expect us to reach this limit, but
// noting it here just in case it becomes an issue in the future.
// Interesting link with alternatives: https://github.com/akrylysov/pogreb/issues/38#issuecomment-850852472

package pogreb

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/adlrocha/mapmutex"
	"github.com/akrylysov/pogreb"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Interface = &pStorage{}

const DefaultSyncInterval = time.Second

type pStorage struct {
	dir   string
	store *pogreb.DB
	mlk   *mapmutex.Mutex
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
		mlk:   mapmutex.NewMapMutex(),
	}, nil
}

func (s *pStorage) Get(c cid.Cid) ([]entry.Value, bool, error) {
	return s.get(c.Bytes())
}

func (s *pStorage) get(k []byte) ([]entry.Value, bool, error) {
	value, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}

	out, err := entry.Unmarshal(value)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil

}

func (s *pStorage) Put(c cid.Cid, entry entry.Value) (bool, error) {
	return s.put(c.Bytes(), entry)
}

func (s *pStorage) put(k []byte, in entry.Value) (bool, error) {
	// Acquire lock
	err := s.lock(k)
	if err != nil {
		return false, err
	}
	defer s.unlock(k)
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

func (s *pStorage) PutMany(cs []cid.Cid, entry entry.Value) error {
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

func (s *pStorage) Remove(c cid.Cid, entry entry.Value) (bool, error) {
	return s.remove(c, entry)
}

func (s *pStorage) remove(c cid.Cid, entry entry.Value) (bool, error) {
	k := c.Bytes()
	// Acquire lock
	err := s.lock(k)
	if err != nil {
		return false, err
	}
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

func (s *pStorage) RemoveMany(cids []cid.Cid, entry entry.Value) error {
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
func (s *pStorage) RemoveProvider(providerID peer.ID) error {
	// NOTE: There is no straightforward way of implementing this
	// batch remove. We could use an offline process which
	// iterates through all keys removing/updating
	// the ones belonging to provider.
	// Deferring to the future
	panic("not implemented")
}

// DuplicateEntry checks if the entry already exists in the index. An entry
// for the same provider but different metadata is not considered
// a duplicate entry,
func duplicateEntry(in entry.Value, old []entry.Value) bool {
	for i := range old {
		if in.Equal(old[i]) {
			return true
		}
	}
	return false
}

func (s *pStorage) removeEntry(k []byte, value entry.Value, stored []entry.Value) (bool, error) {
	for i := range stored {
		if value.Equal(stored[i]) {
			// It is the only value, remove the value
			if len(stored) == 1 {
				return true, s.store.Delete(k)
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

func (s *pStorage) lock(k []byte) error {
	if !s.mlk.TryLock(string(k)) {
		return errors.New("couldn't get the lock after maxRetries")
	}
	return nil
}

func (s *pStorage) unlock(k []byte) {
	s.mlk.Unlock(string(k))
}
