// NOTE: Due to how pogreb is implemented, it is only capable of storing up to
// 4 billion records max (https://github.com/akrylysov/pogreb/issues/38).
// With our current scale I don't expect us to reach this limit, but
// noting it here just in case it becomes an issue in the future.
// Interesting link with alternatives: https://github.com/akrylysov/pogreb/issues/38#issuecomment-850852472

package pogreb

import (
	"bytes"
	"errors"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/akrylysov/pogreb"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Interface = &pStorage{}

const DefaultSyncInterval = time.Second

type pStorage struct {
	dir      string
	store    *pogreb.DB
	entStore *pogreb.DB
}

func New(dir string) (*pStorage, error) {
	opts := pogreb.Options{BackgroundSyncInterval: DefaultSyncInterval}

	s, err := pogreb.Open(dir, &opts)
	if err != nil {
		return nil, err
	}
	// Adding a store exclusively for entries.
	entryDir := path.Join(dir, "entries")
	es, err := pogreb.Open(entryDir, &opts)
	if err != nil {
		return nil, err
	}
	return &pStorage{dir: dir, store: s, entStore: es}, nil
}

func (s *pStorage) Get(c cid.Cid) ([]entry.Value, bool, error) {
	out := []entry.Value{}
	ks, found, err := s.get(c.Bytes())
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, found, nil
	}
	// If keys found, get entries from entryStore
	for i := range ks {
		e, found, err := s.getEntry(ks[i])
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, errors.New("one of the entries couldn't be found in entryStore")
		}
		out = append(out, e.Value)
	}

	return out, true, nil
}

func (s *pStorage) get(k []byte) (store.CidEntry, bool, error) {
	value, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return [][]byte{}, false, nil
	}

	return store.SplitKs(value), true, nil
}

func (s *pStorage) getEntry(k []byte) (*store.Entry, bool, error) {
	value, err := s.entStore.Get(k)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}

	out, err := store.Unmarshal(value)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (s *pStorage) Put(c cid.Cid, entry entry.Value) (bool, error) {
	return s.put(c.Bytes(), entry)
}

func (s *pStorage) put(k []byte, in entry.Value) (bool, error) {
	entK, err := store.EntryKey(in)
	if err != nil {
		return false, err
	}
	old, found, err := s.get(k)
	if err != nil {
		return false, err
	}
	// If found it means there is already a value there.
	// Check if we are trying to put a duplicate entry
	if found && store.DuplicateEntry(entK, old) {
		return false, nil
	}

	// If no duplicate put the entry.
	ok, err := s.putEntry(entK, in)
	if err != nil {
		return false, err
	}
	// Ok needs to be true, RefC needs to be increased successfully
	if !ok {
		return false, errors.New("failed to put entry in entryStore")
	}
	li := append(old, entK)

	err = s.store.Put(k, store.JoinKs(li))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *pStorage) putEntry(k []byte, in entry.Value) (bool, error) {
	old, found, err := s.getEntry(k)
	if err != nil {
		return false, err
	}
	// Found in store. Increase RefC and update in store
	if found {
		old.RefC++
		b, err := store.Marshal(old)
		if err != nil {
			return false, err
		}
		s.entStore.Put(k, b)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// If not found the entry is new and needs to be fully put.
	e := &store.Entry{Value: in, RefC: 1}
	b, err := store.Marshal(e)
	if err != nil {
		return false, err
	}
	err = s.entStore.Put(k, b)
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
	// Flush entryStore
	if err := s.entStore.Sync(); err != nil {
		return err
	}
	// Then flush the main store
	return s.store.Sync()
}

func (s *pStorage) Close() error {
	return s.store.Close()
}

func (s *pStorage) Size() (int64, error) {
	var size int64
	// NOTE: Consider using WalkDir for efficiency?
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

func (s *pStorage) decreaseRefC(k []byte) (bool, error) {
	old, found, err := s.getEntry(k)
	if err != nil {
		return false, err
	}
	if !found {
		return false, errors.New("cannot decrease RefC from non-existing entry")
	}

	// Decrease RefC
	old.RefC--
	// If RefC == 0 remove the full entry
	if old.RefC == 0 {
		return true, s.entStore.Delete(k)
	}
	// If not we put entry with the updated refCount
	b, err := store.Marshal(old)
	if err != nil {
		return false, err
	}
	s.entStore.Put(k, b)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *pStorage) removeEntry(k []byte, value entry.Value, stored store.CidEntry) (bool, error) {
	entK, err := store.EntryKey(value)
	if err != nil {
		return false, err
	}
	for i := range stored {
		if bytes.Equal(entK, stored[i]) {
			// It is the only value, remove the value
			if len(stored) == 1 {
				err := s.store.Delete(k)
				if err != nil {
					return false, err
				}
			} else {
				// else remove the key and put updated structure
				stored[i] = stored[len(stored)-1]
				stored[len(stored)-1] = []byte{}
				b := store.JoinKs(stored[:len(stored)-1])
				if err := s.store.Put(k, b); err != nil {
					return false, err
				}
			}

			// With the key removed, decrease RefCount from entry
			return s.decreaseRefC(entK)
		}
	}
	return false, nil
}
