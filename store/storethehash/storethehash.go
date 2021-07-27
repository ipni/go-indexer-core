package storethehash

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	cidprimary "github.com/ipld/go-storethehash/store/primary/cid"

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
	dir      string
	store    *sth.Store
	entStore *sth.Store
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

	// Initialize entry storage
	indexPath = filepath.Join(dir, "sthEntries.index")
	dataPath = filepath.Join(dir, "sthEntries.data")
	esPrimary, err := cidprimary.OpenCIDPrimary(dataPath)
	if err != nil {
		return nil, err
	}

	es, err := sth.OpenStore(indexPath, esPrimary, DefaultIndexSizeBits, DefaultSyncInterval, DefaultBurstRate)
	if err != nil {
		return nil, err
	}

	// Start both storages
	s.Start()
	es.Start()
	return &sthStorage{dir: dir, store: s, entStore: es}, nil
}

func (s *sthStorage) Get(c cid.Cid) ([]entry.Value, bool, error) {
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

func (s *sthStorage) get(k []byte) (store.CidEntry, bool, error) {
	value, found, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return [][]byte{}, false, nil
	}

	return store.SplitKs(value), true, nil
}

func (s *sthStorage) getEntry(k []byte) (*store.Entry, bool, error) {
	value, found, err := s.entStore.Get(k)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	out, err := store.Unmarshal(value)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (s *sthStorage) Put(c cid.Cid, entry entry.Value) (bool, error) {
	return s.put(c.Bytes(), entry)
}

func (s *sthStorage) put(k []byte, in entry.Value) (bool, error) {
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

func (s *sthStorage) putEntry(k []byte, in entry.Value) (bool, error) {
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
	// Flush entry store
	s.entStore.Flush()
	if err := s.entStore.Err(); err != nil {
		return err
	}
	// Flush store
	s.store.Flush()
	return s.store.Err()
}

func (s *sthStorage) Close() error {
	return s.store.Close()
}

func (s *sthStorage) Size() (int64, error) {
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

func (s *sthStorage) Remove(c cid.Cid, entry entry.Value) (bool, error) {
	return s.remove(c, entry)
}

func (s *sthStorage) remove(c cid.Cid, entry entry.Value) (bool, error) {
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
	// batch remove. We could use an offline process which
	// iterates through all keys removing/updating
	// the ones belonging to provider.
	// Deferring to the future
	panic("not implemented")
}

func (s *sthStorage) decreaseRefC(k []byte) (bool, error) {
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
		return s.entStore.Remove(k)
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

func (s *sthStorage) removeEntry(k []byte, value entry.Value, stored store.CidEntry) (bool, error) {
	entK, err := store.EntryKey(value)
	if err != nil {
		return false, err
	}
	for i := range stored {
		if bytes.Equal(entK, stored[i]) {
			// It is the only value, remove the value
			if len(stored) == 1 {
				_, err := s.store.Remove(k)
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
