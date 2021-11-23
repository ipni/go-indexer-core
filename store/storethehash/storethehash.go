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

	"github.com/filecoin-project/go-indexer-core"
	"github.com/ipld/go-storethehash/store/primary"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	"github.com/multiformats/go-multihash"

	"github.com/gammazero/keymutex"
	sth "github.com/ipld/go-storethehash/store"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var (
	indexKeySuffix = []byte("I")
	valueKeySuffix = []byte("M")
)

type sthStorage struct {
	dir     string
	store   *sth.Store
	mlk     *keymutex.KeyMutex
	valLock sync.RWMutex

	primary *mhprimary.MultihashPrimary
}

type sthIterator struct {
	iter     primary.PrimaryStorageIter
	storage  *sthStorage
	uniqKeys map[string]struct{}
}

// New creates a new indexer.Interface implemented by a storethehash-based
// value store.
func New(dir string, options ...Option) (indexer.Interface, error) {
	// NOTE: Using a single file to store index and data.  This may change in
	// the future, and we may choose to set a max. size to files. Having
	// several files for storage increases complexity but minimizes the
	// overhead of compaction (once we have it)
	indexPath := filepath.Join(dir, "storethehash.index")
	dataPath := filepath.Join(dir, "storethehash.data")
	primary, err := mhprimary.OpenMultihashPrimary(dataPath)
	if err != nil {
		return nil, err
	}

	cfg := config{
		indexSizeBits: defaultIndexSizeBits,
		syncInterval:  defaultSyncInterval,
		burstRate:     defaultBurstRate,
	}
	cfg.apply(options)

	s, err := sth.OpenStore(indexPath, primary, cfg.indexSizeBits, cfg.syncInterval, cfg.burstRate)
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

func (s *sthStorage) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	valKey, err := s.updateValue(value, len(mhs) != 0)
	if err != nil {
		return fmt.Errorf("cannot update stored value: %w", err)
	}

	for i := range mhs {
		err = s.putIndex(mhs[i], valKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *sthStorage) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	for i := range mhs {
		err := s.removeIndex(mhs[i], value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *sthStorage) RemoveProvider(providerID peer.ID) error {
	s.Flush()
	iter, err := s.primary.Iter()
	if err != nil {
		return err
	}

	s.valLock.Lock()
	defer s.valLock.Unlock()

	for {
		key, _, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Decode the key and see if it is key.
		dm, err := multihash.Decode(key)
		if err != nil {
			return err
		}
		if !bytes.HasSuffix(dm.Digest, valueKeySuffix) {
			// Key does not have value suffix, so is not an value key.
			continue
		}

		valueData, found, err := s.store.Get(multihash.Multihash(key))
		if err != nil {
			return err
		}

		if found {
			value, err := indexer.UnmarshalValue(valueData)
			if err != nil {
				return err
			}

			if value.ProviderID != providerID {
				continue
			}
		}

		_, err = s.store.Remove(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *sthStorage) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	valKey := makeValueKey(indexer.Value{
		ProviderID: providerID,
		ContextID:  contextID,
	})

	s.valLock.Lock()
	defer s.valLock.Unlock()

	// Remove any previous value.
	_, err := s.store.Remove(valKey)
	return err
}

func (s *sthStorage) Size() (int64, error) {
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

func (s *sthStorage) Flush() error {
	s.store.Flush()
	return s.store.Err()
}

func (s *sthStorage) Close() error {
	return s.store.Close()
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
		if !bytes.HasSuffix(dm.Digest, indexKeySuffix) {
			// Key does not have index prefix, so is not an index key.
			continue
		}

		mhb := make([]byte, len(dm.Digest)-len(indexKeySuffix))
		copy(mhb, dm.Digest)
		reverseBytes(mhb)
		origMultihash := multihash.Multihash(mhb)
		k := string(origMultihash)
		_, found := it.uniqKeys[k]
		if found {
			continue
		}
		it.uniqKeys[k] = struct{}{}

		valueKeysData, found, err := it.storage.store.Get(multihash.Multihash(key))
		if err != nil {
			return nil, nil, err
		}
		if !found {
			continue
		}

		valueKeys, err := indexer.UnmarshalValueKeys(valueKeysData)
		if err != nil {
			return nil, nil, err
		}

		// Get the value for each value key
		values, err := it.storage.getValues(key, valueKeys)
		if err != nil {
			return nil, nil, err
		}

		if len(values) == 0 {
			continue
		}

		return origMultihash, values, nil
	}
}

func (s *sthStorage) getValueKeys(k []byte) ([][]byte, error) {
	valueKeysData, found, err := s.store.Get(k)
	if err != nil {
		return nil, fmt.Errorf("Cannot get multihash from store: %w", err)
	}
	if !found {
		return nil, nil
	}

	return indexer.UnmarshalValueKeys(valueKeysData)
}

func (s *sthStorage) get(k []byte) ([]indexer.Value, bool, error) {
	valueKeys, err := s.getValueKeys(k)
	if err != nil {
		return nil, false, err
	}
	if valueKeys == nil {
		return nil, false, nil
	}

	// Get the value for each value key.
	values, err := s.getValues(k, valueKeys)
	if err != nil {
		return nil, false, err
	}

	if len(values) == 0 {
		return nil, false, nil
	}

	return values, true, nil
}

func (s *sthStorage) putIndex(m multihash.Multihash, valKey []byte) error {
	k := makeIndexKey(m)

	s.lock(k)
	defer s.unlock(k)

	existingValKeys, err := s.getValueKeys(k)
	if err != nil {
		return err
	}
	// If found it means there is already a value there.  Check if we are
	// trying to put a duplicate value.
	for _, existing := range existingValKeys {
		if bytes.Equal(valKey, existing) {
			return nil
		}
	}

	valKeys := append(existingValKeys, valKey)

	// Store the new list of value keys for the multihash
	b, err := indexer.MarshalValueKeys(valKeys)
	if err != nil {
		return fmt.Errorf("cannot encode value keys for index: %w", err)
	}

	err = s.store.Put(k, b)
	if err != nil {
		return fmt.Errorf("cannot put multihash: %w", err)
	}

	return nil
}

func (s *sthStorage) updateValue(value indexer.Value, saveNew bool) ([]byte, error) {
	// All values must have metadata, even if this only consists of the
	// protocol ID.
	if len(value.MetadataBytes) == 0 {
		return nil, errors.New("value missing metadata")
	}

	valKey := makeValueKey(value)

	s.valLock.Lock()
	defer s.valLock.Unlock()

	// See if there is a previous value.
	valData, found, err := s.store.Get(valKey)
	if err != nil {
		return nil, err
	}
	if !found {
		if saveNew {
			// Store the new value.
			valData, err := indexer.MarshalValue(value)
			if err != nil {
				return nil, err
			}
			err = s.store.Put(valKey, valData)
			if err != nil {
				return nil, err
			}
		}
		return valKey, nil
	}

	// Found previous value.  If it is different, then update it.
	newValData, err := indexer.MarshalValue(value)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(newValData, valData) {
		if err = s.store.Put(valKey, newValData); err != nil {
			return nil, err
		}
	}

	return valKey, nil
}

func (s *sthStorage) removeIndex(m multihash.Multihash, value indexer.Value) error {
	k := makeIndexKey(m)

	s.lock(k)
	defer s.unlock(k)

	valueKeys, err := s.getValueKeys(k)
	if err != nil {
		return err
	}

	valKey := makeValueKey(value)

	for i := range valueKeys {
		if bytes.Equal(valKey, valueKeys[i]) {
			if len(valueKeys) == 1 {
				_, err = s.store.Remove(k)
				return err
			}
			valueKeys[i] = valueKeys[len(valueKeys)-1]
			valueKeys[len(valueKeys)-1] = nil
			valueKeys = valueKeys[:len(valueKeys)-1]
			b, err := indexer.MarshalValueKeys(valueKeys)
			if err != nil {
				return err
			}
			return s.store.Put(k, b)
		}
	}
	return nil
}

func (s *sthStorage) lock(k []byte) {
	s.mlk.LockBytes(k)
}

func (s *sthStorage) unlock(k []byte) {
	s.mlk.UnlockBytes(k)
}

func (s *sthStorage) getValues(key []byte, valueKeys [][]byte) ([]indexer.Value, error) {
	startLen := len(valueKeys)
	var values []indexer.Value

	s.valLock.RLock()
	for i := 0; i < len(valueKeys); {
		// Fetch value from datastore.
		valData, found, err := s.store.Get(valueKeys[i])
		if err != nil {
			s.valLock.RUnlock()
			return nil, err
		}
		if !found {
			// If value not in datastore, this means it has been
			// deleted, and the mapping from the multihash to that value
			// should also be removed.
			valueKeys[i] = valueKeys[len(valueKeys)-1]
			valueKeys[len(valueKeys)-1] = nil
			valueKeys = valueKeys[:len(valueKeys)-1]
			continue
		}
		val, err := indexer.UnmarshalValue(valData)
		if err != nil {
			s.valLock.RUnlock()
			return nil, err
		}
		values = append(values, val)
		i++
	}
	s.valLock.RUnlock()

	if len(valueKeys) < startLen {
		s.lock(key)
		defer s.unlock(key)

		if len(valueKeys) == 0 {
			_, err := s.store.Remove(key)
			return nil, err
		}

		// Update the values this mmultihash maps to.
		b, err := indexer.MarshalValueKeys(valueKeys)
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
	b.Grow(len(mhb) + len(indexKeySuffix))
	b.Write(mhb)
	b.Write(indexKeySuffix)
	data := b.Bytes()
	// Reverse the bytes in the identity-wrapped multihash so that the hash
	// portion of the data is first.
	reverseBytes(data[:len(data)-len(indexKeySuffix)])
	mh, _ := multihash.Encode(data, multihash.IDENTITY)
	return mh
}

func reverseBytes(b []byte) {
	i := 0
	j := len(b) - 1
	for i < j {
		b[i], b[j] = b[j], b[i]
		i++
		j--
	}
}

func makeValueKey(value indexer.Value) multihash.Multihash {
	// Create a sha1 hash of the ProviderID and ContextID so that the key
	// length is fixed.  Note: a faster non-crypto hash could be used here.
	h := sha1.New()
	_, _ = io.WriteString(h, string(value.ProviderID))
	h.Write(value.ContextID)

	var b bytes.Buffer
	b.Grow(sha1.Size + len(valueKeySuffix))
	b.Write(h.Sum(nil))
	b.Write(valueKeySuffix)
	mh, _ := multihash.Encode(b.Bytes(), multihash.IDENTITY)
	return mh
}
