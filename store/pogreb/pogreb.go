// NOTE: Due to how pogreb is implemented, it is only capable of storing up to
// 4 billion records max (https://github.com/akrylysov/pogreb/issues/38).
// With our current scale I don't expect us to reach this limit, but
// noting it here just in case it becomes an issue in the future.
// Interesting link with alternatives: https://github.com/akrylysov/pogreb/issues/38#issuecomment-850852472

package pogreb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	"golang.org/x/crypto/blake2b"
)

const DefaultSyncInterval = time.Second

// valueKeySize is the number of bytes of hash(providerID + contextID) used as
// key to lookup values.
const valueKeySize = 20

var (
	indexKeyPrefix = []byte("idx")
	valueKeyPrefix = []byte("md")
)

type pStorage struct {
	dir     string
	store   *pogreb.DB
	mlk     *keymutex.KeyMutex
	valLock sync.RWMutex
	vserde  indexer.ValueSerde
}

type pogrebIter struct {
	iter *pogreb.ItemIterator
	s    *pStorage
}

// New creates a new indexer.Interface implemented by a pogreb-based value
// store.
//
// The given indexer.ValueSerde is used to serialize and deserialize values.
// If it is set to nil, indexer.JsonValueSerde is used.
func New(dir string, vserde indexer.ValueSerde) (indexer.Interface, error) {
	opts := pogreb.Options{BackgroundSyncInterval: DefaultSyncInterval}
	if vserde == nil {
		vserde = indexer.JsonValueSerde{}
	}
	s, err := pogreb.Open(dir, &opts)
	if err != nil {
		return nil, err
	}
	return &pStorage{
		dir:    dir,
		store:  s,
		mlk:    keymutex.New(0),
		vserde: vserde,
	}, nil
}

func (s *pStorage) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	return s.get(makeIndexKey(m))
}

func (s *pStorage) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	valKey, err := s.updateValue(value, len(mhs) != 0)
	if err != nil {
		return fmt.Errorf("cannot store value: %w", err)
	}

	for i := range mhs {
		err = s.putIndex(mhs[i], valKey)
		if err != nil {
			return fmt.Errorf("cannot store index: %w", err)
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

func (s *pStorage) RemoveProvider(ctx context.Context, providerID peer.ID) error {
	err := s.store.Sync()
	if err != nil {
		return err
	}
	iter := s.store.Items()

	s.valLock.Lock()
	defer s.valLock.Unlock()

	var count int
	for {
		if count%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		count++

		// Iterate through all stored items, examining values and skipping
		// multihashes.
		key, valueData, err := iter.Next()
		if err != nil {
			if err == pogreb.ErrIterationDone {
				break
			}
			return err
		}
		if !bytes.HasPrefix(key, valueKeyPrefix) {
			// Key does not have value prefix, so is not an value key.
			continue
		}

		// If a value was found, skip it if the provider is different than the
		// one being removed.
		if valueData != nil {
			value, err := s.vserde.UnmarshalValue(valueData)
			if err != nil {
				return err
			}

			if value.ProviderID != providerID {
				continue
			}
		}

		// Delete the value of the provider being removed.
		if err = s.store.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

func (s *pStorage) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	valKey := makeValueKey(indexer.Value{
		ProviderID: providerID,
		ContextID:  contextID,
	})

	s.valLock.Lock()
	defer s.valLock.Unlock()

	// Remove any previous value.
	return s.store.Delete(valKey)
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
	s.valLock.Lock()
	defer s.valLock.Unlock()
	if s.store == nil {
		// Already closed
		return nil
	}
	err := s.store.Close()
	s.store = nil
	return err
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
		key, valKeysData, err := it.iter.Next()
		if err != nil {
			if err == pogreb.ErrIterationDone {
				err = io.EOF
			}
			return nil, nil, err
		}

		if !bytes.HasPrefix(key, indexKeyPrefix) {
			continue
		}

		valueKeys, err := it.s.vserde.UnmarshalValueKeys(valKeysData)
		if err != nil {
			return nil, nil, err
		}

		// Get the value for each value key
		values, err := it.s.getValues(key, valueKeys)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot get values for multihash: %w", err)
		}
		if len(values) == 0 {
			continue
		}

		return multihash.Multihash(key[len(indexKeyPrefix):]), values, nil
	}
}

func (s *pStorage) getValueKeys(k []byte) ([][]byte, error) {
	valueKeysData, err := s.store.Get(k)
	if err != nil {
		return nil, fmt.Errorf("cannot get multihash from store: %w", err)
	}
	if valueKeysData == nil {
		return nil, nil
	}

	return s.vserde.UnmarshalValueKeys(valueKeysData)
}

func (s *pStorage) get(k []byte) ([]indexer.Value, bool, error) {
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
		return nil, false, fmt.Errorf("cannot get values for multihash: %w", err)
	}

	if len(values) == 0 {
		return nil, false, nil
	}

	return values, true, nil
}

func (s *pStorage) putIndex(m multihash.Multihash, valKey []byte) error {
	k := makeIndexKey(m)

	s.lock(k)
	defer s.unlock(k)

	existingValKeys, err := s.getValueKeys(k)
	if err != nil {
		return fmt.Errorf("cannot get value keys for multihash: %w", err)
	}
	// If found it means there is already a value there.  Check if we are
	// trying to put a duplicate value.
	for _, existing := range existingValKeys {
		if bytes.Equal(valKey, existing) {
			return nil
		}
	}

	// Store the new list of value keys for the multihash.
	b, err := s.vserde.MarshalValueKeys(append(existingValKeys, valKey))
	if err != nil {
		return err
	}

	err = s.store.Put(k, b)
	if err != nil {
		return fmt.Errorf("cannot put multihash: %w", err)
	}

	return nil
}

func (s *pStorage) removeIndex(m multihash.Multihash, value indexer.Value) error {
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
				return s.store.Delete(k)
			}
			// Remove the value-key from the list of value-keys.
			valueKeys[i] = valueKeys[len(valueKeys)-1]
			valueKeys[len(valueKeys)-1] = nil
			valueKeys = valueKeys[:len(valueKeys)-1]
			// Update the list of value-keys that the multihash maps to.
			b, err := s.vserde.MarshalValueKeys(valueKeys)
			if err != nil {
				return err
			}
			return s.store.Put(k, b)
		}
	}
	return nil
}

func (s *pStorage) updateValue(value indexer.Value, saveNew bool) ([]byte, error) {
	// All values must have metadata, even if this only consists of the
	// protocol ID.
	if len(value.MetadataBytes) == 0 {
		return nil, errors.New("value missing metadata")
	}

	valKey := makeValueKey(value)

	s.valLock.Lock()
	defer s.valLock.Unlock()

	// See if there is a previous value.
	valData, err := s.store.Get(valKey)
	if err != nil {
		return nil, err
	}
	if valData == nil {
		if saveNew {
			// Store the new value.
			valData, err := s.vserde.MarshalValue(value)
			if err != nil {
				return nil, err
			}
			err = s.store.Put(valKey, valData)
			if err != nil {
				return nil, fmt.Errorf("cannot save new value: %w", err)
			}
		}
		return valKey, nil
	}

	// Found previous value.  If it is different, then update it.
	newValData, err := s.vserde.MarshalValue(value)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(newValData, valData) {
		err = s.store.Put(valKey, newValData)
		if err != nil {
			return nil, fmt.Errorf("cannot update existing value: %w", err)
		}
	}

	return valKey, nil
}

func (s *pStorage) lock(k []byte) {
	s.mlk.LockBytes(k)
}

func (s *pStorage) unlock(k []byte) {
	s.mlk.UnlockBytes(k)
}

func (s *pStorage) getValues(key []byte, valueKeys [][]byte) ([]indexer.Value, error) {
	values := make([]indexer.Value, 0, len(valueKeys))

	s.valLock.RLock()
	for i := 0; i < len(valueKeys); {
		// Fetch value from datastore
		valData, err := s.store.Get(valueKeys[i])
		if err != nil {
			s.valLock.RUnlock()
			return nil, fmt.Errorf("cannot get value: %w", err)
		}
		if valData == nil {
			// If value not in datastore, this means it has been
			// deleted, and the mapping from the multihash to that value
			// should also be removed.
			valueKeys[i] = valueKeys[len(valueKeys)-1]
			valueKeys[len(valueKeys)-1] = nil
			valueKeys = valueKeys[:len(valueKeys)-1]
			continue
		}
		val, err := s.vserde.UnmarshalValue(valData)
		if err != nil {
			s.valLock.RUnlock()
			return nil, err
		}
		values = append(values, val)
		i++
	}
	s.valLock.RUnlock()

	// If some of the values were removed, then update the value-key list for
	// the multihash.
	if len(valueKeys) < cap(values) {
		s.lock(key)
		defer s.unlock(key)

		if len(valueKeys) == 0 {
			err := s.store.Delete(key)
			if err != nil {
				return nil, fmt.Errorf("cannot delete multihash: %w", err)
			}
			return nil, nil
		}

		// Update the values this multihash maps to.
		b, err := s.vserde.MarshalValueKeys(valueKeys)
		if err != nil {
			return nil, err
		}
		if err = s.store.Put(key, b); err != nil {
			return nil, fmt.Errorf("cannot update value keys for multihash: %w", err)
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

func makeValueKey(value indexer.Value) []byte {
	// Create a hash of the ProviderID and ContextID so that the key length is
	// fixed.  This hash is used to look up the Value, which contains
	// ProviderID, ContextID, and Metadata.
	h, err := blake2b.New(valueKeySize, nil)
	if err != nil {
		panic(err)
	}
	_, _ = io.WriteString(h, string(value.ProviderID))
	h.Write(value.ContextID)

	var b bytes.Buffer
	b.Grow(len(valueKeyPrefix) + h.Size())
	b.Write(valueKeyPrefix)
	b.Write(h.Sum(nil))
	return b.Bytes()
}
