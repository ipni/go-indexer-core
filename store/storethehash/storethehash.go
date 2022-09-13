package storethehash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/gammazero/keymutex"
	"github.com/gammazero/workerpool"
	sth "github.com/ipld/go-storethehash/store"
	"github.com/ipld/go-storethehash/store/primary"
	mhprimary "github.com/ipld/go-storethehash/store/primary/multihash"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"golang.org/x/crypto/blake2b"
)

// valueKeySize is the number of bytes of hash(providerID + contextID) used as
// key to lookup values.
const valueKeySize = 20

var (
	indexKeySuffix = []byte("I")
	valueKeySuffix = []byte("M")
)

type SthStorage struct {
	dir   string
	store *sth.Store

	// mlk protects against concurrent changes to the array of value keys that
	// a multihash maps to.
	mlk *keymutex.KeyMutex
	// vlk protects against concurrent changes to the same provider value.
	//
	// TODO: Determine the necessity of this. Fixes to underlying valuestore
	// may make this unnecessary.
	vlk *keymutex.KeyRWMutex

	primary *mhprimary.MultihashPrimary
	vcodec  indexer.ValueCodec

	wp *workerpool.WorkerPool
	// wpMutex protects replacing wp
	wpMutex sync.Mutex
}

type sthIterator struct {
	iter     primary.PrimaryStorageIter
	storage  *SthStorage
	uniqKeys map[string]struct{}
}

// New creates a new indexer.Interface implemented by a storethehash-based
// value store.
//
// The given indexer.ValueCodec is used to serialize and deserialize values.
// If it is set to nil, indexer.BinaryWithJsonFallbackCodec is used which
// // will gracefully migrate the codec from JSON to Binary format.
func New(ctx context.Context, dir string, vcodec indexer.ValueCodec, putConcurrency int, options ...sth.Option) (*SthStorage, error) {
	// Using a single file to store index and data. This may change in the
	// future, and we may choose to set a max. size to files. Having several
	// files for storage increases complexity but minimizes the overhead of
	// compaction (once we have it)
	indexPath := filepath.Join(dir, "storethehash.index")
	dataPath := filepath.Join(dir, "storethehash.data")
	primary, err := mhprimary.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("error opening storethehash primary: %w", err)
	}

	s, err := sth.OpenStore(ctx, indexPath, primary, false, options...)
	if err != nil {
		return nil, fmt.Errorf("error opening storethehash index: %w", err)
	}
	if vcodec == nil {
		vcodec = indexer.BinaryWithJsonFallbackCodec{}
	}
	s.Start()
	var wp *workerpool.WorkerPool
	if putConcurrency > 1 {
		wp = workerpool.New(putConcurrency)
	}

	return &SthStorage{
		dir:     dir,
		store:   s,
		mlk:     keymutex.New(putConcurrency),
		vlk:     keymutex.NewRW(0),
		primary: primary,
		vcodec:  vcodec,
		wp:      wp,
	}, nil
}

func (s *SthStorage) Get(m multihash.Multihash) ([]indexer.Value, bool, error) {
	return s.get(makeIndexKey(m))
}

func (s *SthStorage) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	valKey, err := s.updateValue(value, len(mhs) != 0)
	if err != nil {
		return fmt.Errorf("cannot store value: %w", err)
	}

	s.wpMutex.Lock()
	wp := s.wp
	s.wpMutex.Unlock()

	if wp == nil || len(mhs) < 2 {
		for i := range mhs {
			if err = s.putIndex(mhs[i], valKey); err != nil {
				return fmt.Errorf("cannot store index: %w", err)
			}
		}
		return nil
	}

	errCh := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(mhs))
	for i := range mhs {
		m := mhs[i]
		wp.Submit(func() {
			if err := s.putIndex(m, valKey); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			wg.Done()
		})
	}
	wg.Wait()
	close(errCh)
	err = <-errCh
	if err != nil {
		return fmt.Errorf("cannot store index: %w", err)
	}
	return nil
}

func (s *SthStorage) Remove(value indexer.Value, mhs ...multihash.Multihash) error {
	s.wpMutex.Lock()
	wp := s.wp
	s.wpMutex.Unlock()

	if wp == nil || len(mhs) < 2 {
		for i := range mhs {
			if err := s.removeIndex(mhs[i], value); err != nil {
				return fmt.Errorf("cannot remove index: %w", err)
			}
		}
		return nil
	}

	errCh := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(mhs))

	for i := range mhs {
		m := mhs[i]
		wp.Submit(func() {
			if err := s.removeIndex(m, value); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			wg.Done()
		})
	}

	wg.Wait()
	close(errCh)
	err := <-errCh
	if err != nil {
		return fmt.Errorf("cannot remove index: %w", err)
	}
	return nil
}

func (s *SthStorage) RemoveProvider(ctx context.Context, providerID peer.ID) error {
	s.Flush()
	iter, err := s.primary.Iter()
	if err != nil {
		return err
	}

	var count int
	for {
		if count%1024 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		count++

		// Iterate through all stored items, examining values and skipping
		// multihashes.
		key, _, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Decode the key and see if it is a value key.
		dm, err := multihash.Decode(key)
		if err != nil {
			return err
		}
		if !bytes.HasSuffix(dm.Digest, valueKeySuffix) {
			// Key does not have value suffix, so not a value key.
			continue
		}

		s.vlk.RLockBytes(key)
		valueData, found, err := s.store.Get(multihash.Multihash(key))
		s.vlk.RUnlockBytes(key)
		if err != nil {
			return err
		}

		// If a value was found, skip it if the provider is different than the
		// one being removed.
		if found {
			value, err := s.vcodec.UnmarshalValue(valueData)
			if err != nil {
				return err
			}

			if value.ProviderID != providerID {
				continue
			}
		}

		s.vlk.LockBytes(key)
		defer s.vlk.UnlockBytes(key)

		// Delete the value of the provider being removed.
		if _, err = s.store.Remove(key); err != nil {
			return err
		}
	}

	return nil
}

func (s *SthStorage) RemoveProviderContext(providerID peer.ID, contextID []byte) error {
	valKey := makeValueKey(indexer.Value{
		ProviderID: providerID,
		ContextID:  contextID,
	})

	s.vlk.LockBytes(valKey)
	defer s.vlk.UnlockBytes(valKey)

	// Remove any previous value.
	_, err := s.store.Remove(valKey)
	return err
}

func (s *SthStorage) Size() (int64, error) {
	return s.store.StorageSize()
}

func (s *SthStorage) Flush() error {
	s.store.Flush()
	return s.store.Err()
}

func (s *SthStorage) Close() error {
	s.wpMutex.Lock()
	if s.wp != nil {
		s.wp.Stop()
		s.wp = nil
	}
	s.wpMutex.Unlock()
	return s.store.Close()
}

func (s *SthStorage) SetFileCacheSize(size int) {
	s.store.SetFileCacheSize(size)
}

func (s *SthStorage) SetPutConcurrency(n int) {
	s.wpMutex.Lock()
	oldWP := s.wp
	if n > 1 {
		// If there is already a pool of the requested size, do nothing.
		if s.wp != nil && s.wp.Size() == n {
			s.wpMutex.Unlock()
			return
		}
		s.wp = workerpool.New(n)
	} else {
		s.wp = nil
	}
	s.wpMutex.Unlock()

	if oldWP != nil {
		oldWP.StopWait()
	}
}

func (s *SthStorage) Iter() (indexer.Iterator, error) {
	err := s.Flush()
	if err != nil {
		return nil, err
	}
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

		valueKeys, err := it.storage.vcodec.UnmarshalValueKeys(valueKeysData)
		if err != nil {
			return nil, nil, err
		}

		// Get the value for each value key
		values, err := it.storage.getValues(key, valueKeys)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot get values for multihash: %w", err)
		}

		if len(values) == 0 {
			continue
		}

		return origMultihash, values, nil
	}
}

func (it *sthIterator) Close() error { return nil }

func (s *SthStorage) getValueKeys(k []byte) ([][]byte, error) {
	valueKeysData, found, err := s.store.Get(k)
	if err != nil {
		return nil, fmt.Errorf("cannot get multihash from store: %w", err)
	}
	if !found {
		return nil, nil
	}

	return s.vcodec.UnmarshalValueKeys(valueKeysData)
}

func (s *SthStorage) get(k []byte) ([]indexer.Value, bool, error) {
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

func (s *SthStorage) putIndex(m multihash.Multihash, valKey []byte) error {
	k := makeIndexKey(m)

	s.mlk.LockBytes(k)
	defer s.mlk.UnlockBytes(k)

	existingValKeys, err := s.getValueKeys(k)
	if err != nil {
		return fmt.Errorf("cannot get value keys for multihash: %w", err)
	}
	// If found it means there is already a value there. Check if we are trying
	// to put a duplicate value.
	for _, existing := range existingValKeys {
		if bytes.Equal(valKey, existing) {
			return nil
		}
	}

	// Store the new list of value keys for the multihash.
	b, err := s.vcodec.MarshalValueKeys(append(existingValKeys, valKey))
	if err != nil {
		return err
	}

	err = s.store.Put(k, b)
	if err != nil {
		return fmt.Errorf("cannot put multihash: %w", err)
	}

	return nil
}

func (s *SthStorage) updateValue(value indexer.Value, saveNew bool) ([]byte, error) {
	// All values must have metadata, even if this only consists of the
	// protocol ID.
	if len(value.MetadataBytes) == 0 {
		return nil, errors.New("value missing metadata")
	}

	valKey := makeValueKey(value)

	s.vlk.LockBytes(valKey)
	defer s.vlk.UnlockBytes(valKey)

	// See if there is a previous value.
	valData, found, err := s.store.Get(valKey)
	if err != nil {
		return nil, err
	}
	if !found {
		if saveNew {
			// Store the new value.
			valData, err := s.vcodec.MarshalValue(value)
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
	newValData, err := s.vcodec.MarshalValue(value)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(newValData, valData) {
		if err = s.store.Put(valKey, newValData); err != nil {
			return nil, fmt.Errorf("cannot update existing value: %w", err)
		}
	}

	return valKey, nil
}

func (s *SthStorage) removeIndex(m multihash.Multihash, value indexer.Value) error {
	k := makeIndexKey(m)

	s.mlk.LockBytes(k)
	defer s.mlk.UnlockBytes(k)

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
			// Remove the value-key from the list of value-keys.
			valueKeys[i] = valueKeys[len(valueKeys)-1]
			valueKeys[len(valueKeys)-1] = nil
			valueKeys = valueKeys[:len(valueKeys)-1]
			// Update the list of value-keys that the multihash maps to.
			b, err := s.vcodec.MarshalValueKeys(valueKeys)
			if err != nil {
				return err
			}
			return s.store.Put(k, b)
		}
	}
	return nil
}

func (s *SthStorage) getValues(key []byte, valueKeys [][]byte) ([]indexer.Value, error) {
	values := make([]indexer.Value, 0, len(valueKeys))

	for i := 0; i < len(valueKeys); {
		s.vlk.RLockBytes(valueKeys[i])
		// Fetch value from datastore.
		valData, found, err := s.store.Get(valueKeys[i])
		s.vlk.RUnlockBytes(valueKeys[i])
		if err != nil {
			return nil, fmt.Errorf("cannot get value: %w", err)
		}
		if !found {
			// If value not in datastore, this means it has been deleted, and
			// the mapping from the multihash to that value should also be
			// removed.
			valueKeys[i] = valueKeys[len(valueKeys)-1]
			valueKeys[len(valueKeys)-1] = nil
			valueKeys = valueKeys[:len(valueKeys)-1]
			continue
		}
		val, err := s.vcodec.UnmarshalValue(valData)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		i++
	}

	// If some of the values were removed, then update the value-key list for
	// the multihash.
	if len(valueKeys) < cap(values) {
		s.mlk.LockBytes(key)
		defer s.mlk.UnlockBytes(key)

		if len(valueKeys) == 0 {
			_, err := s.store.Remove(key)
			if err != nil {
				return nil, fmt.Errorf("cannot delete multihash: %w", err)
			}
			return nil, nil
		}

		// Update the values this multihash maps to.
		b, err := s.vcodec.MarshalValueKeys(valueKeys)
		if err != nil {
			return nil, err
		}
		if err = s.store.Put(key, b); err != nil {
			return nil, fmt.Errorf("cannot update value keys for multihash: %w", err)
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
	// Create a hash of the ProviderID and ContextID so that the key length is
	// fixed. This hash is used to look up the Value, which contains
	// ProviderID, ContextID, and Metadata.
	h, err := blake2b.New(valueKeySize, nil)
	if err != nil {
		panic(err)
	}
	_, _ = io.WriteString(h, string(value.ProviderID))
	h.Write(value.ContextID)

	var b bytes.Buffer
	b.Grow(h.Size() + len(valueKeySuffix))
	b.Write(h.Sum(nil))
	b.Write(valueKeySuffix)
	mh, _ := multihash.Encode(b.Bytes(), multihash.IDENTITY)
	return mh
}
