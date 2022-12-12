package dhash

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	keyLen   = 32
	nonceLen = 12
)

var (
	log = logging.Logger("store/dhash")
)

type DHash struct {
	doubleHashing bool
	ds            indexer.Datastore
	valueKeyer    *indexer.ValueKeyer
}

func New(ds indexer.Datastore, doubleHashing bool) *DHash {
	return &DHash{
		ds:            ds,
		doubleHashing: doubleHashing,
		valueKeyer:    indexer.NewKeyer(),
	}
}

func (d *DHash) Get(mh multihash.Multihash) ([]indexer.Value, bool, error) {
	mhb := []byte(mh)
	if d.doubleHashing {
		mhb = SecondSHA(mhb, nil)
	}
	vkPayloads, found, err := d.ds.GetValueKeys(mhb)
	if err != nil || !found {
		return nil, false, err
	}

	// Optimistically set the capacity of values slice to the number of value-keys
	// in order to reduce the append footprint in the loop below.
	values := make([]indexer.Value, 0, len(vkPayloads))

	for _, vkPayload := range vkPayloads {
		if d.doubleHashing {
			decrypted, err := decryptAES(vkPayload[:nonceLen], vkPayload[nonceLen:], []byte(mh))
			if err != nil {
				log.Errorw("can't decrypt value key", "err", err)
				return nil, false, err
			}
			vkPayload = decrypted
		}

		val, err := d.ds.GetValue(vkPayload)
		if err != nil {
			return nil, false, err
		}

		if val == nil {
			continue
		}

		values = append(values, *val)
	}

	return values, len(values) > 0, nil
}

// func (s *DHash) valueKey(v *indexer.Value) ([]byte, error) {
// 	keygen := s.p.leaseBlake3Keyer()
// 	defer keygen.Close()
// 	ph, ch, err := keygen.valueKeyPayload(v)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return append(ph, ch...), nil
// }

// // valueKey returns the key by which an indexer.Value is identified
// func (b *blake3Keyer) valueKey(v *indexer.Value, md bool) (*key, error) {
// 	ph, ch, err := b.valueKeyPayload(v)
// 	if err != nil {
// 		return nil, err
// 	}
// 	vk := b.leaseValueKeyWithPrefix(len(ph)+len(ch), md)
// 	vk.append(ph...)
// 	vk.append(ch...)
// 	return vk, nil
// }

// valueKeyPayload returns hashed pieces of the value key payload
// func (b *blake3Keyer) valueKeyPayload(v *indexer.Value) ([]byte, []byte, error) {
// 	b.hasher.Reset()
// 	if _, err := b.hasher.Write([]byte(v.ProviderID)); err != nil {
// 		return nil, nil, err
// 	}
// 	ph := b.hasher.Sum(nil)

// 	b.hasher.Reset()
// 	if _, err := b.hasher.Write(v.ContextID); err != nil {
// 		return nil, nil, err
// 	}
// 	ch := b.hasher.Sum(nil)
// 	return ph, ch, nil
// }

func (d *DHash) Put(v indexer.Value, mhs ...multihash.Multihash) error {
	if len(v.MetadataBytes) == 0 {
		return errors.New("value missing metadata")
	}
	b := d.ds.NewBatch()
	defer d.ds.CloseBatch(b)

	vkPayload, err := d.valueKeyer.Key(&v)
	if err != nil {
		return err
	}

	err = d.ds.PutValue(vkPayload, v, b)
	if err != nil {
		return err
	}

	keys := make([][]byte, len(mhs))
	for i := range mhs {
		if !d.doubleHashing {
			keys[i] = mhs[i]
			continue
		}
		// In the double hashing mode calculate second hash over the original multihash.
		// Append the original index to the resulting value in order to be able to get back to the multihash for encryption after sorting.
		// 32 bytes for SHA256 and 4 bytes for index
		key := make([]byte, 0, 36)
		key = SecondSHA(mhs[i], key)
		// TODO: Is it the best way to change len?
		key = append(key, 0xb, 0xb, 0xb, 0xb)
		binary.LittleEndian.PutUint32(key[32:], uint32(i))
		keys[i] = key
	}

	// Sort keys before insertion to reduce cursor churn.
	// Note that since multihash key is essentially a prefix plus the multihash itself,
	// sorting the multihashes means their resulting key will also be sorted.
	// The ordered key insertion is the main thing we care about here.
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) == -1
	})

	for _, key := range keys {
		if d.doubleHashing {
			// Read multihash index
			mhi := binary.LittleEndian.Uint32(key[32:])
			// Encrypt value key with the original multihash
			nonce, encrypted, err := encryptAES(vkPayload, mhs[mhi])
			if err != nil {
				return err
			}
			mhk := key[:32]
			err = d.ds.PutValueKey(mhk, append(nonce, encrypted...), b)
			if err != nil {
				return err
			}
		} else {
			err = d.ds.PutValueKey(key, vkPayload, b)
			if err != nil {
				return err
			}
		}
	}
	return d.ds.CommitBatch(b)
}

func (d *DHash) Remove(v indexer.Value, mhs ...multihash.Multihash) error {
	if d.doubleHashing {
		return d.removeDoubleHashing(v, mhs...)
	} else {
		return d.removeRegular(v, mhs...)
	}
}

func (d *DHash) removeRegular(v indexer.Value, mhs ...multihash.Multihash) error {
	vk, err := d.valueKeyer.Key(&v)
	if err != nil {
		return err
	}
	b := d.ds.NewBatch()
	defer d.ds.CloseBatch(b)
	for _, mh := range mhs {
		err := d.ds.RemoveValueKey(mh, vk, b)
		if err != nil {
			return err
		}
	}
	return d.ds.CommitBatch(b)
}

func (d *DHash) removeDoubleHashing(v indexer.Value, mhs ...multihash.Multihash) error {
	vkPayload, err := d.valueKeyer.Key(&v)
	if err != nil {
		return err
	}
	b := d.ds.NewBatch()
	defer d.ds.CloseBatch(b)
	for _, mh := range mhs {
		nonce, encrypted, err := encryptAES(vkPayload, mh)
		if err != nil {
			return err
		}
		d.ds.RemoveValueKey(SecondSHA(mh, nil), append(nonce, encrypted...), b)
	}

	// TODO: opportunistically delete garbage key value keys by checking a list
	//       of removed providers during merge.
	return d.ds.CommitBatch(b)
}

func (d *DHash) RemoveProvider(ctx context.Context, p peer.ID) error {
	return errors.New("delete provider not supported")
}

func (d *DHash) RemoveProviderContext(pid peer.ID, ctxID []byte) error {
	valKey, err := d.valueKeyer.Key(&indexer.Value{
		ProviderID: pid,
		ContextID:  ctxID,
	})
	if err != nil {
		return err
	}
	return d.ds.RemoveValue(valKey)
}

func (d *DHash) Size() (int64, error) {
	return d.ds.Size()
}

func (d *DHash) Flush() error {
	return d.ds.Flush()
}

func (d *DHash) Close() error {
	return d.ds.Close()
}

func (d *DHash) Iter() (indexer.Iterator, error) {
	return d.ds.Iter()
}

func (d *DHash) Stats() (*indexer.Stats, error) {
	return d.ds.Stats()
}

// encryptAES uses AESGCM to encrypt the payload with the passphrase and a nonce derived from it.
// returns nonce and encrypted bytes.
func encryptAES(payload, passphrase []byte) ([]byte, []byte, error) {
	// Derive the encryption key
	derivedKey := deriveKey([]byte(passphrase))

	// Create initialization vector (nonse) to be used during encryption
	// Nonce is derived from the mulithash (passpharase) so that encrypted payloads
	// for the same multihash can be compared to each other without having to decrypt
	nonce := SecondSHA(passphrase, nil)[:nonceLen]

	// Create cypher and seal the data
	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}

	encrypted := aesgcm.Seal(nil, nonce, []byte(payload), nil)

	return nonce, encrypted, nil
}

// decryptAES decrypts AES payload using the nonce and the passphrase
func decryptAES(nonce, payload, passphrase []byte) ([]byte, error) {
	key := deriveKey(passphrase)
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}

	return aesgcm.Open(nil, nonce, payload, nil)
}

// SecondSHA returns SHA256 over the payload
func SecondSHA(payload, dest []byte) []byte {
	h := sha256.New()
	h.Write(payload)
	return h.Sum(dest)
}

// deriveKey derives encryptioin key from passphrase using SHA256
func deriveKey(passphrase []byte) []byte {
	return SecondSHA(append([]byte("AESGCM"), passphrase...), nil)
}
