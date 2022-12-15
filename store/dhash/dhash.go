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
	"github.com/ipni/go-indexer-core/store"
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

// DHash is an implementation of the indexer interface that introduces Reader Privacy mechanics around the underlying Datastore.
type DHash struct {
	ds         store.Interface
	valueKeyer *indexer.ValueKeyer
}

func New(ds store.Interface) *DHash {
	return &DHash{
		ds:         ds,
		valueKeyer: indexer.NewKeyer(),
	}
}

func (d *DHash) Get(mh multihash.Multihash) ([]indexer.Value, bool, error) {
	mhb := SecondSHA(mh, nil)
	vkPayloads, found, err := d.ds.GetValueKeys(mhb)
	if err != nil || !found {
		return nil, false, err
	}

	// Optimistically set the capacity of values slice to the number of value-keys
	// in order to reduce the append footprint in the loop below.
	values := make([]indexer.Value, 0, len(vkPayloads))

	for _, vkPayload := range vkPayloads {
		// Decrypt each value key
		decrypted, err := decryptAES(vkPayload[:nonceLen], vkPayload[nonceLen:], []byte(mh))
		if err != nil {
			log.Errorw("can't decrypt value key", "err", err)
			return nil, false, err
		}
		vkPayload = decrypted

		// Look up value with the decrypted payload
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

func (d *DHash) Put(v indexer.Value, mhs ...multihash.Multihash) error {
	if len(v.MetadataBytes) == 0 {
		return errors.New("value missing metadata")
	}
	b := d.ds.NewBatch()
	defer d.ds.CloseBatch(b)

	vk, err := d.valueKeyer.Key(&v)
	if err != nil {
		return err
	}

	err = d.ds.PutValue(vk, v, b)
	if err != nil {
		return err
	}

	keys := make([][]byte, len(mhs))
	for i := range mhs {
		// Calculate second hash over the original multihash.
		// Append the index from the multihash array to the resulting value in order to be able to get back to the multihash for encryption after sorting.
		// Keep in mind - double hashed array will have different order than the original one after sorting.
		// 32 bytes for SHA256 and 4 bytes for index.
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
		// Read multihash index
		mhi := binary.LittleEndian.Uint32(key[32:])
		// Encrypt value key with the original multihash
		nonce, encrypted, err := encryptAES(vk, mhs[mhi])
		if err != nil {
			return err
		}
		mhk := key[:32]
		err = d.ds.PutValueKey(mhk, encValueKey(nonce, encrypted), b)
		if err != nil {
			return err
		}
	}
	return d.ds.CommitBatch(b)
}

func (d *DHash) Remove(v indexer.Value, mhs ...multihash.Multihash) error {
	// TODO: opportunistically delete garbage key value keys by checking a list
	// of removed providers during merge.
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
		d.ds.RemoveValueKey(SecondSHA(mh, nil), encValueKey(nonce, encrypted), b)
	}

	return d.ds.CommitBatch(b)
}

func (d *DHash) RemoveProvider(ctx context.Context, p peer.ID) error {
	// Delete provider is not supported because range queries by peerID aren't possibe over the encrypted data
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

func (d *DHash) GetValueKeys(secondHash multihash.Multihash) ([][]byte, bool, error) {
	return d.ds.GetValueKeys(secondHash)
}

func (d *DHash) GetValue(valKey []byte) (*indexer.Value, error) {
	return d.ds.GetValue(valKey)
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

// encValueKey builds an encrypted value key from nonce and encrypted payload
func encValueKey(nonce, encrypted []byte) []byte {
	return append(nonce, encrypted...)
}
