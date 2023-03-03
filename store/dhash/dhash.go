package dhash

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	// nonceLen defines length of the nonce to use for AESGCM encryption
	nonceLen = 12
)

var (
	// secondHashPrefix is a prefix that a mulithash is prepended with when calculating a second hash
	secondHashPrefix = []byte("CR_DOUBLEHASH\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	// deriveKeyPrefix is a prefix that a multihash is prepended with when deriving an encryption key
	deriveKeyPrefix = []byte("CR_ENCRYPTIONKEY\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	// noncePrefix is a prefix that a multihash is prepended with when calculating a nonce
	noncePrefix = []byte("CR_NONCE\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
)

// SecondSHA returns SHA256 over the payload
func SHA256(payload, dest []byte) []byte {
	return sha256Multiple(dest, payload)
}

func sha256Multiple(dest []byte, payloads ...[]byte) []byte {
	h := sha256.New()
	for _, payload := range payloads {
		h.Write(payload)
	}
	return h.Sum(dest)
}

// SecondMultihash calculates SHA256 over the multihash and wraps it into another multihash with DBL_SHA256 codec
func SecondMultihash(mh multihash.Multihash) (multihash.Multihash, error) {
	digest := SHA256(append(secondHashPrefix, mh...), nil)
	return multihash.Encode(digest, multihash.DBL_SHA2_256)
}

// deriveKey derives encryptioin key from the passphrase using SHA256
func deriveKey(passphrase []byte) []byte {
	return SHA256(append(deriveKeyPrefix, passphrase...), nil)
}

// DecryptAES decrypts AES payload using the nonce and the passphrase
func DecryptAES(nonce, payload, passphrase []byte) ([]byte, error) {
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

// encryptAES uses AESGCM to encrypt the payload with the passphrase and a nonce derived from it.
// returns nonce and encrypted bytes.
func EncryptAES(payload, passphrase []byte) ([]byte, []byte, error) {
	// Derive the encryption key
	derivedKey := deriveKey([]byte(passphrase))

	// Create initialization vector (nonse) to be used during encryption
	// Nonce is derived from the passphrase concatenated with the payload so that the encrypted payloads
	// for the same multihash can be compared to each other without having to decrypt them, as it's not possible.
	payloadLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(payloadLen, uint64(len(payload)))
	nonce := sha256Multiple(nil, noncePrefix, payloadLen, payload, passphrase)[:nonceLen]

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

// DecryptValueKey decrypts the value key using the passphrase
func DecryptValueKey(valKey, mh multihash.Multihash) ([]byte, error) {
	return DecryptAES(valKey[:nonceLen], valKey[nonceLen:], mh)
}

// EncryptValueKey encrypts raw value key using the passpharse
func EncryptValueKey(valKey, mh multihash.Multihash) ([]byte, error) {
	nonce, encValKey, err := EncryptAES(valKey, mh)
	if err != nil {
		return nil, err
	}

	return append(nonce, encValKey...), nil
}

// DecryptMetadata decrypts metdata using the provided passphrase
func DecryptMetadata(encMetadata, valueKey []byte) ([]byte, error) {
	return DecryptAES(encMetadata[:nonceLen], encMetadata[nonceLen:], valueKey)
}

// EncryptMetadata encrypts metadata using the provided passphrase
func EncryptMetadata(metadata, valueKey []byte) ([]byte, error) {
	nonce, encValKey, err := EncryptAES(metadata, valueKey)
	if err != nil {
		return nil, err
	}

	return append(nonce, encValKey...), nil
}

// CreateValueKey creates value key from peer ID and context ID
func CreateValueKey(pid peer.ID, ctxID []byte) []byte {
	var b bytes.Buffer
	b.Grow(len(string(pid)) + len(ctxID))
	b.WriteString(string(pid))
	b.Write(ctxID)
	return b.Bytes()
}

// SplitValueKey splits value key into original components
func SplitValueKey(valKey []byte) (peer.ID, []byte, error) {
	// Extract multihiash from the value key before converting it to peer.ID.
	// Extracting peer.ID directly would fail the length check. There is no overhead in doing that as
	// none of the operations allocates new memory.
	l, mh, err := multihash.MHFromBytes(valKey)
	if err != nil {
		return "", nil, err
	}
	pid, err := peer.IDFromBytes(mh)
	if err != nil {
		return "", nil, err
	}
	return pid, valKey[l:], nil
}
