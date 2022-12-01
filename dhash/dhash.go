package dhash

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
)

const (
	keyLen   = 32
	NonceLen = 12
)

// EncryptAES uses AESGCM to encrypt the payload with passphrase and randomly geenrated salt and nonce.
// returns salt, nonce and encrypted bytes
func EncryptAES(payload, passphrase []byte) ([]byte, []byte, error) {
	// Derive the encryption key
	derivedKey := deriveKey([]byte(passphrase))

	// Create initialization vector (nonse) to be used during encryption
	nonce := make([]byte, NonceLen)
	_, err := rand.Read(nonce)
	if err != nil {
		return nil, nil, err
	}

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

// SecondSHA returns SHA256 over the payload
func SecondSHA(payload []byte) []byte {
	h := sha256.Sum256(payload)
	return h[:]
}

// deriveKey derives a key using pbkdf2 from passphrase and salt
func deriveKey(passphrase []byte) []byte {
	return SecondSHA(append([]byte("AESGCM"), passphrase...))
}
