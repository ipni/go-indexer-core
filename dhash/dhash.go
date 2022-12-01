package dhash

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"

	"golang.org/x/crypto/pbkdf2"
)

const (
	iterations = 1000
	keyLen     = 32
	SaltLen    = 8
	NonceLen   = 12
)

// EncryptAES uses AESGCM to encrypt the payload with passphrase and randomly geenrated salt and nonce.
// returns salt, nonce and encrypted bytes
func EncryptAES(payload, passphrase []byte) ([]byte, []byte, []byte, error) {
	// Generate random salt
	salt := make([]byte, SaltLen)
	_, err := rand.Read(salt)
	if err != nil {
		return nil, nil, nil, err
	}

	// Derive the encryption key
	derivedKey, err := deriveKey([]byte(passphrase), salt)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create initialization vector (nonse) to be used during encryption
	nonce := make([]byte, NonceLen)
	_, err = rand.Read(nonce)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create cypher and seal the data
	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, nil, nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, nil, err
	}

	encrypted := aesgcm.Seal(nil, nonce, []byte(payload), nil)

	return salt, nonce, encrypted, nil
}

// SecondSHA returns SHA256 over the payload
func SecondSHA(payload []byte) []byte {
	return sha256.New().Sum(payload)
}

// deriveKey derives a key using pbkdf2 from passphrase and salt
func deriveKey(passphrase, salt []byte) ([]byte, error) {
	return pbkdf2.Key(passphrase, salt, iterations, keyLen, sha256.New), nil
}
