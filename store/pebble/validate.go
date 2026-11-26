package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// ValidateRecords deletes the value records that the context map does not
// lead to, so that each context of a provider has at most one record. A
// record is kept when it sits under its provider's hash and either the
// context map has an entry for its provider and context that holds the
// record's token, or there is no such entry and the record is legacy: its
// token is the hash of its context ID. Every other record is deleted,
// including one whose key or value does not decode.
//
// It checks the records from position from, nil for the first, up to limit
// of them, which must be positive, and commits its deletions with a synced
// write before it returns. It returns the position to continue from, done once it has
// checked the last record, and how many records it deleted. A position is
// opaque to the caller.
//
// It is meant for a store with no other writers, such as one that an
// import is writing: an import copies records and context map entries at
// different times, so a context that changed on the source in between can
// leave records that no context leads to.
func (s *store) ValidateRecords(ctx context.Context, from []byte, limit int) (next []byte, done bool, deleted int, err error) {
	if limit < 1 {
		return nil, false, 0, fmt.Errorf("the limit of records to check must be positive, not %d", limit)
	}
	keygen := s.p.leaseBlake3Keyer()
	defer func() { _ = keygen.Close() }()
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()

	var checked int
	done = true
	for kv, err := range pebbleKVs(ctx, s.db, &pebble.IterOptions{
		LowerBound: append([]byte{byte(valueKeyPrefix)}, from...),
		UpperBound: []byte{byte(valueKeyPrefix) + 1},
	}) {
		if err != nil {
			return nil, false, 0, err
		}
		if checked == limit {
			done = false
			break
		}
		keep, err := s.recordLedTo(keygen, kv.key, kv.value)
		if err != nil {
			return nil, false, 0, err
		}
		if !keep {
			if err := b.Delete(kv.key, nil); err != nil {
				return nil, false, 0, err
			}
			deleted++
		}
		// The next position is just after this record's key
		next = append(append(next[:0], kv.key[1:]...), 0)
		checked++
	}
	if deleted > 0 {
		if err := b.Commit(pebble.Sync); err != nil {
			return nil, false, 0, err
		}
	}
	return next, done, deleted, nil
}

// recordLedTo reports whether the value record with key and value is the
// one that the context map, or the legacy rule, leads its context to
func (s *store) recordLedTo(keygen *blake3Keyer, key, value []byte) (bool, error) {
	if len(key) != 1+recordIDLength {
		return false, nil
	}
	v, err := s.vcodec.unmarshalValue(value)
	if err != nil {
		return false, nil
	}
	pidHash, err := keygen.hashPeerID(v.ProviderID)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(key[1:1+providerHashLen], pidHash) {
		return false, nil
	}
	ctxHash, err := keygen.hashContextID(v.ContextID)
	if err != nil {
		return false, err
	}
	token := key[1+providerHashLen:]
	mapKey := append(append([]byte{byte(contextMapKeyPrefix)}, pidHash...), ctxHash...)
	mapped, closer, err := s.db.Get(mapKey)
	switch {
	case errors.Is(err, pebble.ErrNotFound):
		return bytes.Equal(token, ctxHash), nil
	case err != nil:
		return false, err
	}
	defer func() { _ = closer.Close() }()
	return bytes.Equal(token, mapped), nil
}
