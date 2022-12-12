package indexer

import (
	"context"
	"errors"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// ErrStatsNotSupported signals that an indexer.Interface does not support Stats calculation.
var ErrStatsNotSupported = errors.New("stats is not supported by store")

type Interface interface {
	// Get retrieves a slice of Value for a multihash.
	Get(multihash.Multihash) ([]Value, bool, error)

	// Put stores a Value and adds a mapping from each of the given multihashes
	// to that Value. If the Value has the same ProviderID and ContextID as a
	// previously stored Value, then update the metadata in the stored Value
	// with the metadata from the provided Value. Call Put without any
	// multihashes to only update existing values.
	Put(Value, ...multihash.Multihash) error

	// Remove removes the mapping of each multihash to the specified value.
	Remove(Value, ...multihash.Multihash) error

	// RemoveProvider removes all values for specified provider. This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(context.Context, peer.ID) error

	// RemoveProviderContext removes all values for specified provider that
	// have the specified contextID. This is used when a provider no longer
	// provides values for a particular context.
	RemoveProviderContext(providerID peer.ID, contextID []byte) error

	// Size returns the total bytes of storage used to store the indexed
	// content in persistent storage. This does not include memory used by any
	// in-memory cache that the indexer implementation may have, as that would
	// only contain a limited quantity of data and not represent the total
	// amount of data stored by the indexer.
	Size() (int64, error)

	// Flush commits any changes to the value storage,
	Flush() error

	// Close gracefully closes the store flushing all pending data from memory,
	Close() error

	// Iter creates a new value store iterator.
	Iter() (Iterator, error)

	// Stats returns statistical information about the indexed values.
	// If unsupported by the backing store, ErrStatsNotSupported is returned.
	Stats() (*Stats, error)
}

type Datastore interface {
	// NewBatch starts a new batch operation that is specific to the datastore implementation
	NewBatch() interface{}

	// CommitBatch commits a batch that is specific to the datastore implementation
	CommitBatch(batch interface{}) error

	// CloseBatch closes a batch that is specific to the datastore implementation
	CloseBatch(batch interface{}) error

	// GetValue returns a value associated with the value key
	GetValue(valKey []byte) (*Value, error)

	// GetValueKeys returns value keys associated with the multihash
	GetValueKeys(multihash multihash.Multihash) ([][]byte, bool, error)

	// PutValue puts a value into the datastore and associates it with the value key using provided batch.
	// Returns a newly assigned value key.
	PutValue(valKey []byte, value Value, batch interface{}) error

	// PutValueKey puts a new mapping from multihash to the value key using provided batch
	PutValueKey(multihash multihash.Multihash, valKey []byte, batch interface{}) error

	// RemoveValue removes a value from the datastore
	RemoveValue(valKey []byte) error

	// RemoveValueKey removes a value key associated with the multihash
	RemoveValueKey(mh multihash.Multihash, valKey []byte, batch interface{}) error

	Size() (int64, error)

	Flush() error

	Close() error

	Iter() (Iterator, error)

	Stats() (*Stats, error)
}

// Iterator iterates multihashes and values in the value store. Any write
// operation invalidates the iterator.
type Iterator interface {
	// Next returns the next multihash and the value it indexer. Returns io.EOF
	// when finished iterating.
	Next() ([]byte, [][]byte, error)

	// Close closes the iterator releasing any resources that may be occupied by it.
	// The iterator will no longer be usable after a call to this function and is
	// discarded.
	Close() error
}

// Stats provides statistics about the indexed values.
type Stats struct {
	// MultihashCount is the number of unique multihashes indexed.
	MultihashCount uint64
}
