package store

import (
	"github.com/ipni/go-indexer-core"
	"github.com/multiformats/go-multihash"
)

type Interface interface {
	// NewBatch starts a new batch operation that is specific to the datastore implementation
	NewBatch() interface{}

	// CommitBatch commits a batch that is specific to the datastore implementation
	CommitBatch(batch interface{}) error

	// CloseBatch closes a batch that is specific to the datastore implementation
	CloseBatch(batch interface{}) error

	// GetValue returns a value associated with the value key
	GetValue(valKey []byte) (*indexer.Value, error)

	// GetValueKeys returns value keys associated with the multihash
	GetValueKeys(multihash multihash.Multihash) ([][]byte, bool, error)

	// PutValue puts a value into the datastore and associates it with the value key using provided batch.
	// Returns a newly assigned value key.
	PutValue(valKey []byte, value indexer.Value, batch interface{}) error

	// PutValueKey puts a new mapping from multihash to the value key using provided batch
	PutValueKey(multihash multihash.Multihash, valKey []byte, batch interface{}) error

	// RemoveValue removes a value from the datastore
	RemoveValue(valKey []byte) error

	// RemoveValueKey removes a value key associated with the multihash
	RemoveValueKey(mh multihash.Multihash, valKey []byte, batch interface{}) error

	Size() (int64, error)

	Flush() error

	Close() error

	Iter() (indexer.Iterator, error)

	Stats() (*indexer.Stats, error)
}
