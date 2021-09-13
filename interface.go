package indexer

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type Interface interface {
	// Get retrieves a slice of Value for a multihash
	Get(multihash.Multihash) ([]Value, bool, error)

	// Put stores a value for a multihash if the value is not already stored.
	// New values are added to those that are already stored for the multihash.
	Put(multihash.Multihash, Value) (bool, error)

	// PutMany stores one Value for multiple multihashes
	PutMany([]multihash.Multihash, Value) error

	// Remove removes a value for the specified multihash
	Remove(multihash.Multihash, Value) (bool, error)

	// RemoveMany removes the specified value from multiple multihashes
	RemoveMany([]multihash.Multihash, Value) error

	// RemoveProvider removes all values for specified provider.  This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(providerID peer.ID) error

	// Size returns the total bytes of storage used to store the indexed
	// content in persistent storage.  This does not include memory used by any
	// in-memory cache that the indexer implementation may have, as that would
	// only contain a limited quantity of data and not represent the total
	// amount of data stored by the indexer.
	Size() (int64, error)
}
