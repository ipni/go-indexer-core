package store

import (
	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

// Interface is the interface any value store used in the indexer. A value
// store keeps all values, unlike a cache which may only keep values up to some
// limit.
type Interface interface {
	// Get retrieves a slice of values for a multihash
	Get(multihash.Multihash) ([]indexer.Value, bool, error)
	// ForEach iterates multihashes, calling the provided function for each
	// multihash index visited
	ForEach(indexer.IterFunc) error
	// Put stores an additional value for a multihash if the value is not already stored
	Put(multihash.Multihash, indexer.Value) (bool, error)
	// PutMany stores a value for multiple multihashes
	PutMany([]multihash.Multihash, indexer.Value) error
	// Remove removes a value for a multihash
	Remove(multihash.Multihash, indexer.Value) (bool, error)
	// RemoveMany removes a value from multiple multihashes
	RemoveMany([]multihash.Multihash, indexer.Value) error
	// RemoveProvider removes all values for specified provider.  This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(peer.ID) error
	// Size returns the total storage capacity being used
	Size() (int64, error)
	// Flush commits changes to storage
	Flush() error
	// Close gracefully closes the store flushing all pending data from memory
	Close() error
}
