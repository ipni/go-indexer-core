package store

import (
	"github.com/filecoin-project/go-indexer-core"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Interface is the interface any value store used in the indexer. A value
// store keeps all values, unlike a cache which may only keep values up to some
// limit.
type Interface interface {
	// Get retrieves a slice of values for a CID
	Get(cid.Cid) ([]indexer.Value, bool, error)
	// Put stores an additional value for a CID if the value is not already stored
	Put(cid.Cid, indexer.Value) (bool, error)
	// PutMany stores a value for multiple CIDs
	PutMany([]cid.Cid, indexer.Value) error
	// Remove removes a value for a CID
	Remove(cid.Cid, indexer.Value) (bool, error)
	// RemoveMany removes a value from multiple CIDs
	RemoveMany([]cid.Cid, indexer.Value) error
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
