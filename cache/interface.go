package cache

import (
	"github.com/filecoin-project/go-indexer-core"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Interface is the interface any result cache used in the indexer. A result
// cache only keeps results for completed requests, and may only store a
// limited quantity of values.
type Interface interface {
	// Get retrieves a slice of indexer.Value for a CID
	Get(cid.Cid) ([]indexer.Value, bool, error)
	// Put stores an additional indexer.Value for a CID if the value is not already stored
	Put(cid.Cid, indexer.Value) (bool, error)
	// PutMany stores an indexer.Value for multiple CIDs
	PutMany([]cid.Cid, indexer.Value) error
	// Remove removes an indexer.Value for a CID
	Remove(cid.Cid, indexer.Value) (bool, error)
	// RemoveMany removes an indexer.Value from multiple CIDs
	RemoveMany([]cid.Cid, indexer.Value) error
	// RemoveProvider removes all entries for specified provider.  This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(peer.ID) error
}
