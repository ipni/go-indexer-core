package cache

import (
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Interface is the interface any result cache used in the indexer. A result
// cache only keeps results for completed requests, and may only store a
// limited quantity of values.
type Interface interface {
	// Get retrieves a slice of IndexEntry for a CID
	Get(cid.Cid) ([]entry.Value, bool, error)
	// Put stores an additional IndexEntry for a CID if the entry is not already stored
	Put(cid.Cid, entry.Value) (bool, error)
	// PutMany stores an IndexEntry for multiple CIDs
	PutMany([]cid.Cid, entry.Value) error
	// Remove removes an IndexEntry for a CID
	Remove(cid.Cid, entry.Value) (bool, error)
	// RemoveMany removes an IndexEntry from multiple CIDs
	RemoveMany([]cid.Cid, entry.Value) error
	// RemoveProvider removes all entries for specified provider.  This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(peer.ID) error
}
