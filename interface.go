package indexer

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Interface interface {
	// Get retrieves a slice of Value for a CID
	Get(c cid.Cid) ([]Value, bool, error)

	// Put stores a value for a CID if the value is not already stored.  New
	// values are added to those that are already stored for the CID.
	Put(c cid.Cid, value Value) (bool, error)

	// PutMany stores one Value for multiple CIDs
	PutMany(cids []cid.Cid, value Value) error

	// Remove removes a value for the specified CID
	Remove(c cid.Cid, value Value) (bool, error)

	// RemoveMany removes the specified value from multiple CIDs
	RemoveMany(cids []cid.Cid, value Value) error

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
