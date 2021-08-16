package indexer

import (
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Interface interface {
	// Get retrieves a slice of entry.Value for a CID
	Get(c cid.Cid) ([]entry.Value, bool, error)

	// Put stores a value for a CID if the value is not already stored.  New
	// values are added to those that are already stored for the CID.
	Put(c cid.Cid, value entry.Value) (bool, error)

	// PutMany stores one entry.Value for multiple CIDs
	PutMany(cids []cid.Cid, value entry.Value) error

	// Remove removes a value for the specified CID
	Remove(c cid.Cid, value entry.Value) (bool, error)

	// RemoveMany removes the specified value from multiple CIDs
	RemoveMany(cids []cid.Cid, value entry.Value) error

	// RemoveProvider removes all values for specified provider.  This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(providerID peer.ID) error

	// Size returns the total storage capacity, in bytes, used to by the value
	// store.  This does not include any space used by the result cache, as it
	// only contains a limited quantity of results for previous queries, so is
	// not representative of the total amount of data stored by the indexer.
	Size() (int64, error)
}
