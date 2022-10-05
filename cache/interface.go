package cache

import (
	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// Interface is the interface any result cache used in the indexer. A result
// cache only keeps results for completed requests, and may only store a
// limited quantity of values.
type Interface interface {
	// Get retrieves a slice of Values that a multihash index maps to.
	Get(multihash.Multihash) ([]indexer.Value, bool)

	// Put stores a Value and adds a mapping from each of the given multihashs
	// to that Value. If the Value has the same ProviderID and ContextID as a
	// previously stored Value, then update the metadata in the stored Value
	// with the metadata from the provided Value. Call Put without any
	// multihashes to only update existing values.
	Put(indexer.Value, ...multihash.Multihash) int

	// Remove removes the mapping of each multihash to the specified Value.
	// Returns the number of indexe mappings removed.
	Remove(indexer.Value, ...multihash.Multihash) int

	// RemoveProvider removes all values for the specified provider, and all
	// indexes that map to those values. This is used when a provider is no
	// longer indexed by the indexer. Returns the number of indexes that were
	// mapped to removed values.
	RemoveProvider(peer.ID) int

	// RemoveProviderContext removes the value that has the specified
	// providerID and contextID, and all indexes mapped to that value. This is
	// used when a provider no longer provides values for a particular context.
	// Returns the number of indexes that were mapped to the value.
	RemoveProviderContext(providerID peer.ID, contextID []byte) int

	// IndexCount returns the numbert of cached multihash-to-values entries.
	IndexCount() int

	// Stats returns a Stats snapshot of cache values.
	Stats() Stats
}

type Stats struct {
	// Indexes counts the indexes cached; each index is multihash->[]Value.
	Indexes int
	// Values counts cached values, whether or not they are reachable by index.
	Values int
	// Evictions counts the number of multihashes evicted from cache.
	Evictions int
}
