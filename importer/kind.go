package importer

// The standard kinds of an indexer import. A kind's wire keys and values
// are the protocol's encoding, the same for every store. They carry no
// store prefix, because the kind implies the keyspace: each implementation
// adds or strips its own prefix and converts its own value layout.
//
// A kind's name starts with its group: "vs/" for the value store's kinds
// and "ds/" for the indexer datastore's. A kind's parts are served at
// importhttp.PartPath followed by its name, so a deployment that serves
// the two groups from different services routes them by that prefix.
//
// In the definitions below, H(x) is the first 10 bytes of the BLAKE3 hash
// of x. A record ID is 20 bytes: H(provider ID), then a 10-byte context
// part. The context part is the token that the context map holds for the
// provider and context ID, or H(context ID) for a legacy record that has no
// map entry.
const (
	// KindRecords is the value records. The key is the record ID. The value
	// is the record in go-indexer-core's binary value encoding: the provider
	// ID, the context ID and the metadata, each as a uvarint length and the
	// bytes.
	KindRecords Kind = "vs/records"
	// KindContextMap is the context map. The key is H(provider ID), then
	// H(context ID), 20 bytes. The value is the 10-byte context token that
	// the context's record ID ends with.
	KindContextMap Kind = "vs/contextmap"
	// KindMultihashes is the multihash index. The key is the multihash
	// bytes. The value is the record IDs of the multihash's live entries,
	// concatenated, 20 bytes each. A multihash with none is not sent.
	KindMultihashes Kind = "vs/multihashes"
	// KindAdProcessed is the processed-ad markers of the indexer's
	// datastore. The key is the ad CID as a string. The value is the
	// marker as stored: the status byte, 1 for a processed ad or 3 for a
	// skipped one, optionally followed by the time the marker was written,
	// 8 bytes of little-endian microseconds since the Unix epoch. Markers of
	// other statuses are not sent.
	KindAdProcessed Kind = "ds/adProcessed"
	// KindAdSkipReason is why skipped ads were skipped. The key is the ad
	// CID as a string, and the value the reason as stored.
	KindAdSkipReason Kind = "ds/adSkipReason"
	// KindSync is the latest processed ad of each publisher. The key is the
	// publisher's peer ID as a string, and the value the ad CID's bytes. A
	// publisher whose latest ad is marked for resync (status 2) is not
	// sent.
	KindSync Kind = "ds/sync"
	// KindProviders is the registry's provider records. The key is the
	// provider's peer ID as a string, and the value the record as stored.
	KindProviders Kind = "ds/pinfo"
)

// AllKinds lists every standard kind, once each
var AllKinds = []Kind{
	KindRecords,
	KindContextMap,
	KindMultihashes,
	KindAdProcessed,
	KindAdSkipReason,
	KindSync,
	KindProviders,
}
