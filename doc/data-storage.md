# Data Storage

Many multihashes map to relatively few provider records. A multihash is used to lookup the provider records describing where that multihash is available. A provider ID and context ID are used to lookup a unique provider record.

Provider data can be updated and removed independently from the multihashes that map to it. Provider data is uniquely identified by a provider ID and a context ID. The context ID is given to the indexer core as part of the provider data value. When a provider data object is updated, all subsequent multihash queries will return the new value for that data whether the queries are cached or not.

Unique provider records are looked up by a provider key derived from the provider ID and context ID. A multihash can map to multiple provider records, since the same content may be stored by different providers and be part of separate deals at the same provider. To provide this functionality, the indexer core maps each multihash to a list of provider keys, and maps each provider key to a provider record.

In the Pebble store, each multihash entry stores those provider keys directly. A separate context-map entry, keyed by the blake3 hashes of the provider ID and context ID, assigns each live `(provider ID, context ID)` an opaque token that forms the context half of the provider key. Removing a context deletes that map entry so a later reuse of the same context ID gets a new provider key, and multihashes that still point at the old key no longer resolve. Records written before the map existed keep using a blake3 hash of the context ID until that context is removed.

![indexer-core-data](indexer-core-data.png)
