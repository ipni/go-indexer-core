# go-indexer-core
[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![Go Reference](https://pkg.go.dev/badge/github.com/filecoin-project/go-indexer-core.svg)](https://pkg.go.dev/github.com/filecoin-project/go-indexer-core)
[![Coverage Status](https://codecov.io/gh/filecoin-project/go-indexer-core/branch/main/graph/badge.svg)](https://codecov.io/gh/filecoin-project/go-indexer-core/branch/main)
> Storage specialized for indexing provider content

The indexer-core is a key-value store that is optimized for storing large numbers of multihashes mapping to relatively few provider data objects. A multihash (CID without codec) uniquely identifies a piece of content, and the provider data describes where and how to retrieve the content.

Content is indexed by giving a provider data object (the value) and a set of multihashes (keys) that map to that value. Typically, the provider value represents a storage deal and the multihash keys identify content stored within that deal. To subsequently retrieve a provider value, the indexer-core is given a multihash key to lookup.

[Data Storage](doc/data-storage.md) provides more detail on how data is stored by the indexer-core.

This indexer-core is the component of an indexer that provides data storage and retrieval for content index data. An indexer must also supply all the service functionality necessary to create an indexing service, which is not included in the indexer-core component.

### Configurable Cache

An integrated cache is included to aid in fast index lookups. By default the cache is configured as a retrieval cache, meaning that items are only stored in the cache when index data is looked up, and will speed up repeated lookups of the same data. The cache can be optionally disabled, and its size is configurable. The cache interface allows alternative cache implementations to be used if desired.

See Usage Example for details.

### Choice of Persistent Storage

The persistent storage is provided by a choice of storage systems that include [storethehash](https://github.com/ipld/go-storethehash), [pogrep](https://github.com/akrylysov/pogreb#readme), and an in-memory implementation. The storage interface allows any other storage system solution to be adapted.

See Usage Example for details.

## Install
```sh
 go get github.com/filecoin-project/go-indexer-core
 ```
 
 ## Usage
 ```go
 import "github.com/filecoin-project/go-indexer-core"
```

See [pkg.go.dev documentation](https://pkg.go.dev/github.com/filecoin-project/go-indexer-core)

### Example
```go
package main

import (
	"log"
	"os"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

func main() {
	// Configuration values.
	const valueStoreDir = "/tmp/indexvaluestore"
	const storeType = "sth"
	const cacheSize = 65536

	// Create value store of configured type.
	os.Mkdir(valueStoreDir, 0770)
	var valueStore indexer.Interface
	var err error
	if storeType == "sth" {
		valueStore, err = storethehash.New(valueStoreDir)
	}
	if storeType == "prgreb" {
		valueStore, err = pogreb.New(valueStoreDir)
	}
	if err != nil {
		log.Fatal(err)
	}

	// Create result cache, or disabled it.
	var resultCache cache.Interface
	if cacheSize > 0 {
		resultCache = radixcache.New(cacheSize)
	} else {
		log.Print("Result cache disabled")
	}

	// Create indexer core.
	indexerCore := engine.New(resultCache, valueStore)

	// Put some index data into indexer core.
	cid1, _ := cid.Decode("QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB")
	cid2, _ := cid.Decode("QmUaPc2U1nUJeVj6HxBxS5fGxTWAmpvzwnhB8kavMVAotE")
	peerID, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	ctxID := []byte("someCtxID")
	value := indexer.Value{
		ProviderID:    peerID,
		ContextID:     ctxID,
		MetadataBytes: []byte("someMetadata"),
	}
	err = indexerCore.Put(value, cid1.Hash(), cid2.Hash())
	if err != nil {
		log.Fatal(err)
	}

	// Lookup provider data by multihash.
	values, found, err := indexerCore.Get(cid1.Hash())
	if err != nil {
		log.Fatal(err)
	}
	if found {
		log.Printf("Found %d values for cid1", len(values))
	}
	
	// Remove provider values by contextID, and multihashes that map to them.
	err = indexerCore.RemoveProviderContext(peerID, ctxID)
	if err != nil {
		log.Fatal(err)                                                                                                                   
	}
}
```

## License
This project is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
