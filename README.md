# go-indexer-core
[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](https://protocol.ai)
[![Go Reference](https://pkg.go.dev/badge/github.com/filecoin-project/go-indexer-core.svg)](https://pkg.go.dev/github.com/filecoin-project/go-indexer-core)
[![Coverage Status](https://codecov.io/gh/filecoin-project/go-indexer-core/branch/main/graph/badge.svg)](https://codecov.io/gh/filecoin-project/go-indexer-core/branch/main)
> Storage specialized for indexing provider content

This is a storage system for storing and retrieving the mapping of content multihashes to provider data.  It is optimized for storing large numbers of multihashes mapping to relatively few provider data objects.  Provider data describes the providers of content referenced by a multihash and information on how to retrieve that content from the providers.  The data to store is given as a provider value object and a set of multihashes that are provided by that provider using that value object.  Data is retrieved by using a multihash to lookup the provider value objects.  Provider data can be updated and removed independent of the multihashes that map to it.

### Configurable Cache
An integrated cache is included to aid in fast index lookups.  The cache can be optionally disabled, and its size is configurable. The cache interface allows other cache implementations to be used.

See Usage Example for details.

### Choice of Persistent Storage
The persistent storage is provided by a choice of storage systems that include [storethehash](https://github.com/ipld/go-storethehash), [pogrep](https://github.com/akrylysov/pogreb#readme), and an in-memory implementation.  The storage interface allows any other storage system solution to be adapted.

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
	indexerCore.Put(value, cid1.Hash(), cid2.Hash())

	// Lookup provider data by multihash.
	values, found, err := indexerCore.Get(cid1.Hash())
	if err != nil {
		log.Fatal(err)
	}
	if found {
		log.Printf("Found %d values for cid1", len(values))
	}
	
	// Remove provider values by contextID, and multihashes that map to them.
	if err = indexerCore.RemoveProviderContext(peerID, ctxID); err != nil {
		log.Fatal(err)                                                                                                                   
	}
}
```

## License
This project is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
