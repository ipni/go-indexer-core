# go-indexer-core
[![Go Reference](https://pkg.go.dev/badge/github.com/filecoin-project/go-indexer-core.svg)](https://pkg.go.dev/github.com/filecoin-project/go-indexer-core)
> Storage specialized for indexing provider content

This is a storage system for storing and retrieving the mapping of content multihashes to provider data.  It is optimized for storing large numbers of multihashes mapping to relatively few provider data objects.  Provider data describes the providers of content referenced by a multihash and information on how to retrieve that content from the providers.  The data to store is given as a provider value object and a set of multihashes that are provided by that provider using that value object.  Data is retrieved by using a multihash to lookup the provider value objects.  Provider data can be updated and removed independent of the multihashes that map to it.

### Configurable Cache
An integrated cache is included to aid in fast index lookups.  The cache can be optionally disabled, and its size is configurable. The cache interface allows other cache implementations to be used.

### Choice of Persistent Storage
The persistent storage is provided by a choice of storage systems that include [storethehash](https://github.com/ipld/go-storethehash), [pogrep](https://github.com/akrylysov/pogreb#readme), and an in-memory implementation.  The storage interface allows any other storage system solution to be adapted.

## Install
```sh
 go get github.com/filecoin-project/go-indexer-core
 ```
 
 ## Usage
 ```go
 import "github.com/filecoin-project/go-indexer-core"
```

See [pkg.go.dev documentation](https://pkg.go.dev/github.com/filecoin-project/go-indexer-core)


## License
This project is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
