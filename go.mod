module github.com/filecoin-project/go-indexer-core

go 1.18

require (
	github.com/akrylysov/pogreb v0.10.1
	// Note, cockroachdb/pebble has no tagged release. Instead, it uses branches.
	// The version below is from: https://github.com/cockroachdb/pebble/tree/crl-release-22.1
	// To update to latest, run: go get github.com/cockroachdb/pebble@crl-release-22.1
	github.com/cockroachdb/pebble v0.0.0-20220726144858-a78491c0086f
	github.com/gammazero/keymutex v0.1.0
	github.com/gammazero/radixtree v0.3.0
	github.com/gammazero/workerpool v1.1.3
	github.com/ipfs/go-cid v0.2.0
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipld/go-storethehash v0.2.6
	github.com/libp2p/go-libp2p-core v0.16.1
	github.com/multiformats/go-multihash v0.1.0
	github.com/multiformats/go-varint v0.0.6
	go.opencensus.io v0.23.0
	golang.org/x/crypto v0.0.0-20211117183948-ae814b36b871
)

require lukechampine.com/blake3 v1.1.7

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/btcsuite/btcd v0.22.1 // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.1.3 // indirect
	github.com/btcsuite/btcd/chaincfg/chainhash v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/gammazero/deque v0.2.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/go-cmp v0.5.4 // indirect
	github.com/klauspost/compress v1.11.7 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/libp2p/go-buffer-pool v0.0.2 // indirect
	github.com/libp2p/go-openssl v0.0.7 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.0.3 // indirect
	github.com/multiformats/go-base36 v0.1.0 // indirect
	github.com/multiformats/go-multiaddr v0.4.1 // indirect
	github.com/multiformats/go-multibase v0.0.3 // indirect
	github.com/multiformats/go-multicodec v0.4.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/spacemonkeygo/spacelog v0.0.0-20180420211403-2296661a0572 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.19.1 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/sys v0.0.0-20220906135438-9e1f76180b77 // indirect
)
