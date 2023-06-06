module github.com/ipni/go-indexer-core

go 1.19

require (
	// Note, cockroachdb/pebble has no tagged release. Instead, it uses branches.
	// The version below is from: https://github.com/cockroachdb/pebble/tree/crl-release-22.1
	// To update to latest, run: go get github.com/cockroachdb/pebble@crl-release-22.1
	github.com/cockroachdb/pebble v0.0.0-20220726144858-a78491c0086f
	github.com/gammazero/radixtree v0.3.1
	github.com/ipfs/go-cid v0.4.1
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipni/go-libipni v0.2.0
	github.com/libp2p/go-libp2p v0.27.4
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multihash v0.2.2
	github.com/multiformats/go-varint v0.0.7
	go.opencensus.io v0.24.0
	lukechampine.com/blake3 v1.1.7
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/klauspost/compress v1.16.4 // indirect
	github.com/klauspost/cpuid/v2 v2.2.4 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/mattn/go-isatty v0.0.18 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr v0.9.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multicodec v0.9.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/crypto v0.7.0 // indirect
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29 // indirect
	golang.org/x/sys v0.7.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)
