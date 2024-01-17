module github.com/ipni/go-indexer-core

go 1.21

toolchain go1.21.6

require (
	// Note, cockroachdb/pebble has no tagged release. Instead, it uses branches.
	// The version below is from: https://github.com/cockroachdb/pebble/tree/crl-release-22.1
	// To update to latest, run: go get github.com/cockroachdb/pebble@crl-release-22.1
	github.com/cockroachdb/pebble v0.0.0-20240116160621-aba7c0ca6483
	github.com/gammazero/radixtree v0.3.1
	github.com/ipfs/go-cid v0.4.1
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipni/go-libipni v0.5.9
	github.com/libp2p/go-libp2p v0.32.2
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multihash v0.2.3
	github.com/multiformats/go-varint v0.0.7
	go.opencensus.io v0.24.0
	lukechampine.com/blake3 v1.2.1
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/errors v1.11.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/getsentry/sentry-go v0.18.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.17.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr v0.12.1 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multicodec v0.9.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.14.0 // indirect
	github.com/prometheus/client_model v0.4.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)
