package indexer_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	providerCount     = 100
	valuesPerProvider = 1_000
)

func BenchmarkBinaryValueSerde_MarshalValue(b *testing.B) {
	benchmarkMarshalValue(b, indexer.BinaryValueSerde{})
}

func BenchmarkJsonValueSerde_MarshalValue(b *testing.B) {
	benchmarkMarshalValue(b, indexer.JsonValueSerde{})
}

func BenchmarkBinaryValueSerde_UnmarshalValue(b *testing.B) {
	benchmarkUnmarshalValue(b, indexer.BinaryValueSerde{})
}

func BenchmarkJsonValueSerde_UnmarshalValue(b *testing.B) {
	benchmarkUnmarshalValue(b, indexer.JsonValueSerde{})
}

func benchmarkMarshalValue(b *testing.B, subject indexer.ValueSerde) {
	values, size := generateRandomValues(b, providerCount, valuesPerProvider)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, v := range values {
				if _, err := subject.MarshalValue(v); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func benchmarkUnmarshalValue(b *testing.B, subject indexer.ValueSerde) {
	values, size := generateRandomValues(b, providerCount, valuesPerProvider)
	var svalues [][]byte
	for _, v := range values {
		sv, err := subject.MarshalValue(v)
		if err != nil {
			b.Fatal(err)
		}
		svalues = append(svalues, sv)
	}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, sv := range svalues {
				if _, err := subject.UnmarshalValue(sv); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func generateRandomValues(b testing.TB, providerCount, valsPerProvider int) ([]indexer.Value, int) {
	// Use fixed RNG for determinism across benchmarks.
	rng := rand.New(rand.NewSource(1413))
	ctxID := make([]byte, 64) // max allowed context ID size
	md := make([]byte, 32)
	pid := make([]byte, 32)
	var values []indexer.Value
	var totalSize int
	var size int
	var err error
	for i := 0; i < providerCount; i++ {
		if _, err = rng.Read(pid); err != nil {
			b.Fatal(err)
		}
		_, pub, err := crypto.GenerateEd25519Key(bytes.NewReader(pid))
		if err != nil {
			b.Fatal(err)
		}
		provId, err := peer.IDFromPublicKey(pub)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < valsPerProvider; j++ {
			if size, err = rng.Read(ctxID); err != nil {
				b.Fatal(err)
			}
			totalSize += size
			if size, err = rng.Read(md); err != nil {
				b.Fatal(err)
			}
			totalSize += size
			v := indexer.Value{
				ProviderID:    provId,
				ContextID:     ctxID,
				MetadataBytes: md,
			}
			totalSize += provId.Size()
			values = append(values, v)
		}
	}
	return values, totalSize
}
