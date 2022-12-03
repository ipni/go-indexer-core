package bench_test

import (
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/bench"
)

func BenchmarkBinaryValueCodec_MarshalValue(b *testing.B) {
	benchmarkMarshalValue(b, indexer.BinaryValueCodec{})
}

func BenchmarkJsonValueCodec_MarshalValue(b *testing.B) {
	benchmarkMarshalValue(b, indexer.JsonValueCodec{})
}

func BenchmarkBinaryValueCodec_UnmarshalValue(b *testing.B) {
	benchmarkUnmarshalValue(b, indexer.BinaryValueCodec{})
}

func BenchmarkJsonValueCodec_UnmarshalValue(b *testing.B) {
	benchmarkUnmarshalValue(b, indexer.JsonValueCodec{})
}

func benchmarkMarshalValue(b *testing.B, subject indexer.ValueCodec) {
	values, size := valueCodecWorkload(b)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, v := range values {
				if _, err := subject.MarshalValue(v.Value); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func benchmarkUnmarshalValue(b *testing.B, subject indexer.ValueCodec) {
	values, size := valueCodecWorkload(b)
	var svs [][]byte
	for _, v := range values {
		sv, err := subject.MarshalValue(v.Value)
		if err != nil {
			b.Fatal(err)
		}
		svs = append(svs, sv)
	}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, sv := range svs {
				if _, err := subject.UnmarshalValue(sv); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func valueCodecWorkload(b *testing.B) ([]bench.GeneratedValue, int) {
	values, size := bench.GenerateRandomValues(b, rng(), bench.GeneratorConfig{
		NumProviders:         100,
		NumValuesPerProvider: func() uint64 { return 1_000 },
		// Do not generate entries; none is needed for value codec benchmarking.
		NumEntriesPerValue: func() uint64 { return 0 },
		ShuffleValues:      true,
	})
	return values, size
}
