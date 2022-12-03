//go:build !386

package bench_test

import (
	"context"
	"testing"
	"time"

	pb2 "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	sth "github.com/ipld/go-storethehash/store"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/go-indexer-core/store/pogreb"
	"github.com/ipni/go-indexer-core/store/storethehash"
)

func BenchmarkStore_PebblePut_W0(b *testing.B) {
	benchmarkStorePut(b, newPebbleSubject(b), workload0(b))
}

func BenchmarkStore_PebbleGet_W0(b *testing.B) {
	benchmarkStoreGet(b, newPebbleSubject(b), workload0(b))
}

func BenchmarkStore_PebblePut_W1(b *testing.B) {
	benchmarkStorePut(b, newPebbleSubject(b), workload1(b))
}

func BenchmarkStore_PebbleGet_W1(b *testing.B) {
	benchmarkStoreGet(b, newPebbleSubject(b), workload1(b))
}

func BenchmarkStore_PebblePut_W2(b *testing.B) {
	benchmarkStorePut(b, newPebbleSubject(b), workload2(b))
}

func BenchmarkStore_PebbleGet_W2(b *testing.B) {
	benchmarkStoreGet(b, newPebbleSubject(b), workload2(b))
}

func BenchmarkStore_PebblePut_W3(b *testing.B) {
	benchmarkStorePut(b, newPebbleSubject(b), workload3(b))
}

func BenchmarkStore_PebbleGet_W3(b *testing.B) {
	benchmarkStoreGet(b, newPebbleSubject(b), workload3(b))
}

func BenchmarkStore_PogrebPut_W0(b *testing.B) {
	benchmarkStorePut(b, newPogrebSubject(b), workload0(b))
}

func BenchmarkStore_PogrebGet_W0(b *testing.B) {
	benchmarkStoreGet(b, newPogrebSubject(b), workload0(b))
}

func BenchmarkStore_PogrebPut_W1(b *testing.B) {
	benchmarkStorePut(b, newPogrebSubject(b), workload1(b))
}

func BenchmarkStore_PogrebGet_W1(b *testing.B) {
	benchmarkStoreGet(b, newPogrebSubject(b), workload1(b))
}

func BenchmarkStore_PogrebPut_W2(b *testing.B) {
	benchmarkStorePut(b, newPogrebSubject(b), workload2(b))
}

func BenchmarkStore_PogrebGet_W2(b *testing.B) {
	benchmarkStoreGet(b, newPogrebSubject(b), workload2(b))
}

func BenchmarkStore_PogrebPut_W3(b *testing.B) {
	benchmarkStorePut(b, newPogrebSubject(b), workload3(b))
}

func BenchmarkStore_PogrebGet_W3(b *testing.B) {
	benchmarkStoreGet(b, newPogrebSubject(b), workload3(b))
}

func BenchmarkStore_StorethehashPut_W0(b *testing.B) {
	benchmarkStorePut(b, sthSubject(b), workload0(b))
}

func BenchmarkStore_StorethehashGet_W0(b *testing.B) {
	benchmarkStoreGet(b, sthSubject(b), workload0(b))
}

func BenchmarkStore_StorethehashPut_W1(b *testing.B) {
	benchmarkStorePut(b, sthSubject(b), workload1(b))
}

func BenchmarkStore_StorethehashGet_W1(b *testing.B) {
	benchmarkStoreGet(b, sthSubject(b), workload1(b))
}

func BenchmarkStore_StorethehashPut_W2(b *testing.B) {
	benchmarkStorePut(b, sthSubject(b), workload2(b))
}

func BenchmarkStore_StorethehashGet_W2(b *testing.B) {
	benchmarkStoreGet(b, sthSubject(b), workload2(b))
}

func BenchmarkStore_StorethehashPut_W3(b *testing.B) {
	benchmarkStorePut(b, sthSubject(b), workload3(b))
}

func BenchmarkStore_StorethehashGet_W3(b *testing.B) {
	benchmarkStoreGet(b, sthSubject(b), workload3(b))
}

func BenchmarkStore_MemoryPut_W0(b *testing.B) {
	benchmarkStorePut(b, newMemorySubject, workload0(b))
}

func BenchmarkStore_MemoryGet_W0(b *testing.B) {
	benchmarkStoreGet(b, newMemorySubject, workload0(b))
}

func BenchmarkStore_MemoryPut_W1(b *testing.B) {
	benchmarkStorePut(b, newMemorySubject, workload1(b))
}

func BenchmarkStore_MemoryGet_W1(b *testing.B) {
	benchmarkStoreGet(b, newMemorySubject, workload1(b))
}

func BenchmarkStore_MemoryPut_W2(b *testing.B) {
	benchmarkStorePut(b, newMemorySubject, workload2(b))
}

func BenchmarkStore_MemoryGet_W2(b *testing.B) {
	benchmarkStoreGet(b, newMemorySubject, workload2(b))
}

func BenchmarkStore_MemoryPut_W3(b *testing.B) {
	benchmarkStorePut(b, newMemorySubject, workload3(b))
}

func BenchmarkStore_MemoryGet_W3(b *testing.B) {
	benchmarkStoreGet(b, newMemorySubject, workload3(b))
}

func newPebbleSubject(b *testing.B) func() (indexer.Interface, error) {
	return func() (indexer.Interface, error) {
		// Default options copied from cockroachdb with the addition of 1GiB cache.
		// See:
		// - https://github.com/cockroachdb/cockroach/blob/v22.1.6/pkg/storage/pebble.go#L479
		pebbleOpts := &pb2.Options{
			BytesPerSync:                10 << 20, // 10 MiB
			WALBytesPerSync:             10 << 20, // 10 MiB
			MaxConcurrentCompactions:    10,
			MemTableSize:                64 << 20, // 64 MiB
			MemTableStopWritesThreshold: 4,
			LBaseMaxBytes:               64 << 20, // 64 MiB
			L0CompactionThreshold:       2,
			L0StopWritesThreshold:       1000,
			WALMinSyncInterval:          func() time.Duration { return 30 * time.Second },
		}

		pebbleOpts.Experimental.ReadCompactionRate = 10 << 20 // 20 MiB
		pebbleOpts.Experimental.MinDeletionRate = 128 << 20   // 128 MiB

		const numLevels = 7
		pebbleOpts.Levels = make([]pb2.LevelOptions, numLevels)
		for i := 0; i < numLevels; i++ {
			l := &pebbleOpts.Levels[i]
			l.BlockSize = 32 << 10       // 32 KiB
			l.IndexBlockSize = 256 << 10 // 256 KiB
			l.FilterPolicy = bloom.FilterPolicy(10)
			l.FilterType = pb2.TableFilter
			if i > 0 {
				l.TargetFileSize = pebbleOpts.Levels[i-1].TargetFileSize * 2
			}
			l.EnsureDefaults()
		}
		pebbleOpts.Levels[numLevels-1].FilterPolicy = nil
		pebbleOpts.Cache = pb2.NewCache(1 << 30) // 1 GiB

		return pebble.New(b.TempDir(), pebbleOpts)
	}
}

func newPogrebSubject(b *testing.B) func() (indexer.Interface, error) {
	return func() (indexer.Interface, error) {
		return pogreb.New(b.TempDir())
	}
}

func sthSubject(b *testing.B) func() (indexer.Interface, error) {
	return func() (indexer.Interface, error) {
		return storethehash.New(context.Background(), b.TempDir(),
			64,
			sth.GCInterval(0),
			sth.SyncInterval(time.Second),
			sth.IndexBitSize(24),
		)
	}
}

func newMemorySubject() (indexer.Interface, error) {
	return memory.New(), nil
}

func benchmarkStorePut(b *testing.B, newSubject func() (indexer.Interface, error), w *workload) {
	subject, err := newSubject()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := subject.Close(); err != nil {
			b.Fatal()
		}
	}()
	b.SetBytes(int64(w.size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, v := range w.values {
				if err := subject.Put(v.Value, v.Entries...); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.StopTimer()
	reportSize(b, subject)
}

func benchmarkStoreGet(b *testing.B, newSubject func() (indexer.Interface, error), w *workload) {
	subject, err := newSubject()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := subject.Close(); err != nil {
			b.Fatal()
		}
	}()

	// Populate the subject with data in preparation for Get benchmark
	for _, v := range w.values {
		if err := subject.Put(v.Value, v.Entries...); err != nil {
			b.Fatal(err)
		}
	}
	flush(b, subject)

	b.SetBytes(int64(w.size))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, want := range w.values {
			Entries:
				for _, e := range want.Entries {
					if gotValues, found, err := subject.Get(e); err != nil {
						b.Fatal(err)
					} else if !found {
						b.Fatalf("failed to find %s", e.B58String())
					} else {
						for _, got := range gotValues {
							if want.Equal(got) {
								continue Entries
							}
						}
						b.Fatalf("failed to find value %v for entry %s", want, e.B58String())
					}
				}
			}
		}
	})
	b.StopTimer()
	reportSize(b, subject)
}

func reportSize(b *testing.B, subject indexer.Interface) {
	flush(b, subject)
	size, err := subject.Size()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(size), "size")
}

func flush(b *testing.B, subject indexer.Interface) {
	if err := subject.Flush(); err != nil {
		b.Fatal(err)
	}
}
