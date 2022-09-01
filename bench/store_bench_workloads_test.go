package bench_test

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-indexer-core/bench"
)

type workload struct {
	values []bench.GeneratedValue
	size   int
}

var w0, w1, w2, w3 *workload

func rng() *rand.Rand {
	// Use fixed rng to assure deterministic workload generation.
	const seed = 1413
	return rand.New(rand.NewSource(seed))
}

// workload0 generates 1000 values with total size of 32.522 MiB for 1 provider with 1000
// multihash entries for each value, without any duplicate entries.
func workload0(b testing.TB) *workload {
	if w0 == nil {
		values, size := bench.GenerateRandomValues(b, rng(), bench.GeneratorConfig{
			NumProviders:         1,
			NumValuesPerProvider: func() uint64 { return 1_000 },
			NumEntriesPerValue:   func() uint64 { return 1_000 },
			ShuffleValues:        true,
		})
		b.Logf("Workload 1 generated %d values with total size of %.3f MiB.\n", len(values), toMiB(size))
		w0 = &workload{
			values: values,
			size:   size,
		}
	}
	return w0
}

// workload1 is the same as workload1 but with 10 providers, with total size of 325.222 MiB
func workload1(b testing.TB) *workload {
	if w1 == nil {
		values, size := bench.GenerateRandomValues(b, rng(), bench.GeneratorConfig{
			NumProviders:         10,
			NumValuesPerProvider: func() uint64 { return 1_000 },
			NumEntriesPerValue:   func() uint64 { return 1_000 },
			ShuffleValues:        true,
		})
		b.Logf("Workload 1 generated %d values with total size of %.3f MiB.\n", len(values), toMiB(size))
		w1 = &workload{
			values: values,
			size:   size,
		}
	}
	return w1
}

// workload2 uses Zipf distribution to determine the number of values and entries in each value for
// 10 providers.
// This workload generates 1728 values with total size of 247.743 MiB.
func workload2(b testing.TB) *workload {
	if w2 == nil {
		r := rng()
		values, size := bench.GenerateRandomValues(b, r, bench.GeneratorConfig{
			NumProviders:         10,
			NumValuesPerProvider: rand.NewZipf(r, 1.001, 10_000, 50_000).Uint64,
			NumEntriesPerValue:   rand.NewZipf(r, 1.001, 10_000, 10_000).Uint64,
			ShuffleValues:        true,
		})
		b.Logf("Workload 2 generated %d values with total size of %.3f MiB.\n", len(values), toMiB(size))
		w2 = &workload{
			values: values,
			size:   size,
		}
	}
	return w2
}

// workload3 is the same as workload2 but with 30% chance that identical entries are
// provided by more than one provider.
// This workload generates 1385 values with total size of 67.466 MiB.
func workload3(b testing.TB) *workload {
	if w3 == nil {
		r := rng()
		values, size := bench.GenerateRandomValues(b, r, bench.GeneratorConfig{
			NumProviders:         10,
			NumValuesPerProvider: rand.NewZipf(r, 1.001, 10_000, 50_000).Uint64,
			NumEntriesPerValue:   rand.NewZipf(r, 1.001, 10_000, 10_000).Uint64,
			DuplicateEntries:     func() bool { return r.Float32() >= 0.3 },
			ShuffleValues:        true,
		})
		b.Logf("Workload 3 generated %d values with total size of %.3f MiB.\n", len(values), toMiB(size))
		w3 = &workload{
			values: values,
			size:   size,
		}
	}
	return w3
}

func toMiB(size int) float64 {
	return float64(size) / float64(1<<20)
}

func TestWorkloads_GenerateWithoutError(t *testing.T) {
	workload0(t)
	workload1(t)
	workload2(t)
	workload3(t)
}
