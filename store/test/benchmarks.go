package test

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	testDataDir = "../test/test_data/"
	testDataExt = ".data"
)

// prepare reads a multihash list and imports it into the value store getting it
// ready for benchmarking.
func prepare(s indexer.Interface, size string, t *testing.T) {
	testFile := fmt.Sprint(testDataDir, size, testDataExt)
	file, err := os.OpenFile(testFile, os.O_RDONLY, 0644)
	if err != nil {
		// If the file does not exist then skip and do not fail test
		if os.IsNotExist(err) {
			t.Skipf("Test file %q not found, needs to be generated)", testFile)
		}
		t.Fatalf("could not open input file: %v", err)
	}
	defer file.Close()

	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")

	out := make(chan multihash.Multihash)
	errOut := make(chan error, 1)
	go ReadCids(file, out, errOut)

	metadataBytes := []byte("dummy-metadata")
	for mh := range out {
		value := indexer.Value{
			ProviderID:    p,
			ContextID:     []byte(mh),
			MetadataBytes: metadataBytes,
		}
		err = s.Put(value, mh)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = <-errOut
	if err != nil {
		t.Fatal(err)
	}

}

// readAll reads all of the multihashes from a file and tries to get it from
// the value store.
func readAll(s indexer.Interface, size string, m *metrics, t *testing.T) {
	file, err := os.OpenFile(fmt.Sprint(testDataDir, size, testDataExt), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	out := make(chan multihash.Multihash)
	errOut := make(chan error, 1)
	go ReadCids(file, out, errOut)

	for mh := range out {
		now := time.Now()
		_, found, err := s.Get(mh)
		if err != nil || !found {
			t.Errorf("multihash not found")
		}
		m.getTime.add(time.Since(now).Microseconds())

	}
	err = <-errOut
	if err != nil {
		t.Fatal(err)
	}

}

// Benchmark the average time per get by all multihashes and the total storage used.
func BenchReadAll(s indexer.Interface, size string, t *testing.T) {
	m := initMetrics()
	prepare(s, size, t)
	readAll(s, size, m, t)
	err := s.Flush()
	if err != nil {
		t.Fatal(err)
	}

	report(s, m, true, t)
}

// Benchmark single thread get operation
func BenchMultihashGet(s indexer.Interface, b *testing.B) {
	mhs := RandomMultihashes(1)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("dummy-metadata"),
	}

	mhs = RandomMultihashes(4096)
	err := s.Put(value, mhs...)
	if err != nil {
		panic(err)
	}

	// Bench average time for a single get
	b.Run("Get single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, ok, _ := s.Get(mhs[i%len(mhs)])
			if !ok {
				panic("missing multihash")
			}
		}
	})

	// Test time to fetch certain amount of requests
	for testCount := 1024; testCount < 10240; testCount *= 2 {
		b.Run(fmt.Sprint("Get", testCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N*testCount; i++ {
				_, ok, _ := s.Get(mhs[i%len(mhs)])
				if !ok {
					panic("missing multihash")
				}
			}
		})
	}
}

func BenchParallelMultihashGet(s indexer.Interface, b *testing.B) {
	mhs := RandomMultihashes(1)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("dummy-metadata"),
	}

	mhs = RandomMultihashes(4096)
	err := s.Put(value, mhs...)
	if err != nil {
		panic(err)
	}
	rand.Seed(time.Now().UnixNano())

	// Benchmark the average request time for different number of go routines.
	for rout := 10; rout <= 100; rout += 10 {
		b.Run(fmt.Sprint("Get parallel", rout), func(b *testing.B) {
			var wg sync.WaitGroup
			ch := make(chan bool)
			for i := 0; i < rout; i++ {
				wg.Add(1)
				go func(wg *sync.WaitGroup, ch chan bool) {
					// Each go routine starts fetching multihashes from different offset.
					// TODO: Request follow a zipf distribution.
					off := rand.Int()
					// Wait for all routines to be started
					<-ch
					for i := 0; i < b.N; i++ {
						_, ok, _ := s.Get(mhs[(off+i)%len(mhs)])
						if !ok {
							panic("missing multihash")
						}

					}
					wg.Done()
				}(&wg, ch)
			}
			b.ReportAllocs()
			b.ResetTimer()
			close(ch)
			wg.Wait()
		})
	}
}

type metric struct {
	val int64
	n   uint64
}

func (m *metric) add(val int64) {
	m.val += val
	m.n++
}

func (m *metric) avg() float64 {
	return float64(m.val) / float64(m.n)
}

type metrics struct {
	getTime *metric
}

func initMetrics() *metrics {
	return &metrics{
		getTime: &metric{},
	}
}

func report(s indexer.Interface, m *metrics, storage bool, t *testing.T) {
	memSize, _ := s.Size()
	avgT := m.getTime.avg() / 1000
	t.Log("Avg time per get (ms):", avgT)
	if storage {
		sizeMB := float64(memSize) / 1000000
		t.Log("Memory size (MB):", sizeMB)
	}
}

func SkipStorage(t *testing.T) {
	if os.Getenv("TEST_STORAGE") == "" {
		t.SkipNow()
	}
}
