package pogreb_test

import (
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/store/pogreb"
	"github.com/ipni/go-indexer-core/store/test"
)

func initBenchStore(b *testing.B) indexer.Interface {
	s, err := pogreb.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func BenchmarkGet(b *testing.B) {
	test.BenchMultihashGet(initBenchStore(b), b)
}
func BenchmarkParallelGet(b *testing.B) {
	test.BenchParallelMultihashGet(initBenchStore(b), b)
}

// To run this storage benchmarks run:
// TEST_STORAGE=true go test -v -timeout=30m
func TestBenchSingle10MB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initPogreb(t), "10MB", t)
}

func TestBenchSingle100MB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initPogreb(t), "100MB", t)
}

func TestBenchSingle1GB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initPogreb(t), "1GB", t)
}
