package storethehash_test

import (
	"testing"

	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/test"
)

func initBenchStore() store.Interface {
	s, err := initSth()
	if err != nil {
		panic(err)
	}
	return s
}

func BenchmarkGet(b *testing.B) {
	test.BenchCidGet(initBenchStore(), b)
}
func BenchmarkParallelGet(b *testing.B) {
	test.BenchParallelCidGet(initBenchStore(), b)
}

// To run this storage benchmarks run:
// TEST_STORAGE=true go test -v -timeout=30m
func TestBenchSingle10MB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initBenchStore(), "10MB", t)
}

func TestBenchSingle100MB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initBenchStore(), "100MB", t)
}

func TestBenchSingle1GB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initBenchStore(), "1GB", t)
}
