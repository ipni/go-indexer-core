package storethehash_test

import (
	"context"
	"testing"

	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/store/storethehash"
	"github.com/ipni/go-indexer-core/store/test"
)

func initBenchStore(b *testing.B, putConcurrency int) indexer.Interface {
	s, err := storethehash.New(context.Background(), b.TempDir(), putConcurrency)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func BenchmarkGet(b *testing.B) {
	test.BenchMultihashGet(initBenchStore(b, 1), b)
}
func BenchmarkParallelGet(b *testing.B) {
	test.BenchParallelMultihashGet(initBenchStore(b, 1), b)
}

func BenchmarkGetConcurrent(b *testing.B) {
	test.BenchMultihashGet(initBenchStore(b, 64), b)
}
func BenchmarkParallelGetConcurrent(b *testing.B) {
	test.BenchParallelMultihashGet(initBenchStore(b, 64), b)
}

// To run this storage benchmarks run:
// TEST_STORAGE=true go test -v -timeout=30m
func TestBenchSingle10MB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initSth(t), "10MB", t)
}

func TestBenchSingle100MB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initSth(t), "100MB", t)
}

func TestBenchSingle1GB(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initSth(t), "1GB", t)
}

func TestBenchSingle10MBConcurren(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initSth(t, 64), "10MB", t)
}

func TestBenchSingle100MBConcurren(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initSth(t, 64), "100MB", t)
}

func TestBenchSingle1GBConcurren(t *testing.T) {
	test.SkipStorage(t)
	test.BenchReadAll(initSth(t, 64), "1GB", t)
}
