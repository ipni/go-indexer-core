//go:build !386

package bench_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	sth "github.com/ipld/go-storethehash/store"
)

func BenchmarkStore_Sth_PutConcurrency_W0_1(b *testing.B) {
	benchmarkStorePut(b, sthWithPutConcurrency(b, 1), workload0(b))
}

func BenchmarkStore_Sth_PutConcurrency_W0_2(b *testing.B) {
	benchmarkStorePut(b, sthWithPutConcurrency(b, 2), workload0(b))
}

func BenchmarkStore_Sth_PutConcurrency_W0_64(b *testing.B) {
	benchmarkStorePut(b, sthWithPutConcurrency(b, 64), workload0(b))
}

func BenchmarkStore_Sth_PutConcurrency_W1_1(b *testing.B) {
	benchmarkStorePut(b, sthWithPutConcurrency(b, 1), workload1(b))
}

func BenchmarkStore_Sth_PutConcurrency_W1_2(b *testing.B) {
	benchmarkStorePut(b, sthWithPutConcurrency(b, 2), workload1(b))
}

func BenchmarkStore_Sth_PutConcurrency_W1_64(b *testing.B) {
	benchmarkStorePut(b, sthWithPutConcurrency(b, 64), workload1(b))
}

func sthWithPutConcurrency(b *testing.B, c int) func() (indexer.Interface, error) {
	return func() (indexer.Interface, error) {
		return storethehash.New(context.Background(), b.TempDir(),
			c,
			sth.GCInterval(0),
			sth.SyncInterval(3*time.Second),
			sth.IndexBitSize(24),
		)
	}
}
