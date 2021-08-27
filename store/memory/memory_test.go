package memory_test

import (
	"testing"

	"github.com/filecoin-project/go-indexer-core/store/memory"
	"github.com/filecoin-project/go-indexer-core/store/test"
)

func TestE2E(t *testing.T) {
	s := memory.New()
	test.E2ETest(t, s)
}

func TestParallel(t *testing.T) {
	s := memory.New()
	test.ParallelUpdateTest(t, s)
}

func TestRemoveMany(t *testing.T) {
	s := memory.New()
	test.RemoveManyTest(t, s)
}
