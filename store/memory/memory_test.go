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

func TestRemove(t *testing.T) {
	s := memory.New()
	test.RemoveTest(t, s)
}

func TestRemoveProviderContext(t *testing.T) {
	s := memory.New()
	test.RemoveProviderContextTest(t, s)
}

func TestRemoveProvider(t *testing.T) {
	s := memory.New()
	test.RemoveProviderTest(t, s)
}
