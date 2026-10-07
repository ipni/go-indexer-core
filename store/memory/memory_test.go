package memory_test

import (
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-indexer-core/store/test"
)

func TestConformance(t *testing.T) {
	test.RunConformance(t, func(*testing.T) indexer.Interface {
		return memory.New()
	})
}
