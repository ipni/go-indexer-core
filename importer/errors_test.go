package importer_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/require"
)

func TestPermanentError(t *testing.T) {
	base := errors.New("base")
	err := importer.PermanentError(base)
	require.True(t, importer.IsPermanent(err))
	require.ErrorIs(t, err, base)
	require.Equal(t, "base", err.Error())

	wrapped := fmt.Errorf("context: %w", err)
	require.True(t, importer.IsPermanent(wrapped))

	require.False(t, importer.IsPermanent(base))
	require.False(t, importer.IsPermanent(nil))
	require.NoError(t, importer.PermanentError(nil))
}
