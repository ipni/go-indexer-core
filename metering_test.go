package indexer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanCancelledError(t *testing.T) {
	require.ErrorIs(t, ScanCancelledError(""), ErrScanCancelled)
	require.Equal(t, ErrScanCancelled, ScanCancelledError(""))

	err := ScanCancelledError("paused for gc")
	require.ErrorIs(t, err, ErrScanCancelled)
	require.Equal(t, "user cancelled: paused for gc", err.Error())
}
