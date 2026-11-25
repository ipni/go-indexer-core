package pebble

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

// openKVsDB opens an in-memory Pebble database that holds the keys a, b and
// c, each with its upper-case letter as the value
func openKVsDB(t *testing.T) *pebble.DB {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, db.Set([]byte(k), []byte{k[0] - 'a' + 'A'}, pebble.NoSync))
	}
	return db
}

func TestPebbleKVsReadsAll(t *testing.T) {
	db := openKVsDB(t)
	snap := db.NewSnapshot()

	var got []string
	for kv, err := range pebbleKVs(t.Context(), snap, &pebble.IterOptions{UpperBound: []byte("c")}) {
		require.NoError(t, err)
		got = append(got, string(kv.key)+"="+string(kv.value))
	}
	require.Equal(t, []string{"a=A", "b=B"}, got)

	require.NoError(t, snap.Close())
	require.NoError(t, db.Close())
}

func TestPebbleKVsBreakClosesIterator(t *testing.T) {
	db := openKVsDB(t)

	n := 0
	for _, err := range pebbleKVs(t.Context(), db, &pebble.IterOptions{}) {
		require.NoError(t, err)
		n++
		break
	}
	require.Equal(t, 1, n)

	// Close fails when an iterator is still open
	require.NoError(t, db.Close())
}

func TestPebbleKVsContextCancel(t *testing.T) {
	db := openKVsDB(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	cause := errors.New("scan cancelled")

	var keys []string
	var gotErr error
	for kv, err := range pebbleKVs(ctx, db, &pebble.IterOptions{}) {
		if err != nil {
			gotErr = err
			continue
		}
		keys = append(keys, string(kv.key))
		cancel(cause)
	}
	require.Equal(t, []string{"a"}, keys)
	require.ErrorIs(t, gotErr, cause)

	require.NoError(t, db.Close())
}

func TestPebbleKVsNilOptions(t *testing.T) {
	db := openKVsDB(t)

	for range pebbleKVs(t.Context(), db, nil) {
		require.FailNow(t, "nil options yielded")
	}

	require.NoError(t, db.Close())
}
