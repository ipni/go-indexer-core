package pebble

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testProviderID peer.ID = "provider"

func TestProvidersLock_RelockAfterUnlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newProvidersLock()
		unlock, err := l.lock(t.Context(), testProviderID)
		require.NoError(t, err)
		unlock()

		unlock, err = l.lock(t.Context(), testProviderID)
		require.NoError(t, err)
		unlock()
	})
}

func TestProvidersLock_DifferentProvidersDoNotBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newProvidersLock()
		started := make(chan struct{}, 2)
		release := make(chan struct{})
		var wg sync.WaitGroup
		for _, id := range []peer.ID{"a", "b"} {
			wg.Go(func() {
				unlock, err := l.lock(t.Context(), id)
				if !assert.NoError(t, err) {
					started <- struct{}{}
					return
				}
				defer unlock()
				started <- struct{}{}
				<-release
			})
		}

		for range 2 {
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("different providers blocked each other")
			}
		}
		close(release)
		wg.Wait()
	})
}

func TestProvidersLock_SameProviderBlocksUntilUnlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newProvidersLock()
		unlock, err := l.lock(t.Context(), testProviderID)
		require.NoError(t, err)

		acquired := make(chan struct{})
		go func() {
			nextUnlock, err := l.lock(t.Context(), testProviderID)
			if !assert.NoError(t, err) {
				return
			}
			close(acquired)
			nextUnlock()
		}()

		select {
		case <-acquired:
			t.Fatal("second lock acquired while the first was held")
		case <-time.After(50 * time.Millisecond):
		}

		unlock()

		select {
		case <-acquired:
		case <-time.After(2 * time.Second):
			t.Fatal("second lock was not acquired after unlock")
		}
	})
}

func TestProvidersLock_CancelWhileWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newProvidersLock()
		unlock, err := l.lock(t.Context(), testProviderID)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		errCh := make(chan error, 1)
		go func() {
			_, err := l.lock(ctx, testProviderID)
			errCh <- err
		}()

		time.Sleep(time.Second)
		cancel()
		require.ErrorIs(t, <-errCh, context.Canceled)

		unlock()

		nextUnlock, err := l.lock(t.Context(), testProviderID)
		require.NoError(t, err)
		nextUnlock()
	})
}

func TestProvidersLock_CanceledContextDoesNotAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := newProvidersLock()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		unlock, err := l.lock(ctx, testProviderID)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, unlock)

		next, err := l.lock(t.Context(), testProviderID)
		require.NoError(t, err)
		next()
	})
}

func TestProvidersLock_CriticalSection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const goroutines = 64

		l := newProvidersLock()

		var in atomic.Int32
		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				unlock, err := l.lock(t.Context(), testProviderID)
				if !assert.NoError(t, err) {
					return
				}
				defer unlock()

				assert.EqualValues(t, 1, in.Add(1))
				defer in.Add(-1)

				time.Sleep(time.Second)
			})
		}
		wg.Wait()
		require.False(t, t.Failed())
		require.EqualValues(t, 0, in.Load())
	})
}
