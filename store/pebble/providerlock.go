package pebble

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

type providersLock struct {
	m     sync.Mutex
	locks map[peer.ID]chan struct{}
}

func newProvidersLock() *providersLock {
	return &providersLock{
		locks: make(map[peer.ID]chan struct{}),
	}
}

// lock acquires the provider lock.
// On success, unlock is non-nil and must be called exactly once.
// On error, unlock is nil.
func (l *providersLock) lock(
	ctx context.Context,
	providerID peer.ID,
) (unlockFunc func(), err error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		l.m.Lock()
		lock, ok := l.locks[providerID]
		if !ok {
			// Not locked, acquire
			lock = make(chan struct{})
			l.locks[providerID] = lock
			l.m.Unlock()

			return func() {
				l.m.Lock()
				defer l.m.Unlock()

				delete(l.locks, providerID)
				close(lock)
			}, nil
		}
		l.m.Unlock()

		// Already locked, wait for release
		select {
		case <-lock:
			// Released, try again
			continue

		case <-ctx.Done():
			// Give up
			return nil, ctx.Err()
		}
	}
}
