package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults of a KindManager's options
const (
	DefaultWorkers     = 16
	DefaultRetryWindow = 30 * time.Minute
	DefaultStagger     = 300 * time.Millisecond
	// MaxWorkers is the most workers a KindManager runs
	MaxWorkers = 1024

	// A failing part waits minBackoff before its first retry, backoffFactor
	// times longer before each next one up to maxBackoff, and each wait
	// up to backoffJitter of it shorter or longer, so that parts that fail
	// together retry apart
	minBackoff    = time.Second
	maxBackoff    = time.Minute
	backoffFactor = 2
	backoffJitter = 0.2
)

type managerConfig struct {
	start       []byte
	workers     int
	retryWindow time.Duration
	stagger     time.Duration
	minBackoff  time.Duration
	maxBackoff  time.Duration
	progress    func([]byte) error
}

// ManagerOption sets an option of NewKindManager
type ManagerOption func(*managerConfig)

// WithStart sets the start of the first part, nil (the beginning) by
// default: the progress that an earlier run of the kind reported
func WithStart(start []byte) ManagerOption {
	return func(c *managerConfig) { c.start = start }
}

// WithWorkers sets the number of workers that Run starts with,
// DefaultWorkers by default. SetWorkers changes it while the kind runs.
func WithWorkers(n int) ManagerOption {
	return func(c *managerConfig) { c.workers = n }
}

// WithRetryWindow sets how long a part may keep failing with transient
// errors before Run stops with an error, DefaultRetryWindow by default
func WithRetryWindow(d time.Duration) ManagerOption {
	return func(c *managerConfig) { c.retryWindow = d }
}

// WithStagger sets the time between the starts of new workers,
// DefaultStagger by default, since a burst of new connections gets reset
func WithStagger(d time.Duration) ManagerOption {
	return func(c *managerConfig) { c.stagger = d }
}

// WithProgress sets a function that the manager calls with the kind's
// progress each time a part, or a part's stretch up to a partial stop, is
// written: the lowest start still registered. Every entry before it is
// written, so a later run can start there (see WithStart). The calls are
// serialized and each has the current progress, so a later call never
// reports an older progress. The function makes the target's writes
// durable before it stores the progress (see Target.Sync). An error from it
// stops the kind.
func WithProgress(f func(start []byte) error) ManagerOption {
	return func(c *managerConfig) { c.progress = f }
}

// KindManager copies one kind from a Source to a Target with a pool of
// workers.
//
// The start of each part waits in a queue. A worker takes a start, opens
// the part there, and enqueues the end that the source picked for it as
// the start of the next part, so the next worker can start while it
// streams. A part that runs to the end of the data has no end, so nothing
// is enqueued after it. Parts are taken in order, never overlap and leave
// no gap, and the manager never compares cursors.
//
// Every start is registered when it is enqueued, and stays registered
// until its part is written; a partial stop moves it to the cursor the
// part resumes from. The lowest registered start, the first one enqueued
// that is still registered, is the kind's progress. A part registers the
// next start before it is unregistered, and only the last part registers
// none, so the kind is done when no start is registered.
type KindManager struct {
	kind Kind
	src  Source
	dst  Target
	cfg  managerConfig

	// starts holds the next part's start. Each part enqueues at most one
	// start, after it is taken, so one slot is enough.
	starts chan *managedPart

	mu sync.Mutex
	// parts are the registered parts, in the order they were enqueued
	parts []*managedPart

	// done is closed when no part is registered: the kind is done
	done chan struct{}

	// ctx and fail are those of the running Run, nil before it. Run sets
	// them before it starts a worker, and they do not change after.
	ctx  context.Context
	fail context.CancelCauseFunc

	// targetWorkersCnt is the number of workers set
	targetWorkersCnt int

	// workerRunning is set for each worker ID with a worker workerRunning. A worker
	// clears it when it exits.
	workerRunning []bool

	// workersChanged is closed and replaced when the number of workers
	// changes, to wake the idle workers
	workersChanged chan struct{}

	// workersFinished waits for the workers to return
	workersFinished sync.WaitGroup

	// progressMu serializes the calls of cfg.progress, so that a later
	// call never reports an older progress. It is not mu: the progress
	// function syncs the target and stores the progress, and the workers
	// must not wait for that to register and unregister parts.
	progressMu sync.Mutex

	// The counters of Stats
	partsInFlight, partsWritten, entries, bytes, retries atomic.Int64
}

// KindStats is a snapshot of a KindManager's work since its Run started
type KindStats struct {
	Kind Kind `json:"kind"`
	// Workers is the number of workers set, and WorkersRunning the number
	// that run: more while workers over a lower number finish their parts
	Workers        int `json:"workers"`
	WorkersRunning int `json:"workersRunning"`
	// PartsInFlight is the number of parts that workers hold
	PartsInFlight int64 `json:"partsInFlight"`
	// PartsWritten is the number of parts written whole
	PartsWritten int64 `json:"partsWritten"`
	// Entries and Bytes count the entries written and their keys' and
	// values' bytes, including those of parts that are written again after
	// a failure
	Entries int64 `json:"entries"`
	Bytes   int64 `json:"bytes"`
	// Retries is the number of times a part failed and was tried again
	Retries int64 `json:"retries"`
}

// Stats returns a snapshot of the manager's work
func (m *KindManager) Stats() KindStats {
	m.mu.Lock()
	workers, running := m.targetWorkersCnt, 0
	for _, r := range m.workerRunning {
		if r {
			running++
		}
	}
	m.mu.Unlock()
	return KindStats{
		Kind:           m.kind,
		Workers:        workers,
		WorkersRunning: running,
		PartsInFlight:  m.partsInFlight.Load(),
		PartsWritten:   m.partsWritten.Load(),
		Entries:        m.entries.Load(),
		Bytes:          m.bytes.Load(),
		Retries:        m.retries.Load(),
	}
}

// managedPart is one part, from when its start is enqueued until it is
// written
type managedPart struct {
	// start is where the part, or the rest of it after a partial stop, starts
	start []byte
}

// NewKindManager returns a manager that copies kind from src to dst
func NewKindManager(kind Kind, src Source, dst Target, opts ...ManagerOption) *KindManager {
	cfg := managerConfig{
		workers:     DefaultWorkers,
		retryWindow: DefaultRetryWindow,
		stagger:     DefaultStagger,
		minBackoff:  minBackoff,
		maxBackoff:  maxBackoff,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &KindManager{
		kind:             kind,
		src:              src,
		dst:              dst,
		cfg:              cfg,
		targetWorkersCnt: cfg.workers,
		starts:           make(chan *managedPart, 1),
		done:             make(chan struct{}),
		workersChanged:   make(chan struct{}),
	}
}

// Run copies the kind. It returns nil once every part is written, and the
// error that stopped it otherwise: a permanent error, a part that kept
// failing for the retry window, an error of the progress function, or
// ctx's. A manager runs once.
func (m *KindManager) Run(ctx context.Context) error {
	ctx, fail := context.WithCancelCause(ctx)
	defer fail(nil)
	m.mu.Lock()
	m.ctx, m.fail = ctx, fail
	m.enqueue(m.cfg.start)
	m.setWorkers(m.targetWorkersCnt)
	m.mu.Unlock()

	var err error
	select {
	case <-m.done:
	case <-ctx.Done():
		err = context.Cause(ctx)
	}
	// Cancelling the run makes the idle workers exit
	fail(err)
	m.workersFinished.Wait()
	return err
}

// SetWorkers sets the number of workers of a running kind, from 0, which
// pauses the kind between parts, to MaxWorkers; WithWorkers sets the number
// it starts with. A worker over the new number finishes the part it holds
// before it exits; if the number grows back meanwhile, it goes on as one of
// them.
func (m *KindManager) SetWorkers(n int) error {
	if n < 0 || n > MaxWorkers {
		return fmt.Errorf("the number of workers must be from 0 to %d, not %d", MaxWorkers, n)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ctx == nil {
		return errors.New("the kind is not running; WithWorkers sets the number it starts with")
	}
	m.setWorkers(n)
	return nil
}

// setWorkers sets the number of workers to n. It starts a worker, a
// stagger apart, for each ID under n that has none running, and wakes the
// idle workers so that those from n up exit. Call it with mu held.
func (m *KindManager) setWorkers(n int) {
	m.targetWorkersCnt = n

	var started int
	for id := range n {
		if id == len(m.workerRunning) {
			m.workerRunning = append(m.workerRunning, false)
		}
		if m.workerRunning[id] {
			continue
		}
		m.workerRunning[id] = true

		delay := time.Duration(started) * m.cfg.stagger
		m.workersFinished.Go(func() { m.work(m.ctx, id, delay) })
		started++
	}

	// wake the idle workers so that those from n up exit
	close(m.workersChanged)
	m.workersChanged = make(chan struct{})
}

// work takes parts and copies them until the run ends, which is when the
// kind is done, or the number of workers drops to id or below. It first
// waits delay.
func (m *KindManager) work(ctx context.Context, id int, delay time.Duration) {
	// initial delay to spread startig new workers over time
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return
	}

	for {
		// The worker exits in the same lock that sees it over the number,
		// so a SetWorkers after it starts a new one
		m.mu.Lock()
		if id >= m.targetWorkersCnt {
			m.workerRunning[id] = false
			m.mu.Unlock()
			return
		}
		changed := m.workersChanged
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return

		case <-changed:
			// if change signal is received, the number of workers has changed,
			// go over the check at the beginning of the loop again to see if
			// this worker should still be running

		case p := <-m.starts:
			m.partsInFlight.Add(1)
			err := m.copyPart(ctx, p)
			m.partsInFlight.Add(-1)
			if err != nil {
				// part failure at this level means that all the workers should stop
				m.fail(err)
				return
			}
		}
	}
}

// enqueue registers a part that starts at start, and queues it. Call it
// with mu held.
func (m *KindManager) enqueue(start []byte) {
	p := &managedPart{start: start}
	m.parts = append(m.parts, p)
	m.starts <- p
}

// copyPart copies part p, and retries it until it is written or fails for
// good. It returns nil once the part is written, and the error that stops
// the kind otherwise.
func (m *KindManager) copyPart(ctx context.Context, p *managedPart) error {
	var (
		start = p.start
		// end is the part's end, once the source has picked it; until then
		// the part is opened open
		end          []byte
		err          error
		failingSince time.Time
		backoff      = m.cfg.minBackoff
	)

	for {
		var partial *PartialError

		end, err = m.copyOnce(ctx, p, start, end)
		switch {
		case err == nil:
			// Part finished successfully
			return m.reportProgress()

		case errors.As(err, &partial):
			// The stretch before the stop is written; the rest of the
			// part starts at its resume cursor
			start = partial.Resume
			failingSince, backoff = time.Time{}, m.cfg.minBackoff
			if err := m.reportProgress(); err != nil {
				return err
			}
			continue

		case ctx.Err() != nil:
			return context.Cause(ctx)

		case IsPermanent(err):
			return fmt.Errorf("%s part from %x: %w", m.kind, start, err)
		}

		// Transient error, back off and retry
		m.retries.Add(1)
		if failingSince.IsZero() {
			failingSince = time.Now()
		} else if time.Since(failingSince) >= m.cfg.retryWindow {
			return fmt.Errorf("%s part from %x kept failing for %s: %w", m.kind, start, m.cfg.retryWindow, err)
		}

		wait := time.Duration(float64(backoff) * (1 - backoffJitter + 2*backoffJitter*rand.Float64()))
		backoff = min(backoffFactor*backoff, m.cfg.maxBackoff)

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// copyOnce opens part p from start, closed at end when the source has
// picked the part's end and open otherwise, and writes its entries to the
// target. It returns the part's end: end, or the end that the source picks
// for an open request, which it enqueues as the next part's start. On a
// partial stop it writes the stretch before it, moves p's start to the
// resume cursor and returns the *PartialError. Once the part is written
// whole, it unregisters p and returns nil.
func (m *KindManager) copyOnce(ctx context.Context, p *managedPart, start, end []byte) ([]byte, error) {
	partEnd, entries, err := m.src.OpenStream(ctx, m.kind, start, end, 0)
	if err != nil {
		return end, err
	}

	switch {
	case end != nil:
		if !bytes.Equal(partEnd, end) {
			return end, PermanentError(fmt.Errorf("the source ended a part at %x, not at its end %x", partEnd, end))
		}

	case partEnd != nil:
		// TODO: should't we check partEnd > start instead of equal?
		if bytes.Equal(partEnd, start) {
			return end, PermanentError(fmt.Errorf("the source ended an open part at its start %x", start))
		}
		end = partEnd

		m.mu.Lock()
		m.enqueue(end)
		m.mu.Unlock()
	}

	var partial *PartialError
	var n, size int64
	err = m.dst.Write(ctx, m.kind, func(yield func(Entry, error) bool) {
		for e, err := range untilPartial(entries, &partial) {
			if err == nil {
				n++
				size += int64(len(e.Key) + len(e.Value))
			}
			if !yield(e, err) {
				return
			}
		}
	})
	m.entries.Add(n)
	m.bytes.Add(size)
	if err != nil {
		return end, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if partial != nil {
		p.start = partial.Resume
		return end, partial
	}
	m.partsWritten.Add(1)
	m.parts = slices.DeleteFunc(m.parts, func(q *managedPart) bool { return q == p })
	if len(m.parts) == 0 {
		close(m.done)
	}
	return end, nil
}

// reportProgress calls the progress function with the current progress
func (m *KindManager) reportProgress() error {
	if m.cfg.progress == nil {
		return nil
	}
	m.progressMu.Lock()
	defer m.progressMu.Unlock()
	m.mu.Lock()
	if len(m.parts) == 0 {
		// The kind is done; Run reports that
		m.mu.Unlock()
		return nil
	}
	progress := m.parts[0].start
	m.mu.Unlock()
	return m.cfg.progress(progress)
}

// untilPartial yields the entries until a *PartialError, which it stores
// in *partial instead of yielding, so the target writes the stretch before
// it
func untilPartial(entries iter.Seq2[Entry, error], partial **PartialError) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		for e, err := range entries {
			if errors.As(err, partial) {
				return
			}
			if !yield(e, err) {
				return
			}
		}
	}
}
