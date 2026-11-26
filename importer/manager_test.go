package importer_test

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
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/storeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testKind = importer.KindSync

// testSource is a Source of sorted entries in memory. It plans open parts
// with storeutil.PlanPart and stops every part after testStopBytes, which
// is far shorter than a part, so most parts stop partially and resume.
// fail, when set, is called on every open and every entry, and its error
// fails that open or ends the entries. end, when set, replaces the end
// that OpenStream returns.
type testSource struct {
	mu      sync.Mutex
	entries []importer.Entry
	opens   atomic.Int64
	fail    func(start []byte) error
	end     func(start, end []byte) []byte
	// openDelay is how long each open takes
	openDelay time.Duration
	// openParts counts the open requests that do not resume a part
	openParts atomic.Int64
}

const (
	testPartBytes = 512
	testStopBytes = 200
)

func testEntries(n int) []importer.Entry {
	out := make([]importer.Entry, n)
	for i := range out {
		out[i] = importer.Entry{Key: fmt.Appendf(nil, "k%06d", i), Value: fmt.Appendf(nil, "v%d", i)}
	}
	return out
}

// add adds entries after every entry the source has
func (s *testSource) add(entries ...importer.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entries...)
}

func (s *testSource) snapshot() []importer.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.entries)
}

// from yields the entries in [start, end); a nil end is the end of the data
func (s *testSource) from(entries []importer.Entry, start, end []byte) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		i, _ := slices.BinarySearchFunc(entries, start, func(e importer.Entry, k []byte) int { return bytes.Compare(e.Key, k) })
		for _, e := range entries[i:] {
			if end != nil && bytes.Compare(e.Key, end) >= 0 {
				return
			}
			if s.fail != nil {
				if err := s.fail(e.Key); err != nil {
					yield(importer.Entry{}, err)
					return
				}
			}
			if !yield(e, nil) {
				return
			}
		}
	}
}

func (s *testSource) OpenStream(ctx context.Context, _ importer.Kind, start, end []byte, _ uint64) ([]byte, iter.Seq2[importer.Entry, error], error) {
	s.opens.Add(1)
	if end == nil {
		s.openParts.Add(1)
	}
	time.Sleep(s.openDelay)
	if s.fail != nil {
		if err := s.fail(start); err != nil {
			return nil, nil, err
		}
	}
	entries := s.snapshot()
	if end != nil {
		partEnd := end
		if s.end != nil {
			partEnd = s.end(start, end)
		}
		return partEnd, storeutil.StreamPart(ctx, nil, s.from(entries, start, end), testStopBytes), nil
	}
	partEnd, head, next, err := storeutil.PlanPart(func(from []byte) iter.Seq2[importer.Entry, error] {
		return s.from(entries, from, nil)
	}, start, testPartBytes, func(key []byte) int { return len(key) })
	if err != nil {
		return nil, nil, err
	}
	var rest iter.Seq2[importer.Entry, error]
	if next != nil {
		rest = s.from(entries, next, partEnd)
	}
	if s.end != nil {
		partEnd = s.end(start, nil)
	}
	return partEnd, storeutil.StreamPart(ctx, head, rest, testStopBytes), nil
}

// testTarget is a Target of entries in memory. fail, when set, is called
// on every entry, and its error fails the write.
type testTarget struct {
	mu      sync.Mutex
	entries map[string]string
	fail    func(key []byte) error
	// delay is how long each write takes
	delay time.Duration
	// writing and maxWriting count the writes in progress, and the most
	// at once
	writing, maxWriting atomic.Int64
	// stored counts the entries stored, including those written again
	stored atomic.Int64
}

func newTestTarget() *testTarget {
	return &testTarget{entries: make(map[string]string)}
}

func (t *testTarget) Write(ctx context.Context, _ importer.Kind, entries iter.Seq2[importer.Entry, error]) error {
	n := t.writing.Add(1)
	defer t.writing.Add(-1)
	if n > t.maxWriting.Load() {
		t.maxWriting.Store(n)
	}
	if t.delay > 0 {
		select {
		case <-time.After(t.delay):
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	for e, err := range entries {
		if err != nil {
			return err
		}
		if t.fail != nil {
			if err := t.fail(e.Key); err != nil {
				return err
			}
		}
		t.mu.Lock()
		t.entries[string(e.Key)] = string(e.Value)
		t.mu.Unlock()
		t.stored.Add(1)
	}
	return nil
}

func (t *testTarget) Sync() error { return nil }

// requireHas checks that the target holds exactly entries
func (t *testTarget) requireHas(tb testing.TB, entries []importer.Entry) {
	tb.Helper()
	t.mu.Lock()
	defer t.mu.Unlock()
	want := make(map[string]string, len(entries))
	for _, e := range entries {
		want[string(e.Key)] = string(e.Value)
	}
	require.Equal(tb, want, t.entries)
}

// testProgressCheck returns a progress function that checks that every
// entry before each progress is in the target, and that the progress never
// goes back
func testProgressCheck(t *testing.T, src *testSource, dst *testTarget, calls *atomic.Int64) func([]byte) error {
	var last []byte
	return func(progress []byte) error {
		calls.Add(1)
		assert.GreaterOrEqual(t, bytes.Compare(progress, last), 0, "the progress went back")
		last = progress
		dst.mu.Lock()
		defer dst.mu.Unlock()
		for _, e := range src.snapshot() {
			if bytes.Compare(e.Key, progress) >= 0 {
				break
			}
			_, ok := dst.entries[string(e.Key)]
			assert.True(t, ok, "%s is before the progress %s and not written", e.Key, progress)
		}
		return nil
	}
}

func TestKindManagerCopies(t *testing.T) {
	src := &testSource{entries: testEntries(5000)}
	dst := newTestTarget()
	var calls atomic.Int64
	m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(4), importer.WithStagger(0),
		importer.WithProgress(testProgressCheck(t, src, dst, &calls)))
	require.NoError(t, m.Run(t.Context()))
	dst.requireHas(t, src.entries)
	require.Greater(t, src.opens.Load(), int64(20), "the kind is copied in many parts")
	require.Positive(t, calls.Load())
}

func TestKindManagerEmptyKind(t *testing.T) {
	src := &testSource{}
	dst := newTestTarget()
	m := importer.NewKindManager(testKind, src, dst, importer.WithStagger(0))
	require.NoError(t, m.Run(t.Context()))
	dst.requireHas(t, nil)
	require.EqualValues(t, 1, src.opens.Load())
}

// TestKindManagerResumes checks that a run from a start copies the entries
// from there on
func TestKindManagerResumes(t *testing.T) {
	src := &testSource{entries: testEntries(1000)}
	dst := newTestTarget()
	m := importer.NewKindManager(testKind, src, dst, importer.WithStart(src.entries[600].Key), importer.WithStagger(0))
	require.NoError(t, m.Run(t.Context()))
	dst.requireHas(t, src.entries[600:])
}

// TestKindManagerRetries runs kinds whose opens, streams and writes fail
// at random with transient errors, and checks that every entry is copied
func TestKindManagerRetries(t *testing.T) {
	for seed := range uint64(20) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var mu sync.Mutex
				rng := rand.New(rand.NewPCG(seed, 2))
				fails := func(p float64) func([]byte) error {
					return func([]byte) error {
						mu.Lock()
						defer mu.Unlock()
						if rng.Float64() < p {
							return errors.New("transient")
						}
						return nil
					}
				}
				src := &testSource{entries: testEntries(2000), fail: fails(0.005)}
				dst := newTestTarget()
				dst.fail = fails(0.002)
				var calls atomic.Int64
				m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(8),
					importer.WithProgress(testProgressCheck(t, src, dst, &calls)))
				require.NoError(t, m.Run(t.Context()))
				dst.requireHas(t, src.entries)
			})
		})
	}
}

// TestKindManagerLastPartGrows checks that data added after the last part
// was opened is copied when a retry of that part gets an end
func TestKindManagerLastPartGrows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		all := testEntries(3000)
		src := &testSource{entries: slices.Clone(all[:1000])}
		var cut atomic.Bool
		src.fail = func(key []byte) error {
			// Cut the stream of the last part once, at its last entry,
			// and add the rest of the data before its retry
			if bytes.Equal(key, all[999].Key) && cut.CompareAndSwap(false, true) {
				src.add(all[1000:]...)
				return errors.New("cut off")
			}
			return nil
		}
		dst := newTestTarget()
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(4))
		require.NoError(t, m.Run(t.Context()))
		require.True(t, cut.Load())
		dst.requireHas(t, all)
	})
}

func TestKindManagerPermanentError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entries := testEntries(2000)
		src := &testSource{entries: entries}
		src.fail = func(key []byte) error {
			if bytes.Equal(key, entries[1500].Key) {
				return importer.PermanentError(errors.New("broken"))
			}
			return nil
		}
		m := importer.NewKindManager(testKind, src, newTestTarget(), importer.WithWorkers(4))
		start := time.Now()
		err := m.Run(t.Context())
		require.ErrorContains(t, err, "broken")
		require.True(t, importer.IsPermanent(err))
		require.Zero(t, time.Since(start), "a permanent error was retried")
	})
}

// TestKindManagerRetryWindow checks that a part that keeps failing stops
// the kind after the retry window, and not before
func TestKindManagerRetryWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entries := testEntries(2000)
		src := &testSource{entries: entries}
		src.fail = func(key []byte) error {
			if bytes.Equal(key, entries[1500].Key) {
				return errors.New("busy")
			}
			return nil
		}
		m := importer.NewKindManager(testKind, src, newTestTarget(), importer.WithWorkers(4), importer.WithRetryWindow(10*time.Minute))
		start := time.Now()
		err := m.Run(t.Context())
		require.ErrorContains(t, err, "kept failing")
		require.ErrorContains(t, err, "busy")
		require.GreaterOrEqual(t, time.Since(start), 10*time.Minute)
		require.Less(t, time.Since(start), 12*time.Minute)
	})
}

// TestKindManagerWorkers checks that no part is copied with 0 workers, and
// that raising the number of workers copies the kind
func TestKindManagerWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		dst := newTestTarget()
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(0))
		errc := make(chan error)
		go func() { errc <- m.Run(t.Context()) }()
		synctest.Wait()
		require.Zero(t, src.opens.Load(), "a paused kind opened a part")

		require.NoError(t, m.SetWorkers(3))
		require.NoError(t, <-errc)
		dst.requireHas(t, src.entries)

		require.Error(t, m.SetWorkers(-1))
		require.Error(t, m.SetWorkers(importer.MaxWorkers+1))
	})
}

func TestKindManagerStopsWithContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		m := importer.NewKindManager(testKind, src, newTestTarget(), importer.WithWorkers(0))
		ctx, cancel := context.WithCancel(t.Context())
		errc := make(chan error)
		go func() { errc <- m.Run(ctx) }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-errc, context.Canceled)
	})
}

func TestKindManagerProgressError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		m := importer.NewKindManager(testKind, src, newTestTarget(),
			importer.WithProgress(func([]byte) error { return errors.New("no disk") }))
		require.ErrorContains(t, m.Run(t.Context()), "no disk")
	})
}

// TestKindManagerPartialStops checks that parts that stop partially resume
// where they stopped, and that the progress moves past each stop: every
// part here is longer than a stop
func TestKindManagerPartialStops(t *testing.T) {
	src := &testSource{entries: testEntries(2000)}
	dst := newTestTarget()
	var calls atomic.Int64
	check := testProgressCheck(t, src, dst, &calls)
	var progress []string
	m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(1), importer.WithStagger(0),
		importer.WithProgress(func(p []byte) error {
			if len(progress) == 0 || progress[len(progress)-1] != string(p) {
				progress = append(progress, string(p))
			}
			return check(p)
		}))
	require.NoError(t, m.Run(t.Context()))
	dst.requireHas(t, src.entries)
	// With one worker, each open but the last, a part's or a resume's,
	// moves the progress
	require.EqualValues(t, src.opens.Load()-1, len(progress))
}

// TestKindManagerFewerWorkers checks that lowering the number of workers
// lets the workers over it finish their parts and take no more
func TestKindManagerFewerWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		dst := newTestTarget()
		dst.delay = time.Second
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(8), importer.WithStagger(0))
		errc := make(chan error)
		go func() { errc <- m.Run(t.Context()) }()
		time.Sleep(3 * time.Second)
		require.Greater(t, dst.maxWriting.Load(), int64(1))

		require.NoError(t, m.SetWorkers(1))
		// The workers over 1 finish the part they write: a few writes of
		// a second each, one per partial stop
		time.Sleep(10 * time.Second)
		dst.maxWriting.Store(dst.writing.Load())
		require.NoError(t, <-errc)
		require.EqualValues(t, 1, dst.maxWriting.Load())
		dst.requireHas(t, src.entries)
	})
}

// TestKindManagerWorkersBeforeRun checks that SetWorkers refuses a kind
// that is not running
func TestKindManagerWorkersBeforeRun(t *testing.T) {
	m := importer.NewKindManager(testKind, &testSource{}, newTestTarget())
	require.ErrorContains(t, m.SetWorkers(2), "not running")
}

// TestKindManagerCancelled checks that a cancel stops a kind while a part
// waits to retry and while parts are written
func TestKindManagerCancelled(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  *testSource
		dst  *testTarget
	}{
		{"waiting to retry", &testSource{entries: testEntries(1000), fail: func([]byte) error { return errors.New("busy") }}, newTestTarget()},
		{"writing", &testSource{entries: testEntries(1000)}, &testTarget{entries: make(map[string]string), delay: time.Hour}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := importer.NewKindManager(testKind, tc.src, tc.dst, importer.WithWorkers(2))
				ctx, cancel := context.WithCancel(t.Context())
				errc := make(chan error)
				go func() { errc <- m.Run(ctx) }()
				time.Sleep(10 * time.Second)
				cancel()
				require.ErrorIs(t, <-errc, context.Canceled)
			})
		})
	}
}

// TestKindManagerBadEnds checks that a source that ends a closed part
// elsewhere than its end, or an open part at its start, stops the kind
func TestKindManagerBadEnds(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(start, end []byte) []byte
		msg  string
	}{
		{"closed part elsewhere", func(start, end []byte) []byte {
			if end != nil {
				return append(slices.Clone(end), 0)
			}
			return testOpenEnd(start)
		}, "not at its end"},
		{"open part at its start", func(start, end []byte) []byte { return start }, "at its start"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entries := testEntries(1000)
				src := &testSource{entries: entries, end: tc.end}
				var cut atomic.Bool
				// A cut-off part is retried closed
				src.fail = func(key []byte) error {
					if bytes.Equal(key, entries[10].Key) && cut.CompareAndSwap(false, true) {
						return errors.New("cut off")
					}
					return nil
				}
				m := importer.NewKindManager(testKind, src, newTestTarget(), importer.WithWorkers(2))
				err := m.Run(t.Context())
				require.ErrorContains(t, err, tc.msg)
				require.True(t, importer.IsPermanent(err))
			})
		})
	}
}

// testOpenEnd is the end of an open part from start in TestKindManagerBadEnds:
// 100 entries on
func testOpenEnd(start []byte) []byte {
	var i int
	if start != nil {
		_, _ = fmt.Sscanf(string(start), "k%06d", &i)
	}
	return fmt.Appendf(nil, "k%06d", i+100)
}

// TestKindManagerQuitWhileStaggered checks that workers told to quit
// before their staggered start never take a part
func TestKindManagerQuitWhileStaggered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		dst := newTestTarget()
		dst.delay = time.Second
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(3), importer.WithStagger(time.Hour))
		errc := make(chan error)
		go func() { errc <- m.Run(t.Context()) }()
		synctest.Wait()
		require.NoError(t, m.SetWorkers(1))
		require.NoError(t, <-errc)
		require.EqualValues(t, 1, dst.maxWriting.Load(), "only the first worker wrote")
		dst.requireHas(t, src.entries)
	})
}

// TestKindManagerShrinkAndGrow checks that workers told to exit go on with
// their parts when the number grows back before they finish, rather than
// new workers starting beside them
func TestKindManagerShrinkAndGrow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		dst := newTestTarget()
		dst.delay = time.Second
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(4), importer.WithStagger(0))
		errc := make(chan error)
		go func() { errc <- m.Run(t.Context()) }()
		time.Sleep(1500 * time.Millisecond)
		for range 3 {
			require.NoError(t, m.SetWorkers(1))
			require.NoError(t, m.SetWorkers(4))
		}
		require.NoError(t, <-errc)
		require.EqualValues(t, 4, dst.maxWriting.Load(), "more than 4 workers wrote at once")
		dst.requireHas(t, src.entries)
	})
}

// TestKindManagerPauseWakesIdleWorkers checks that pausing the kind makes
// the idle workers exit at once, so none takes the start that a busy
// worker enqueues after, and that the workers start again when the number
// grows
func TestKindManagerPauseWakesIdleWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000), openDelay: time.Second}
		dst := newTestTarget()
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(3), importer.WithStagger(0))
		errc := make(chan error)
		go func() { errc <- m.Run(t.Context()) }()
		// One worker opens the first part; the other two wait for a start
		time.Sleep(500 * time.Millisecond)
		require.NoError(t, m.SetWorkers(0))
		time.Sleep(time.Minute)
		require.EqualValues(t, 1, src.openParts.Load(), "a part was taken after the pause")

		require.NoError(t, m.SetWorkers(2))
		require.NoError(t, <-errc)
		dst.requireHas(t, src.entries)
	})
}

// TestKindManagerReusedSlots checks that workers started again after a
// pause count as running, so setting the same number again starts no more
func TestKindManagerReusedSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		dst := newTestTarget()
		dst.delay = time.Second
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(2), importer.WithStagger(0))
		errc := make(chan error)
		go func() { errc <- m.Run(t.Context()) }()
		time.Sleep(1500 * time.Millisecond)
		require.NoError(t, m.SetWorkers(0))
		// The workers finish their parts and exit
		time.Sleep(time.Minute)
		require.Zero(t, dst.writing.Load())

		require.NoError(t, m.SetWorkers(2))
		time.Sleep(1500 * time.Millisecond)
		require.NoError(t, m.SetWorkers(2))
		require.NoError(t, <-errc)
		require.EqualValues(t, 2, dst.maxWriting.Load(), "more than 2 workers wrote at once")
		dst.requireHas(t, src.entries)
	})
}

func TestKindManagerStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &testSource{entries: testEntries(1000)}
		dst := newTestTarget()
		dst.delay = time.Second
		m := importer.NewKindManager(testKind, src, dst, importer.WithWorkers(3), importer.WithStagger(0))
		errc := make(chan error)
		go func() { errc <- m.Run(t.Context()) }()
		time.Sleep(500 * time.Millisecond)
		st := m.Stats()
		require.Equal(t, importer.KindStats{Kind: testKind, Workers: 3, WorkersRunning: 3, PartsInFlight: 3}, st)

		// Workers over a lower number run until they finish their parts
		require.NoError(t, m.SetWorkers(1))
		st = m.Stats()
		require.Equal(t, 1, st.Workers)
		require.Equal(t, 3, st.WorkersRunning)

		require.NoError(t, <-errc)
		var size int64
		for _, e := range src.entries {
			size += int64(len(e.Key) + len(e.Value))
		}
		st = m.Stats()
		require.Zero(t, st.PartsInFlight)
		require.EqualValues(t, len(src.entries), st.Entries)
		require.Equal(t, size, st.Bytes)
		require.Equal(t, src.openParts.Load(), st.PartsWritten)
		require.Zero(t, st.Retries)
	})
}

// testFlakySource is a Source of one open part of n entries, whose first
// attempt fails after half of them
type testFlakySource struct {
	n     int
	tries int
}

func (s *testFlakySource) OpenStream(context.Context, importer.Kind, []byte, []byte, uint64) ([]byte, iter.Seq2[importer.Entry, error], error) {
	s.tries++
	try := s.tries
	return nil, func(yield func(importer.Entry, error) bool) {
		for i, e := range testEntries(s.n) {
			if try == 1 && i == s.n/2 {
				yield(importer.Entry{}, errors.New("cut off"))
				return
			}
			if !yield(e, nil) {
				return
			}
		}
	}, nil
}

// TestKindManagerStatsRetries checks that retries are counted, and that the
// entries count every entry stored, those written again too, and not the
// error that cut a part off
func TestKindManagerStatsRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dst := newTestTarget()
		m := importer.NewKindManager(testKind, &testFlakySource{n: 20}, dst, importer.WithWorkers(1))
		require.NoError(t, m.Run(t.Context()))
		st := m.Stats()
		require.EqualValues(t, 1, st.Retries)
		require.EqualValues(t, 30, dst.stored.Load())
		require.EqualValues(t, 30, st.Entries)
		require.EqualValues(t, 1, st.PartsWritten)
	})
}
