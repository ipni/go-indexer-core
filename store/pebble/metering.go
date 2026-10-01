package pebble

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// meteringRunner walks the pebble store in the background and keeps per-provider counters.
type meteringRunner struct {
	db     *pebble.DB
	vcodec *codec
	cfg    MeteringConfig
	cancel context.CancelFunc
	wg     sync.WaitGroup

	scanning atomic.Bool
	trigger  chan struct{}
}

// startMetering launches the scanner goroutine.
//
// The store must call stop before closing the database.
func (s *store) startMetering(cfg MeteringConfig) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &meteringRunner{
		db:      s.db,
		vcodec:  s.vcodec,
		cfg:     cfg,
		cancel:  cancel,
		trigger: make(chan struct{}, 1),
	}
	s.metering = r
	r.wg.Go(func() { r.run(ctx) })
}

// stop cancels the scanner and waits for its goroutine to finish.
func (r *meteringRunner) stop() {
	r.cancel()
	r.wg.Wait()
}

// run resumes an unfinished scan, then starts a new one when the interval
// elapses or a manual trigger arrives. A manual trigger restarts the interval.
func (r *meteringRunner) run(ctx context.Context) {
	if progress, err := r.loadMultihashScanProgress(r.db); err != nil {
		log.Errorw("metering: failed to load progress on startup", "err", err)
	} else if progress != nil && progress.Error == "" {
		if err := r.runMultihashScan(ctx, progress); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorw("metering: resumed scan failed", "err", err)
		}
	} else if cur, err := r.loadCompletedScan(r.db); err != nil {
		log.Errorw("metering: failed to load completed scan on startup", "err", err)
	} else if cur != nil {
		providers, err := r.loadProviderStats(r.db, cur.ScanID, nil)
		if err != nil {
			log.Errorw("metering: failed to load completed providers on startup", "err", err)
		} else {
			r.publishMeteringGauges(&indexer.AllStatsReport{
				CompletedScanStats: indexer.CompletedScanStats{
					MeasuredAt: cur.CompletedAt,
					Totals:     cur.Totals,
				},
				Providers: providers,
			}, cur.CompletedAt.Sub(cur.StartedAt))
		}
	}

	var tick <-chan time.Time
	var timer *time.Timer
	if r.cfg.Interval > 0 {
		timer = time.NewTimer(r.cfg.Interval)
		tick = timer.C
		defer timer.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.trigger:
			if err := r.runMultihashScan(ctx, nil); err != nil && !errors.Is(err, context.Canceled) {
				log.Errorw("metering: triggered scan failed", "err", err)
			}
			if timer != nil {
				timer.Reset(r.cfg.Interval)
			}
		case <-tick:
			if err := r.runMultihashScan(ctx, nil); err != nil && !errors.Is(err, context.Canceled) {
				log.Errorw("metering: scheduled scan failed", "err", err)
			}
			timer.Reset(r.cfg.Interval)
		}
	}
}

// createMultihashScanProgress persists a new scan with empty totals and returns its progress record.
func (r *meteringRunner) createMultihashScanProgress() (*multihashScanProgressRecord, error) {
	now := time.Now().UTC()
	progress := &multihashScanProgressRecord{
		ScanID:    uint64(now.UnixMicro()),
		StartedAt: now,
	}
	b := r.db.NewBatch()
	defer b.Close()
	progressBytes, err := encodeMultihashScanProgress(progress)
	if err != nil {
		return nil, err
	}
	if err := b.Set(meteringProgressKey, progressBytes, nil); err != nil {
		return nil, err
	}
	totalsBytes, err := encodeStoreTotals(&indexer.StoreTotals{})
	if err != nil {
		return nil, err
	}
	if err := b.Set(meteringTotalsKey(progress.ScanID), totalsBytes, nil); err != nil {
		return nil, err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return nil, err
	}
	return progress, nil
}

// runMultihashScan counts multihash keys to completion.
// A nil progress starts a new scan; otherwise the passed progress is resumed.
// scanning stays set until this returns.
func (r *meteringRunner) runMultihashScan(
	ctx context.Context,
	progress *multihashScanProgressRecord,
) (
	err error,
) {
	if !r.scanning.CompareAndSwap(false, true) {
		return nil
	}
	defer r.scanning.Store(false)

	// Persist failures against this progress record instead of reloading it.
	defer func() { r.persistScanError(progress, err) }()

	var counters *multihashScanCounters

	if progress == nil {
		progress, err = r.createMultihashScanProgress()
		if err != nil {
			return err
		}

		counters = &multihashScanCounters{
			providers: make(map[string]*indexer.ProviderStats),
		}

		log.Infow("metering: running scan", "scanID", progress.ScanID)
	} else {
		counters, err = r.loadMultihashScanCounters(r.db, progress.ScanID)
		if err != nil {
			return err
		}

		log.Infow("metering: resuming scan",
			"scanID", progress.ScanID,
			"keysRead", progress.KeysRead,
			"startedAt", progress.StartedAt,
		)
	}

	providerIDs := make(map[string]peer.ID)

	for {
		if err = ctx.Err(); err != nil {
			return err
		}

		batchStart := time.Now()

		if done, err := r.runMultihashScanBatch(ctx, progress, counters, providerIDs); err != nil {
			return err
		} else if done {
			break
		}

		if pause := batchSleep(time.Since(batchStart), r.cfg.TimeFill); pause > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(pause):
			}
		}
	}

	return r.finishMultihashScan(progress, counters)
}

// maxBatchSleep is the longest pause between metering scan batches.
const maxBatchSleep = time.Hour

// minBatchElapsed is used when a batch finishes faster than the clock can
// measure. Without it, TimeFill would sleep nothing after a near-zero batch.
const minBatchElapsed = time.Millisecond

// batchSleep is the wait that makes a batch of duration elapsed occupy fill of
// the time until the next batch. fill is work/(work+sleep). The wait is at most maxBatchSleep.
func batchSleep(elapsed time.Duration, fill float64) time.Duration {
	if fill <= 0 || fill >= 1 {
		return 0
	}
	if elapsed < minBatchElapsed {
		elapsed = minBatchElapsed
	}
	sleep := float64(elapsed) * (1 - fill) / fill
	if sleep > float64(maxBatchSleep) {
		return maxBatchSleep
	}
	return time.Duration(sleep)
}

// runMultihashScanBatch reads one batch of multihash keys, adds them to
// counters, and commits the cursor together with the updated totals.
func (r *meteringRunner) runMultihashScanBatch(
	ctx context.Context,
	progress *multihashScanProgressRecord,
	counters *multihashScanCounters,
	providerIDs map[string]peer.ID,
) (
	done bool,
	err error,
) {
	if progress.Done {
		return true, nil
	}
	lower, upper := multihashScanBounds(progress.Cursor)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()

	// Provider hashes whose counters changed in this batch.
	// Only those rows are written later.
	modifiedProviderContext := make(map[string]struct{})

	keysRead := 0
	var bytesRead uint64
	var lastKey []byte
	valid := iter.First()
	for valid && keysRead < r.cfg.BatchSize {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		key := iter.Key()
		value, err := iter.ValueAndErr()
		if err != nil {
			return false, err
		}
		bytesRead += uint64(len(key) + len(value))
		r.countMultihashScanRecord(counters, key, value, modifiedProviderContext, providerIDs)
		lastKey = append(lastKey[:0], key...)

		keysRead++
		valid = iter.Next()
	}
	exhausted := !valid

	progress.KeysRead += uint64(keysRead)
	progress.BytesRead += bytesRead
	if keysRead > 0 {
		progress.Cursor = slices.Clone(lastKey)
	}
	if exhausted {
		progress.Done = true
	}

	b := r.db.NewBatch()
	defer b.Close()
	if err := counters.writeModifiedCounters(b, progress.ScanID, modifiedProviderContext); err != nil {
		return false, err
	}
	progressBytes, err := encodeMultihashScanProgress(progress)
	if err != nil {
		return false, err
	}
	if err := b.Set(meteringProgressKey, progressBytes, nil); err != nil {
		return false, err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return false, err
	}

	stats.Record(context.Background(),
		metrics.MeteringScanKeysRead.M(int64(progress.KeysRead)),
		metrics.MeteringScanBytesRead.M(int64(progress.BytesRead)),
	)

	return progress.Done, nil
}

// countMultihashScanRecord adds one multihash key to the scan counters.
func (r *meteringRunner) countMultihashScanRecord(
	counters *multihashScanCounters,
	key, value []byte,
	modifiedProviderContext map[string]struct{},
	providerIDs map[string]peer.ID,
) {
	if len(key) == 0 || keyPrefix(key[0]) != multihashKeyPrefix {
		return
	}

	vks, err := r.vcodec.unmarshalValueKeys(value)
	if err != nil {
		log.Warnw("metering: skip undecodable multihash value", "err", err)
		return
	}
	defer vks.Close()

	r.countMultihashScanKey(counters, key, value, vks, modifiedProviderContext, providerIDs)
}

// finishMultihashScan stores this scan as the latest completed result and deletes older scan rows.
func (r *meteringRunner) finishMultihashScan(progress *multihashScanProgressRecord, counters *multihashScanCounters) error {
	completedAt := time.Now().UTC()

	b := r.db.NewBatch()
	defer b.Close()
	curBytes, err := encodeMultihashScanResults(&multihashScanResults{
		ScanID:      progress.ScanID,
		StartedAt:   progress.StartedAt,
		CompletedAt: completedAt,
		Totals:      counters.totals,
	})
	if err != nil {
		return err
	}
	if err := b.Set(meteringCurrentKey, curBytes, nil); err != nil {
		return err
	}
	if err := b.Delete(meteringProgressKey, nil); err != nil {
		return err
	}
	scanPrefix, nextScanPrefix := meteringScanIDRange(progress.ScanID)
	if err := b.DeleteRange(meteringAllScansStart, scanPrefix, nil); err != nil {
		return err
	}
	if err := b.DeleteRange(nextScanPrefix, meteringAllScansEnd, nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}

	log.Infow("metering: scan completed",
		"scanID", progress.ScanID,
		"keysRead", progress.KeysRead,
		"bytesRead", progress.BytesRead,
		"multihashes", counters.totals.Multihashes,
		"slots", counters.totals.Slots,
		"duration", completedAt.Sub(progress.StartedAt),
	)

	providers := make([]indexer.ProviderStats, 0, len(counters.providers))
	for _, ps := range counters.providers {
		if ps.ProviderID == "" {
			continue
		}
		providers = append(providers, *ps)
	}
	// Counters are already in memory. Publishing them here avoids another
	// database read while the scan is still marked running.
	r.publishMeteringGauges(
		&indexer.AllStatsReport{
			CompletedScanStats: indexer.CompletedScanStats{
				MeasuredAt: completedAt,
				Totals:     counters.totals,
			},
			Providers: providers,
		},
		completedAt.Sub(progress.StartedAt),
	)
	stats.Record(context.Background(),
		metrics.MeteringScanKeysRead.M(0),
		metrics.MeteringScanBytesRead.M(0),
	)
	return nil
}

// loadMultihashScanProgress returns the unfinished scan, or nil when none is stored.
func (r *meteringRunner) loadMultihashScanProgress(rd pebble.Reader) (*multihashScanProgressRecord, error) {
	b, closer, err := rd.Get(meteringProgressKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return decodeMultihashScanProgress(b)
}

// loadCompletedScan returns the latest completed scan, or nil when no scan has completed.
func (r *meteringRunner) loadCompletedScan(rd pebble.Reader) (*multihashScanResults, error) {
	b, closer, err := rd.Get(meteringCurrentKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return decodeMultihashScanResults(b)
}

// triggerScan signals the runner to start a scan when none is running.
func (r *meteringRunner) triggerScan() error {
	if r.scanning.Load() {
		return indexer.ErrScanInProgress
	}
	select {
	case r.trigger <- struct{}{}:
	default:
	}
	return nil
}

// persistScanError records scanErr on progress so status reports can show why
// the scan stopped. Cancellation and an already-recorded Error are ignored.
func (r *meteringRunner) persistScanError(progress *multihashScanProgressRecord, scanErr error) {
	if progress == nil ||
		scanErr == nil ||
		errors.Is(scanErr, context.Canceled) ||
		progress.Error != "" {
		return
	}

	progress.Error = scanErr.Error()
	progressBytes, err := encodeMultihashScanProgress(progress)
	if err != nil {
		log.Errorw("metering: failed to encode progress error", "err", err)
		return
	}

	if err := r.db.Set(meteringProgressKey, progressBytes, pebble.Sync); err != nil {
		log.Errorw("metering: failed to persist scan error", "err", err)
	}
}

// loadAllStats returns the latest completed scan.
// providerIDs filters provider rows; totals are always for the whole store.
func (r *meteringRunner) loadAllStats(ctx context.Context, providerIDs []peer.ID) (*indexer.AllStatsReport, error) {
	_ = ctx

	// Load data from a snapshot to ensure consistent view of the database.
	snap := r.db.NewSnapshot()
	defer snap.Close()

	cur, err := r.loadCompletedScan(snap)
	if err != nil || cur == nil {
		return nil, err
	}
	providers, err := r.loadProviderStats(snap, cur.ScanID, providerIDs)
	if err != nil {
		return nil, err
	}
	return &indexer.AllStatsReport{
		CompletedScanStats: indexer.CompletedScanStats{
			MeasuredAt: cur.CompletedAt,
			Totals:     cur.Totals,
		},
		Providers: providers,
	}, nil
}

// loadProviderStats returns provider rows for scanID.
// A nil providerIDs returns every provider with a known ID. An empty slice returns none.
// Rows whose provider ID is still empty are omitted; those exist only so a scan can resume.
func (r *meteringRunner) loadProviderStats(rd pebble.Reader, scanID uint64, providerIDs []peer.ID) ([]indexer.ProviderStats, error) {
	if providerIDs != nil && len(providerIDs) == 0 {
		return []indexer.ProviderStats{}, nil
	}

	if providerIDs != nil {
		out := make([]indexer.ProviderStats, 0, len(providerIDs))
		for _, id := range providerIDs {
			val, closer, err := rd.Get(meteringProviderKey(scanID, hashProviderID(id)))
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			ps, err := decodeProviderStats(val)
			closer.Close()
			if err != nil {
				return nil, err
			}
			if ps.ProviderID == "" {
				continue
			}
			out = append(out, *ps)
		}
		return out, nil
	}

	prefix := meteringProviderPrefix(scanID)
	upper := meteringProviderPrefixEnd(scanID)
	iter, err := rd.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var out []indexer.ProviderStats
	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		providerHash := providerHashFromStatsKey(key)
		if providerHash == nil {
			continue
		}
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		ps, err := decodeProviderStats(val)
		if err != nil {
			return nil, err
		}
		if ps.ProviderID == "" {
			continue
		}
		out = append(out, *ps)
	}
	return out, nil
}

// loadScanTotals returns the store totals committed for scanID. A missing record is zero.
func (r *meteringRunner) loadScanTotals(rd pebble.Reader, scanID uint64) (*indexer.StoreTotals, error) {
	b, closer, err := rd.Get(meteringTotalsKey(scanID))
	if errors.Is(err, pebble.ErrNotFound) {
		return &indexer.StoreTotals{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	totals, err := decodeStoreTotals(b)
	if err != nil {
		return nil, err
	}
	return totals, nil
}

// loadMultihashScanCounters reads the totals and every provider row committed for scanID.
func (r *meteringRunner) loadMultihashScanCounters(rd pebble.Reader, scanID uint64) (*multihashScanCounters, error) {
	totals, err := r.loadScanTotals(rd, scanID)
	if err != nil {
		return nil, err
	}
	result := &multihashScanCounters{
		totals:    *totals,
		providers: make(map[string]*indexer.ProviderStats),
	}
	prefix := meteringProviderPrefix(scanID)
	iter, err := rd.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: meteringProviderPrefixEnd(scanID),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		providerHash := providerHashFromStatsKey(key)
		if providerHash == nil {
			continue
		}
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		ps, err := decodeProviderStats(val)
		if err != nil {
			return nil, err
		}
		result.providers[providerStatsMapKey(providerHash)] = ps
	}
	return result, nil
}

// loadScanStatus reports the unfinished multihash scan and the counters committed so far.
// providerIDs filters provider rows the same way as loadAllStats.
func (r *meteringRunner) loadScanStatus(ctx context.Context, providerIDs []peer.ID) (*indexer.ScanStatus, error) {
	_ = ctx
	snap := r.db.NewSnapshot()
	defer snap.Close()
	p, err := r.loadMultihashScanProgress(snap)
	if err != nil {
		return nil, err
	}
	st := &indexer.ScanStatus{}
	if p == nil {
		return st, nil
	}
	st.ScanID = p.ScanID
	st.StartedAt = p.StartedAt
	st.KeysRead = p.KeysRead
	st.BytesRead = p.BytesRead
	st.Error = p.Error

	// A failed scan keeps its progress row with Error set; that is not in progress.
	st.InProgress = r.scanning.Load() || (p.Error == "" && !p.Done)

	totals, err := r.loadScanTotals(snap, p.ScanID)
	if err != nil {
		return nil, err
	}

	providers, err := r.loadProviderStats(snap, p.ScanID, providerIDs)
	if err != nil {
		return nil, err
	}

	st.Current = indexer.StatsSnapshot{Totals: *totals, Providers: providers}
	if len(p.Cursor) > 0 {
		st.CursorKey = slices.Clone(p.Cursor)
	}

	if st.InProgress {
		st.EstimatedPercentDone, st.EstimatedFinish = scanEstimate(p.StartedAt, st.CursorKey, time.Now())
	}

	return st, nil
}

// scanEstimate reports estimated percent done, from 0 to 100, and a finish time.
// The finish time is nil when progress is still 0 or the start time is unset.
func scanEstimate(started time.Time, cursor []byte, now time.Time) (float64, *time.Time) {
	frac := sha256MultihashKeyFraction(cursor)
	percent := frac * 100
	if frac <= 0 || started.IsZero() || !now.After(started) {
		return percent, nil
	}
	elapsed := now.Sub(started)
	remaining := time.Duration(float64(elapsed) * (1 - frac) / frac)
	finish := now.Add(remaining)
	return percent, &finish
}

// publishMeteringGauges records whole-store gauges from a completed scan.
// Per-provider gauges are recorded only when ExportProviderMetrics is set.
func (r *meteringRunner) publishMeteringGauges(report *indexer.AllStatsReport, duration time.Duration) {
	ctx := context.Background()
	stats.Record(ctx,
		metrics.MeteringTotalMultihashes.M(int64(report.Totals.Multihashes)),
		metrics.MeteringTotalSlots.M(int64(report.Totals.Slots)),
		metrics.MeteringScanCompletedAt.M(float64(report.MeasuredAt.Unix())),
		metrics.MeteringScanDurationMs.M(float64(duration.Milliseconds())),
	)
	if !r.cfg.ExportProviderMetrics {
		return
	}
	for _, ps := range report.Providers {
		if ps.ProviderID == "" {
			continue
		}
		mctx, err := tag.New(ctx, tag.Upsert(metrics.Provider, string(ps.ProviderID)))
		if err != nil {
			continue
		}
		stats.Record(mctx,
			metrics.MeteringProviderMultihashes.M(int64(ps.Multihashes)),
		)
	}
}

// multihashScanCounters holds the totals for one multihash scan.
// Each provider row is keyed by the provider hash from the multihash slots.
type multihashScanCounters struct {
	totals    indexer.StoreTotals
	providers map[string]*indexer.ProviderStats
}

// providerStats returns the counters for a provider hash, creating an empty
// row when this scan has not seen that provider yet.
func (s *multihashScanCounters) providerStats(providerHash []byte) (string, *indexer.ProviderStats) {
	key := providerStatsMapKey(providerHash)
	stats, ok := s.providers[key]
	if !ok {
		stats = &indexer.ProviderStats{}
		s.providers[key] = stats
	}
	return key, stats
}

// cachedProviderID returns the peer ID for a provider hash, reading one value record
// the first time this scan sees that hash. An empty ID means that lookup already ran.
// Value records hold the peer ID written when the advertisement was indexed.
func cachedProviderID(providerIDs map[string]peer.ID, providerHash []byte, readProviderID func([]byte) peer.ID) peer.ID {
	key := string(providerHash)
	if id, ok := providerIDs[key]; ok {
		return id
	}
	id := readProviderID(providerHash)
	providerIDs[key] = id
	return id
}

// readProviderID reads the peer ID from the first value record of a provider hash.
func (r *meteringRunner) readProviderID(providerHash []byte) peer.ID {
	start := make([]byte, 1+len(providerHash))
	start[0] = byte(valueKeyPrefix)
	copy(start[1:], providerHash)
	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: nextProviderValueKey(providerHash),
	})
	if err != nil {
		log.Errorw("metering: provider lookup failed", "err", err)
		return ""
	}
	defer iter.Close()
	if !iter.First() {
		return ""
	}
	raw, err := iter.ValueAndErr()
	if err != nil {
		log.Errorw("metering: provider lookup failed", "err", err)
		return ""
	}
	v, err := r.vcodec.unmarshalValue(slices.Clone(raw))
	if err != nil {
		log.Errorw("metering: provider lookup failed", "err", err)
		return ""
	}
	return peer.ID(slices.Clone([]byte(v.ProviderID)))
}

// countMultihashScanKey adds one multihash key: one multihash, its key and
// value sizes, and one multihash for each distinct provider in its slots.
func (r *meteringRunner) countMultihashScanKey(counters *multihashScanCounters, key, value []byte, valueKeys *keyList, modifiedProviderContext map[string]struct{}, providerIDs map[string]peer.ID) {
	counters.totals.Multihashes++
	counters.totals.MultihashKeyBytes += uint64(len(key))
	counters.totals.MultihashValueBytes += uint64(len(value))
	counters.totals.Slots += uint64(len(valueKeys.keys))

	providersInMultihash := make(map[string]struct{}, len(valueKeys.keys))
	for _, valueKey := range valueKeys.keys {
		providerHash := providerHashFromValueKey(valueKey.buf)
		if providerHash == nil {
			continue
		}
		providerKey, provider := counters.providerStats(providerHash)
		if provider.ProviderID == "" {
			provider.ProviderID = cachedProviderID(providerIDs, providerHash, r.readProviderID)
		}
		if _, ok := providersInMultihash[providerKey]; ok {
			continue
		}
		providersInMultihash[providerKey] = struct{}{}
		provider.Multihashes++
		modifiedProviderContext[providerKey] = struct{}{}
	}
}

// writeModifiedCounters puts the current store totals and the provider rows
// listed in modifiedProviderContext into batch. Other provider rows stay as stored.
func (s *multihashScanCounters) writeModifiedCounters(batch *pebble.Batch, scanID uint64, modifiedProviderContext map[string]struct{}) error {
	totalsBytes, err := encodeStoreTotals(&s.totals)
	if err != nil {
		return err
	}
	if err := batch.Set(meteringTotalsKey(scanID), totalsBytes, nil); err != nil {
		return err
	}
	for hash, ps := range s.providers {
		if _, ok := modifiedProviderContext[hash]; !ok {
			continue
		}
		psBytes, err := encodeProviderStats(ps)
		if err != nil {
			return err
		}
		if err := batch.Set(meteringProviderKey(scanID, []byte(hash)), psBytes, nil); err != nil {
			return err
		}
	}
	return nil
}

// multihashScanBounds is the inclusive iterator window for the next batch of
// multihash keys. An empty cursor starts at the multihash prefix. Otherwise
// the lower bound is the cursor plus a trailing zero, the first key strictly
// after it. The upper bound is the next byte after the multihash prefix.
func multihashScanBounds(cursor []byte) (lower, upper []byte) {
	if len(cursor) == 0 {
		return multihashScanLower, multihashScanUpper
	}
	lower = make([]byte, 0, len(cursor)+1)
	lower = append(lower, cursor...)
	lower = append(lower, 0)
	return lower, multihashScanUpper
}
