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

	// scanCancel is the current scan's context cancel, or nil when no scan is
	// running. A new WithCancelCause context is created for each scan so a
	// previous cancel cannot affect the next one. The first CancelCause wins;
	// later calls do not replace the cause.
	scanCancel atomic.Pointer[context.CancelCauseFunc]
	trigger    chan struct{}
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
// DoneAt is written in the same batch as the completed-scan header, so a scan
// with DoneAt set is finished and is left as-is.
func (r *meteringRunner) run(ctx context.Context) {
	if progress, err := r.loadMultihashScanProgress(r.db); err != nil {
		log.Errorw("metering: failed to load progress on startup", "err", err)
	} else if r.scanNeedsResume(progress) {
		if err := r.runMultihashScan(ctx, progress); err != nil {
			logScanErr("metering: resumed scan failed", err)
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
			if err := r.runMultihashScan(ctx, nil); err != nil {
				logScanErr("metering: triggered scan failed", err)
			}
			if timer != nil {
				timer.Reset(r.cfg.Interval)
			}
		case <-tick:
			if err := r.runMultihashScan(ctx, nil); err != nil {
				logScanErr("metering: scheduled scan failed", err)
			}
			timer.Reset(r.cfg.Interval)
		}
	}
}

// scanNeedsResume reports whether startup should continue this progress record.
// A failed scan is not resumed. DoneAt is stored with the completed header, so
// a scan with DoneAt set is already finished.
func (r *meteringRunner) scanNeedsResume(progress *multihashScanProgressRecord) bool {
	return progress != nil && progress.Error == "" && progress.DoneAt.IsZero()
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

// logScanErr logs a scan failure unless the scan was cancelled by shutdown or
// by MeteringCancelScan.
func logScanErr(msg string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	if errors.Is(err, indexer.ErrScanCancelled) {
		log.Infow("metering: scan cancelled", "err", err)
		return
	}
	log.Errorw(msg, "err", err)
}

// runMultihashScan counts multihash keys to completion.
// A nil progress starts a new scan; otherwise the passed progress is resumed.
// scanCancel stays set until this returns.
func (r *meteringRunner) runMultihashScan(
	ctx context.Context,
	progress *multihashScanProgressRecord,
) (
	err error,
) {
	scanCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	if !r.scanCancel.CompareAndSwap(nil, &cancel) {
		return nil
	}
	defer r.scanCancel.Store(nil)

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

		stats.Record(context.Background(),
			metrics.MeteringScansStarted.M(1),
			metrics.MeteringScanStartedAt.M(float64(progress.StartedAt.Unix())),
		)
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

	// providerIDByHash remembers value-record lookups for this scan. A value key maps
	// to the provider ID in that record. A provider hash maps to an ID read
	// from any remaining record for that provider. An empty ID means the
	// lookup ran and the record is gone.
	providerIDByHash := make(map[string]peer.ID)

	for {
		if err = context.Cause(scanCtx); err != nil {
			return err
		}

		batchStart := time.Now()

		if done, err := r.runMultihashScanBatch(scanCtx, progress, counters, providerIDByHash); err != nil {
			return err
		} else if done {
			break
		}

		if pause := batchSleep(time.Since(batchStart), r.cfg.TimeFill); pause > 0 {
			select {
			case <-scanCtx.Done():
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
	providerIDByHash map[string]peer.ID,
) (
	done bool,
	err error,
) {
	if !progress.DoneAt.IsZero() {
		return true, nil
	}
	batchStart := time.Now()
	stats.Record(context.Background(),
		metrics.MeteringBatchesStarted.M(1),
		metrics.MeteringBatchStartedAt.M(float64(batchStart.Unix())),
	)

	lower, upper := multihashScanBounds(progress.Cursor)

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()

	// An uncommitted batch holds only its own byte buffer. Merge copies each
	// key as it is called, so the iterator buffer can be reused on Next.
	b := r.db.NewBatch()
	defer b.Close()

	// Provider hashes whose counters changed in this batch.
	// Only those rows are written later.
	modifiedProviderContext := make(map[string]struct{})

	keysRead := 0
	var bytesRead uint64
	var lastKey []byte
	valid := iter.First()
	for valid && keysRead < r.cfg.BatchSize {
		if err := context.Cause(ctx); err != nil {
			return false, err
		}

		key := iter.Key()
		value, err := iter.ValueAndErr()
		if err != nil {
			return false, err
		}
		bytesRead += uint64(len(key) + len(value))
		if err := r.countMultihashScanRecord(
			counters,
			key, value,
			modifiedProviderContext,
			providerIDByHash,
			b,
		); err != nil {
			return false, err
		}
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
	// Progress is accepted into the memtable without waiting for a WAL fsync.
	// A crash can drop the latest unfinished batches; resume then continues
	// from the last durable cursor.
	if err := b.Commit(pebble.NoSync); err != nil {
		return false, err
	}

	stats.Record(context.Background(),
		metrics.MeteringBatchesFinished.M(1),
		metrics.MeteringBatchDurationMs.M(float64(time.Since(batchStart).Milliseconds())),
		metrics.MeteringScanKeysRead.M(int64(progress.KeysRead)),
		metrics.MeteringScanBytesRead.M(int64(progress.BytesRead)),
		metrics.MeteringTotalDeleted.M(int64(counters.totals.Deleted.Entries)),
		metrics.MeteringDeletedSlots.M(int64(counters.totals.Deleted.Slots)),
		metrics.MeteringDeletedKeyBytes.M(int64(counters.totals.Deleted.KeyBytes)),
		metrics.MeteringDeletedValueBytes.M(int64(counters.totals.Deleted.ValueBytes)),
	)

	// DoneAt stays unset here. The finish batch writes it together with the
	// completed-scan header, so a crash cannot observe one without the other.
	return exhausted, nil
}

// countMultihashScanRecord adds one multihash key to the scan counters:
// one multihash, its key and value sizes, and one slot for each remaining
// value record.
//
// A provider's multihash count increases once per key, even when that key
// has several of its contexts. Slots whose value record is gone are deleted
// contexts when the provider remains, or part of a deleted entry when no
// provider on the key remains.
//
// With Cleanup set, a slot whose value record is gone is merge-deleted into
// the batch and is not counted. The value-key buffer is switched to the delete
// prefix for that merge and restored before this call returns.
func (r *meteringRunner) countMultihashScanRecord(
	counters *multihashScanCounters,
	key, value []byte,
	modifiedProviderContext map[string]struct{},
	providerIDByHash map[string]peer.ID,
	cleanupBatch *pebble.Batch,
) error {
	if len(key) == 0 || keyPrefix(key[0]) != multihashKeyPrefix {
		return nil
	}

	vks, err := r.vcodec.unmarshalValueKeys(value)
	if err != nil {
		counters.totals.Invalid.Entries++
		counters.totals.Invalid.KeyBytes += uint64(len(key))
		counters.totals.Invalid.ValueBytes += uint64(len(value))
		log.Warnw("metering: skip undecodable multihash value", "err", err)
		return nil
	}
	defer vks.Close()

	liveSlots := 0
	providersInMultihash := make(map[string]struct{}, len(vks.keys))
	for _, valueKey := range vks.keys {
		providerHash := providerHashFromValueKey(valueKey.buf)
		if providerHash == nil {
			// Invalid value key?
			continue
		}

		// Check if the providerID can be detected from provider+context hash
		providerID, err := r.providerIDForValueKey(providerIDByHash, valueKey.buf)
		if err != nil {
			return err
		}
		deletedContext := providerID == ""

		if r.cfg.Cleanup && deletedContext {
			if err = mergeDeleteSlot(cleanupBatch, key, valueKey.buf); err != nil {
				return err
			}
			continue
		}

		if deletedContext {
			// Try to detect the providerID from the provider hash itself
			providerID = r.providerIDForHash(providerIDByHash, providerHash)
		}
		deletedProvider := providerID == ""

		if deletedProvider {
			continue
		}

		providerKey, provider := counters.providerStats(providerHash)
		if provider.ProviderID == "" {
			provider.ProviderID = providerID
		}

		if deletedContext {
			provider.DeletedContexts++
			modifiedProviderContext[providerKey] = struct{}{}
			continue
		}

		liveSlots++
		provider.Slots++
		modifiedProviderContext[providerKey] = struct{}{}

		if _, notThereYet := providersInMultihash[providerKey]; !notThereYet {
			providersInMultihash[providerKey] = struct{}{}
			provider.Multihashes++
		}
	}

	if r.cfg.Cleanup {
		// A key whose slots were all removed is gone. Count only what remains.
		if liveSlots == 0 {
			return nil
		}

		counters.totals.Active.Entries++
		counters.totals.Active.KeyBytes += uint64(len(key))
		counters.totals.Active.ValueBytes += uint64(liveSlots * marshalledValueKeyLength)
		counters.totals.Active.Slots += uint64(liveSlots)
		return nil
	}

	bucket := &counters.totals.Active
	if liveSlots == 0 && len(vks.keys) > 0 {
		bucket = &counters.totals.Deleted
	}

	bucket.Entries++
	bucket.KeyBytes += uint64(len(key))
	bucket.ValueBytes += uint64(len(value))
	bucket.Slots += uint64(len(vks.keys))

	return nil
}

// mergeDeleteSlot records a merge-delete of valueKey from multihashKey.
// The valueKey prefix is switched for the merge and restored before return.
// The batch copies both buffers before returning.
func mergeDeleteSlot(batch *pebble.Batch, multihashKey, valueKey []byte) error {
	prefix := valueKey[0]
	valueKey[0] = byte(mergeDeleteValueKeyPrefix)
	err := batch.Merge(multihashKey, valueKey, nil)
	valueKey[0] = prefix
	return err
}

// finishMultihashScan stores this scan as the latest completed result and deletes older scan rows.
// DoneAt on the progress record is set in this same batch.
func (r *meteringRunner) finishMultihashScan(
	progress *multihashScanProgressRecord,
	counters *multihashScanCounters,
) error {
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
	// Keep the progress record after success so status can still show this
	// scan as done, with the counters it produced. The next scan replaces it.
	progress.DoneAt = completedAt
	progressBytes, err := encodeMultihashScanProgress(progress)
	if err != nil {
		return err
	}
	if err := b.Set(meteringProgressKey, progressBytes, nil); err != nil {
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
		"active", counters.totals.Active.Entries,
		"deleted", counters.totals.Deleted.Entries,
		"slots", counters.totals.Active.Slots,
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
	stats.Record(context.Background(), metrics.MeteringScansFinished.M(1))
	stats.Record(context.Background(),
		metrics.MeteringScanKeysRead.M(0),
		metrics.MeteringScanBytesRead.M(0),
	)
	return nil
}

// loadMultihashScanProgress returns the latest scan's progress record, or nil when none is stored.
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
	if r.scanCancel.Load() != nil {
		return indexer.ErrScanInProgress
	}
	select {
	case r.trigger <- struct{}{}:
	default:
	}
	return nil
}

// cancelScan cancels the in-progress scan with ScanCancelledError as the
// context cause. A second cancel does not replace the cause.
func (r *meteringRunner) cancelScan(reason string) error {
	p := r.scanCancel.Load()
	if p == nil {
		return indexer.ErrScanNotInProgress
	}
	(*p)(indexer.ScanCancelledError(reason))
	return nil
}

// persistScanError records scanErr on progress so status reports can show why
// the scan stopped. A shutdown cancel has no cause, so it is context.Canceled
// and is not stored; the unfinished scan resumes on the next start. A user
// cancel carries ErrScanCancelled as the cause and is stored. An already-
// recorded Error is left as-is.
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
func (r *meteringRunner) loadAllStats(
	ctx context.Context,
	providerIDs []peer.ID,
) (
	*indexer.AllStatsReport,
	error,
) {
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
func (r *meteringRunner) loadProviderStats(
	rd pebble.Reader,
	scanID uint64,
	providerIDs []peer.ID,
) (
	[]indexer.ProviderStats,
	error,
) {
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
func (r *meteringRunner) loadMultihashScanCounters(
	rd pebble.Reader,
	scanID uint64,
) (
	*multihashScanCounters,
	error,
) {
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

// loadScanStatus reports the latest multihash scan and the counters it has committed.
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
		st.State = indexer.ScanStateNone
		return st, nil
	}
	st.ScanID = p.ScanID
	st.StartedAt = p.StartedAt
	st.Error = p.Error
	switch {
	case st.Error != "":
		st.State = indexer.ScanStateError
	case !p.DoneAt.IsZero():
		st.State = indexer.ScanStateDone
	default:
		st.State = indexer.ScanStateInProgress
	}

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

	switch st.State {
	case indexer.ScanStateInProgress:
		st.EstimatedPercentDone, st.EstimatedFinish = scanEstimate(p.StartedAt, st.CursorKey, time.Now())

	case indexer.ScanStateDone:
		st.EstimatedPercentDone, st.EstimatedFinish = 100, &p.DoneAt
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
		metrics.MeteringTotalMultihashes.M(int64(report.Totals.Active.Entries)),
		metrics.MeteringTotalSlots.M(int64(report.Totals.Active.Slots)),
		metrics.MeteringTotalDeleted.M(int64(report.Totals.Deleted.Entries)),
		metrics.MeteringDeletedSlots.M(int64(report.Totals.Deleted.Slots)),
		metrics.MeteringDeletedKeyBytes.M(int64(report.Totals.Deleted.KeyBytes)),
		metrics.MeteringDeletedValueBytes.M(int64(report.Totals.Deleted.ValueBytes)),
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

// multihashScanCounters is the in-memory copy of one scan's committed
// counters. totals is written to the scan-id totals key at the end of each
// batch and copied into multihashScanResults at the finish. providers is
// keyed by provider hash; only rows touched by the current batch are written
// to the scan-id provider keys. A resume loads both back from those keys.
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

// providerIDForHash returns the peer ID for a provider hash, reading one value
// record the first time this scan sees that hash. providerIDByHash is the
// scan-wide lookup cache. An empty ID means that lookup already ran and no
// record remains. Value records hold the peer ID written when the advertisement
// was indexed.
func (r *meteringRunner) providerIDForHash(providerIDByHash map[string]peer.ID, providerHash []byte) peer.ID {
	key := string(providerHash)
	if id, ok := providerIDByHash[key]; ok {
		return id
	}
	id := r.readProviderID(providerHash)
	providerIDByHash[key] = id
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

// providerIDForValueKey returns the provider ID stored in the value record for
// valueKey. providerIDByHash remembers the lookup. An empty ID means the record
// is absent. A present record also records that ID under the provider hash, so
// a later missing slot can tell that the provider still has a value record.
func (r *meteringRunner) providerIDForValueKey(providerIDByHash map[string]peer.ID, valueKey []byte) (peer.ID, error) {
	key := string(valueKey)
	if providerID, ok := providerIDByHash[key]; ok {
		return providerID, nil
	}
	raw, closer, err := r.db.Get(valueKey)
	if errors.Is(err, pebble.ErrNotFound) {
		providerIDByHash[key] = ""
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer closer.Close()
	v, err := r.vcodec.unmarshalValue(slices.Clone(raw))
	if err != nil {
		return "", err
	}
	providerID := peer.ID(slices.Clone([]byte(v.ProviderID)))
	providerIDByHash[key] = providerID
	if providerID != "" {
		if providerHash := providerHashFromValueKey(valueKey); providerHash != nil {
			if _, ok := providerIDByHash[string(providerHash)]; !ok {
				providerIDByHash[string(providerHash)] = providerID
			}
		}
	}
	return providerID, nil
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
