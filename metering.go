package indexer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	// ErrMeteringNotSupported signals that an indexer.Interface does not support
	// storage metering.
	ErrMeteringNotSupported = errors.New("metering is not supported by store")
	// ErrScanInProgress is returned by MeteringTriggerScan when a scan is already running.
	ErrScanInProgress = errors.New("metering scan already in progress")
	// ErrScanNotInProgress is returned by MeteringCancelScan when no scan is running.
	ErrScanNotInProgress = errors.New("metering scan is not in progress")
	// ErrScanCancelled is recorded on scan status when MeteringCancelScan stops a scan.
	// A non-empty reason is appended after this text.
	ErrScanCancelled = errors.New("user cancelled")
)

// StatsMeter is embedded in Interface. Implementations that cannot scan
// return ErrMeteringNotSupported from each method.
//
// providerIDs selects which providers are included in a result.
// nil includes every provider. An empty slice includes none.
// A non-empty slice includes only those providers, omitting any with no
// recorded stats.
type StatsMeter interface {
	// MeteringAllStats returns the latest completed scan, or nil if no scan has
	// completed yet. Totals are always for the whole store. Providers follows
	// the providerIDs selection.
	MeteringAllStats(ctx context.Context, providerIDs []peer.ID) (*AllStatsReport, error)
	// MeteringScanStatus returns the in-progress scan, or an idle status when no scan
	// is running. Current provider rows follow the providerIDs selection.
	// Totals stay whole-store.
	MeteringScanStatus(ctx context.Context, providerIDs []peer.ID) (*ScanStatus, error)
	// MeteringTriggerScan signals that a scan should start now. ErrScanInProgress means
	// a scan is already running. Progress is available from MeteringScanStatus while
	// the scan runs, and from MeteringAllStats after it completes.
	MeteringTriggerScan(ctx context.Context) error
	// MeteringCancelScan asks the in-progress scan to stop. reason is recorded
	// after ErrScanCancelled on MeteringScanStatus. An empty reason records
	// ErrScanCancelled alone. The caller formats reason.
	// ErrScanNotInProgress means no scan is running. A second cancel while the
	// same scan is still stopping is a no-op.
	MeteringCancelScan(ctx context.Context, reason string) error
}

// ScanCancelledError is ErrScanCancelled, optionally annotated with reason.
// The result is always errors.Is(..., ErrScanCancelled).
func ScanCancelledError(reason string) error {
	if reason == "" {
		return ErrScanCancelled
	}
	return fmt.Errorf("%w: %s", ErrScanCancelled, reason)
}

// CompletedScanStats is a completed metering scan: when it was measured and the
// whole-store totals.
type CompletedScanStats struct {
	MeasuredAt time.Time
	Totals     StoreTotals
}

// AllStatsReport is a completed metering scan plus the selected provider rows.
type AllStatsReport struct {
	CompletedScanStats
	Providers []ProviderStats
}

// StatsSnapshot is a point-in-time view of whole-store totals and the
// selected providers.
type StatsSnapshot struct {
	Totals    StoreTotals
	Providers []ProviderStats
}

// StoreTotals holds whole-store counters from a metering scan.
type StoreTotals struct {
	// Multihashes is the number of distinct multihash keys.
	Multihashes uint64
	// Slots is the number of value-key entries packed inside multihash records.
	// One multihash mapped to two contexts counts as two slots. Slots whose
	// value record was deleted are still counted.
	Slots uint64
	// MultihashKeyBytes is the total size of multihash keys.
	MultihashKeyBytes uint64
	// MultihashValueBytes is the total size of the packed value-key lists
	// stored under multihash keys.
	MultihashValueBytes uint64
}

// ProviderStats holds per-provider counters from a metering scan.
type ProviderStats struct {
	// ProviderID is the provider these counters belong to.
	ProviderID peer.ID
	// Multihashes is the number of distinct multihashes that have at least one
	// slot for this provider.
	Multihashes uint64
}

// ScanStatus describes the metering scan goroutine.
type ScanStatus struct {
	InProgress bool
	ScanID     uint64
	StartedAt  time.Time
	KeysRead   uint64
	BytesRead  uint64
	// CursorKey is the latest key the scan has finished. JSON encodes it as
	// base64. Empty until the first batch is committed.
	CursorKey []byte
	// EstimatedPercentDone is the estimated progress of the scan, from 0 to 100.
	EstimatedPercentDone float64
	// EstimatedFinish is when the scan is expected to complete. Nil when the
	// estimate is not available.
	EstimatedFinish *time.Time
	// Current is the statistics committed by finished batches of this scan.
	// Totals cover the whole store. Providers follows the requested selection.
	Current StatsSnapshot
	// Error is set when the last scan stopped with a failure. Empty while a
	// scan is running or after a successful completion.
	Error string `json:",omitempty"`
}
