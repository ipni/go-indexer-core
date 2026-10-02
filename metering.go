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
	// MeteringScanStatus returns the latest scan. State is none when no scan
	// has been recorded, in_progress while one is running, done after it
	// finishes, and error when it stopped with a failure. A finished or failed
	// scan keeps its counters until the next scan replaces them. Current
	// provider rows follow the providerIDs selection. Totals stay whole-store.
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

// Metering reports have two views of one scan, and both carry the same
// whole-store counters in StoreTotals.
//
// The completed view is AllStatsReport, returned by MeteringAllStats. Its
// header is CompletedScanStats: when measuring stopped, plus StoreTotals.
// Provider rows are loaded separately and filtered by the caller's selection.
//
// The process view is ScanStatus, returned by MeteringScanStatus. Fields that
// describe the walk itself — whether it is running, when it started, the
// cursor, and the computed finish estimate — live only here. Current.Totals
// is a StoreTotals for the batches committed so far, the same struct the
// finished report stores. After a successful finish the process record is
// deleted, so status goes idle while AllStatsReport keeps the result. A
// failed scan keeps the process record and sets Error.
//
// Provider rows whose ProviderID is still empty are kept so a resumed scan
// can keep counting, and are left out of both API responses.

// EntryMeters counts one class of multihash records: how many there are, how
// many bytes their keys and values occupy, and how many value-key slots they
// pack. StoreTotals uses one value for records that decoded and another for
// records that did not. The in-progress snapshot and the finished report both
// store StoreTotals, so the two views stay the same shape. Invalid records
// leave Slots at zero because their value list did not decode.
type EntryMeters struct {
	// Entries is the number of multihash keys in this class.
	Entries uint64
	// KeyBytes is the total size of those keys.
	KeyBytes uint64
	// ValueBytes is the total size of those values.
	ValueBytes uint64
	// Slots is the number of value-key entries packed inside those records.
	// One multihash mapped to two contexts counts as two slots. A slot whose
	// value record was later deleted is still counted.
	Slots uint64
}

// CompletedScanStats is the header of a finished scan: when measuring stopped
// and the whole-store counters. It does not include per-provider rows or the
// walk cursor. MeteringAllStats returns it embedded in AllStatsReport. The
// Pebble store writes it as the current-scan record when the walk finishes.
type CompletedScanStats struct {
	// MeasuredAt is when the scan finished counting.
	MeasuredAt time.Time
	// Totals is the whole-store counters. It does not change with the caller's
	// provider selection. The same struct is ScanStatus.Current.Totals while
	// the scan is still running.
	Totals StoreTotals
}

// AllStatsReport is a finished scan as returned by MeteringAllStats. The
// embedded CompletedScanStats is the whole store. Providers is the subset
// selected by the caller: nil means every provider that has a peer ID, an
// empty slice means none, and a non-empty slice means only those IDs.
// Providers with an empty ProviderID are omitted.
type AllStatsReport struct {
	CompletedScanStats
	Providers []ProviderStats
}

// StatsSnapshot is StoreTotals plus provider rows at one moment during a scan.
// ScanStatus.Current uses it for batches committed so far. The provider
// selection rules are the same as AllStatsReport.Providers. Totals stay
// whole-store and use the same StoreTotals as the finished report.
type StatsSnapshot struct {
	Totals    StoreTotals
	Providers []ProviderStats
}

// StoreTotals is the whole-store counters for one scan. It is written at the
// end of each batch, returned in ScanStatus.Current while the walk is running,
// and copied into the finished scan header when the walk completes.
//
// Active, Deleted, and Invalid are separate: each multihash key is counted in
// one of them.
type StoreTotals struct {
	// Active counts multihash keys that decoded and still have at least one
	// value record. A key another provider still serves is active, including
	// slots whose own context record was removed.
	Active EntryMeters
	// Deleted counts multihash keys that decoded and whose value records are
	// all gone.
	Deleted EntryMeters
	// Invalid counts multihash keys whose value could not be decoded.
	// Slots stays zero for these records.
	Invalid EntryMeters
}

// ProviderStats counts one provider inside one scan. The scan addresses the
// row by the provider hash taken from multihash slots, then fills ProviderID
// from that provider's value record the first time the hash is seen.
//
// Multihashes here is not a share of StoreTotals.Active. A multihash
// that names two providers increments each provider once and the store total
// once. Multihashes counts only slots whose value record is still present.
// An empty ProviderID means every value record for that provider was already
// gone; the row is still stored so a resumed scan does not count the provider
// again, and both APIs omit it.
type ProviderStats struct {
	// ProviderID is the provider these counters belong to. Empty when no
	// value record for this provider could be read.
	ProviderID peer.ID
	// Multihashes is the number of distinct multihash keys that have at least
	// one remaining value record for this provider.
	Multihashes uint64
	// Slots is the number of value-key entries for this provider whose value
	// record is still present. One multihash advertised under two contexts
	// counts as two slots and one multihash.
	Slots uint64
	// DeletedContexts is the number of slots whose context value record is
	// gone while this provider still has some other value record. Each slot
	// is counted once. A provider with no value records left is counted in
	// StoreTotals.Deleted instead.
	DeletedContexts uint64
}

// ScanState is the lifecycle of the latest metering scan. It is the process
// view returned by MeteringScanStatus. The counters themselves are
// Current.Totals, the same StoreTotals the finished report stores.
type ScanState string

const (
	// ScanStateNone means no scan has been recorded.
	ScanStateNone ScanState = "none"
	// ScanStateInProgress means a scan is running, or an interrupted scan is
	// waiting to resume.
	ScanStateInProgress ScanState = "in_progress"
	// ScanStateDone means the latest scan finished and its counters are kept.
	ScanStateDone ScanState = "done"
	// ScanStateError means the latest scan stopped with a failure. Error
	// carries the reason. The counters committed before the failure are kept.
	ScanStateError ScanState = "error"
)

// ScanStatus is the walk, returned by MeteringScanStatus. State, the start
// time, the cursor, and the estimate describe the walk. Current.Totals is a
// StoreTotals, the same struct the finished report stores, including after
// the scan is done or has failed.
//
// EstimatedPercentDone and EstimatedFinish are not stored on this struct.
// While the scan is running they are computed from StartedAt and CursorKey.
// A done scan reports 100 and the time the scan finished.
type ScanStatus struct {
	// State is none, in_progress, done, or error.
	State ScanState
	// ScanID identifies this walk. It is the start time in microseconds.
	// Empty when State is none.
	ScanID uint64
	// StartedAt is when this walk started.
	StartedAt time.Time
	// CursorKey is the latest key the scan has finished. JSON encodes it as
	// base64. Empty until the first batch is committed. The estimate treats
	// its position in the sha2-256 multihash range as the fraction done.
	CursorKey []byte
	// EstimatedPercentDone is the cursor's position in the sha2-256 multihash
	// range, from 0 to 100. It is computed when status is read, not stored.
	// A done scan reports 100.
	EstimatedPercentDone float64
	// EstimatedFinish is when the scan is expected to complete if the rate so
	// far continues. Nil when the fraction done is still zero or StartedAt is
	// unset. A done scan reports the time it finished. Filled when status is
	// read, not stored on this struct.
	EstimatedFinish *time.Time
	// Current is the counters committed by finished batches, and by a scan
	// that has since finished or failed. Totals is a StoreTotals, the same
	// struct as the finished report. Providers follows the caller's selection.
	Current StatsSnapshot
	// Error is why the scan stopped when State is error. Empty otherwise.
	Error string `json:",omitempty"`
}
