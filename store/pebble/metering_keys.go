package pebble

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"time"

	"github.com/ipni/go-indexer-core"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"lukechampine.com/blake3"
)

// Metering key layout under meteringKeyPrefix (0x04):
//
//	0x04 'c'                              current completed scan
//	0x04 'p'                              in-progress scan progress
//	0x04 's' scanID(8B BE) 't'            totals for scanID
//	0x04 's' scanID(8B BE) 'v' pidHash    provider stats for scanID
const (
	meteringKindCurrent  = 'c'
	meteringKindProgress = 'p'
	meteringKindScan     = 's'
	meteringScanTotals   = 't'
	meteringScanProvider = 'v'
)

// multihashScanProgressRecord is the persisted process view of one scan, at
// the single progress key. MeteringScanStatus copies it into ScanStatus and
// computes EstimatedPercentDone and EstimatedFinish from StartedAt and Cursor.
// Each batch overwrites this record. Success keeps it with DoneAt set, so
// status can still show the finished scan. The next scan replaces it.
// Failure keeps it and sets Error, which stops the next startup from
// resuming the walk.
//
// KeysRead and BytesRead count every visited key. They are not part of
// StoreTotals: decoded keys are StoreTotals.Active or StoreTotals.Deleted,
// and undecodable keys are StoreTotals.Invalid.
//
// A future shard keeps its own progress record under a separate key, so
// workers do not write the same record.
type multihashScanProgressRecord struct {
	// ScanID is the walk identity, the start time in microseconds. It selects
	// the per-scan totals and provider rows.
	ScanID uint64
	// StartedAt is when the walk started. The status estimate uses it with
	// Cursor to project a finish time.
	StartedAt time.Time
	// KeysRead is how many keys the iterator has visited, including values
	// that did not decode. It drives the cursor and the finish estimate.
	// It is not part of StoreTotals; decoded keys are StoreTotals.Active or
	// StoreTotals.Deleted, and undecodable keys are StoreTotals.Invalid.
	KeysRead uint64
	// BytesRead is the total size of visited keys and values, including values
	// that did not decode. Same split as KeysRead: the public counters are
	// StoreTotals.Active, StoreTotals.Deleted, and StoreTotals.Invalid.
	BytesRead uint64
	// Cursor is the last multihash key finished. Empty until the first batch
	// commits. The next batch starts strictly after it. Copied to
	// ScanStatus.CursorKey.
	Cursor []byte
	// DoneAt is when the scan finished. It is written in the same batch as the
	// completed-scan header. Zero while the scan is unfinished. Status reports
	// this as the finish time once the scan is done.
	DoneAt time.Time `json:",omitzero"`
	// Error is why the scan stopped, when it stopped with a failure. Empty
	// while the scan is running. A non-empty Error is not resumed on startup.
	Error string `json:",omitempty"`
}

// multihashScanResults is the persisted header of the latest finished scan,
// at the single current-scan key. It becomes CompletedScanStats inside
// AllStatsReport. Provider rows are not in this record; they stay under the
// scan-id prefix and are loaded when the report is read. The next successful
// scan replaces this record and deletes every other scan-id prefix.
type multihashScanResults struct {
	// ScanID selects the provider rows that belong to this finished scan.
	ScanID uint64
	// StartedAt is when the walk started.
	StartedAt time.Time
	// CompletedAt is when counting finished. Reported as MeasuredAt.
	CompletedAt time.Time
	// Totals is the whole-store counters at completion. The same value is
	// stored under the scan-id totals key during the walk; this copy is the
	// one MeteringAllStats returns.
	Totals indexer.StoreTotals
}

var (
	// meteringCurrentKey addresses the latest completed multihash scan.
	meteringCurrentKey = []byte{byte(meteringKeyPrefix), meteringKindCurrent}
	// meteringProgressKey addresses the multihash scan in progress.
	meteringProgressKey = []byte{byte(meteringKeyPrefix), meteringKindProgress}

	// meteringAllScansStart is the first key of any per-scan record.
	meteringAllScansStart = []byte{byte(meteringKeyPrefix), meteringKindScan}
	// meteringAllScansEnd is the first key after every per-scan record.
	meteringAllScansEnd = []byte{byte(meteringKeyPrefix), meteringKindScan + 1}
)

// meteringScanPrefix is the key prefix shared by the totals and provider rows of scanID.
func meteringScanPrefix(scanID uint64) []byte {
	k := make([]byte, 2+8)
	k[0] = byte(meteringKeyPrefix)
	k[1] = meteringKindScan
	binary.BigEndian.PutUint64(k[2:], scanID)
	return k
}

// meteringTotalsKey addresses the store totals for scanID.
func meteringTotalsKey(scanID uint64) []byte {
	return append(
		meteringScanPrefix(scanID),
		meteringScanTotals,
	)
}

// meteringProviderPrefix is the first provider-row key for scanID.
func meteringProviderPrefix(scanID uint64) []byte {
	return append(meteringScanPrefix(scanID), meteringScanProvider)
}

// meteringProviderPrefixEnd is the first key after the provider rows of scanID.
func meteringProviderPrefixEnd(scanID uint64) []byte {
	return append(meteringScanPrefix(scanID), meteringScanProvider+1)
}

// meteringProviderKey addresses one provider's counters for scanID.
// providerHash is the blake3 prefix taken from a multihash slot.
func meteringProviderKey(scanID uint64, providerHash []byte) []byte {
	return append(meteringProviderPrefix(scanID), providerHash...)
}

// meteringScanIDRange is the half-open key span of scanID.
// scanPrefix is the first key of this scan. nextScanPrefix is the first key of the following scan ID.
func meteringScanIDRange(scanID uint64) (scanPrefix, nextScanPrefix []byte) {
	scanPrefix = meteringScanPrefix(scanID)
	nextScanPrefix = make([]byte, len(scanPrefix))
	copy(nextScanPrefix, scanPrefix)
	// scanID is big-endian in bytes [2:10]. The following scan ID is that value plus one.
	nextID := binary.BigEndian.Uint64(nextScanPrefix[2:]) + 1
	binary.BigEndian.PutUint64(nextScanPrefix[2:], nextID)
	return scanPrefix, nextScanPrefix
}

// multihashScanLower is the first multihash. multihashScanUpper is the
// exclusive end, one byte past the multihash prefix.
var (
	multihashScanLower = []byte{byte(multihashKeyPrefix)}
	multihashScanUpper = []byte{byte(multihashKeyPrefix) + 1}
)

// encodeMultihashScanProgress serializes a multihash scan cursor as JSON.
func encodeMultihashScanProgress(p *multihashScanProgressRecord) ([]byte, error) {
	return json.Marshal(p)
}

// decodeMultihashScanProgress reads a multihash scan cursor from JSON.
func decodeMultihashScanProgress(b []byte) (*multihashScanProgressRecord, error) {
	var p multihashScanProgressRecord
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// encodeMultihashScanResults serializes the latest completed multihash scan as JSON.
func encodeMultihashScanResults(c *multihashScanResults) ([]byte, error) {
	return json.Marshal(c)
}

// decodeMultihashScanResults reads the latest completed multihash scan from JSON.
func decodeMultihashScanResults(b []byte) (*multihashScanResults, error) {
	var c multihashScanResults
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// encodeStoreTotals serializes whole-store multihash counters as JSON.
func encodeStoreTotals(t *indexer.StoreTotals) ([]byte, error) {
	return json.Marshal(t)
}

// decodeStoreTotals reads whole-store multihash counters from JSON.
func decodeStoreTotals(b []byte) (*indexer.StoreTotals, error) {
	var t indexer.StoreTotals
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// providerStatsJSON embeds ProviderStats so every counter follows that struct.
// ProviderID is the text form from peer.ID.String (base58). The raw peer ID
// is not always valid UTF-8, which JSON cannot store. The tag makes this
// field win over the embedded peer.ID of the same name.
type providerStatsJSON struct {
	indexer.ProviderStats
	ProviderID string `json:"ProviderID"`
}

// encodeProviderStats serializes one provider's multihash counters as JSON.
func encodeProviderStats(ps *indexer.ProviderStats) ([]byte, error) {
	return json.Marshal(providerStatsJSON{
		ProviderStats: *ps,
		ProviderID:    ps.ProviderID.String(),
	})
}

// decodeProviderStats reads one provider's multihash counters from JSON.
func decodeProviderStats(b []byte) (*indexer.ProviderStats, error) {
	var raw providerStatsJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	if raw.ProviderID != "" {
		pid, err := peer.Decode(raw.ProviderID)
		if err != nil {
			return nil, err
		}
		raw.ProviderStats.ProviderID = pid
	}
	return &raw.ProviderStats, nil
}

// meteringProviderHashOff is where the provider hash starts in a provider-row key.
// The prefix has the same length for every scan ID.
var meteringProviderHashOff = len(meteringProviderPrefix(0))

// providerHashFromStatsKey reads the provider hash from a provider-row key.
func providerHashFromStatsKey(key []byte) []byte {
	if len(key) < meteringProviderHashOff+providerHashLen {
		return nil
	}
	return key[meteringProviderHashOff : meteringProviderHashOff+providerHashLen]
}

// providerStatsMapKey is the in-memory key for a provider hash.
func providerStatsMapKey(providerHash []byte) string {
	return string(providerHash)
}

// nextProviderValueKey is the first value key of the provider after providerHash.
func nextProviderValueKey(providerHash []byte) []byte {
	next := make([]byte, 1+len(providerHash))
	next[0] = byte(valueKeyPrefix)
	copy(next[1:], providerHash)
	for i := len(next) - 1; i >= 1; i-- {
		next[i]++
		if next[i] != 0 {
			return next
		}
	}
	return []byte{byte(valueKeyPrefix) + 1}
}

// sha256MultihashProgressPrefix is the multihash key header for sha2-256 32-byte digests.
var sha256MultihashProgressPrefix = []byte{byte(multihashKeyPrefix), byte(multihash.SHA2_256), 32}

// sha256ProgressScale is one past the largest uint32, so a digest prefix maps into [0, 1).
const sha256ProgressScale = float64(uint64(math.MaxUint32) + 1)

// sha256MultihashKeyFraction is the cursor's position in [0, 1] through the sha2-256
// 32-byte multihash range. Keys before that header are 0 and keys after it are 1.
// Inside the range, the first four digest bytes are the position.
func sha256MultihashKeyFraction(key []byte) float64 {
	if !bytes.HasPrefix(key, sha256MultihashProgressPrefix) {
		if bytes.Compare(key, sha256MultihashProgressPrefix) < 0 {
			return 0
		}
		return 1
	}
	const digestPrefixLen = 4
	if len(key) < len(sha256MultihashProgressPrefix)+digestPrefixLen {
		return 0
	}
	off := len(sha256MultihashProgressPrefix)
	u := binary.BigEndian.Uint32(key[off : off+digestPrefixLen])
	return float64(u) / sha256ProgressScale
}

// hashProviderID is the blake3 prefix used to address a provider's stats.
// It matches the provider half of a value key.
func hashProviderID(pid peer.ID) []byte {
	h := blake3.New(providerHashLen, nil)
	_, _ = h.Write([]byte(pid))
	return h.Sum(nil)
}

// providerHashFromValueKey extracts the provider hash from a value key
// (0x02 | pidHash | ctxHash). Returns nil if the key is too short.
func providerHashFromValueKey(vk []byte) []byte {
	if len(vk) < 1+providerHashLen {
		return nil
	}
	if vk[0] != byte(valueKeyPrefix) {
		return nil
	}
	return vk[1 : 1+providerHashLen]
}
