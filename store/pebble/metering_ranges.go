package pebble

import (
	"bytes"
	"context"
	"slices"

	"github.com/cockroachdb/pebble/v2"
	"github.com/multiformats/go-multihash"
	mhcore "github.com/multiformats/go-multihash/core"
	"github.com/multiformats/go-varint"
)

// rangeSampleSize is how many keys of one multihash header the progress probe
// reads before extrapolating that header's share of the dataset.
const rangeSampleSize = 1024

// rangeSpanCap is the most ranges the progress probe records. The last one
// covers every key from its start to the end of the multihash space, so a
// flood of distinct headers cannot pin the probe.
const rangeSpanCap = 256

// rangeTailWeight is the fixed weight of that last range. The probe does not
// read its keys, so the weight is a guess: rangeSpanCap ranges of
// rangeSampleSize keys each. It outweighs the ranges that are not
// extrapolated, but next to an extrapolated range of a common hash it is a
// small share of the total.
const rangeTailWeight = rangeSpanCap * rangeSampleSize

// multihashScanRange is one contiguous multihash-header span and its estimated
// key count. Start is inclusive and End is exclusive. A range's share of the
// dataset is its Weight divided by the sum of every range's Weight.
type multihashScanRange struct {
	Start  []byte
	End    []byte
	Weight float64
	// Uniform is true when the range holds the keys of one uniform header
	// only. The estimate then reads progress inside the range from the
	// cursor's digest.
	Uniform bool `json:",omitempty"`
	// Tail is true for the last range of a capped probe. It covers every key
	// from its start to the end of the multihash space, and its Weight is a
	// guess rather than a count.
	Tail bool `json:",omitempty"`
}

// sampleMultihashRanges walks the multihash key space once and returns the
// range table used by the scan progress estimate. A uniform code gets one
// range per digest length, and its weight is extrapolated from a sample of
// rangeSampleSize keys and the digest position of the last one. Any other
// code, identity included, gets one range for all its lengths that weighs the
// keys read, at most rangeSampleSize, so it stays a small share of the total.
// A key that does not decode is a range of its own with weight 1. The probe checks ctx before each header and
// returns its cause once it is done.
func (r *meteringRunner) sampleMultihashRanges(ctx context.Context) ([]multihashScanRange, error) {
	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: multihashScanLower,
		UpperBound: multihashScanUpper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var ranges []multihashScanRange

	// Each pass seeks to the first key of the next multihash header
	for lower := multihashScanLower; iter.SeekGE(lower); {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		header, end, uniform := rangeHeader(iter.Key())

		if len(ranges) == rangeSpanCap-1 {
			// Last slot holds the rest of the multihash space as one range.
			// If we end up here, the dataset is really weird already and
			// we won't be able to properly estimate the progress.
			ranges = append(ranges, multihashScanRange{
				Start:  header,
				End:    multihashScanUpper,
				Weight: rangeTailWeight,
				Tail:   true,
			})
			break
		}

		// Calculate the number of consecutive keys in the range.
		k := 1
		for ; k < rangeSampleSize && iter.Next() && bytes.Compare(iter.Key(), end) < 0; k++ {
		}

		weight := float64(k)
		if uniform && k == rangeSampleSize {
			// Multihashes with the current header exceed the sample size,
			// based on the digest prefix we can estimate the total number
			// of keys with this header.
			digest := iter.Key()[len(header):]
			if p := digestPrefixFraction(digest); p > 0 && p < 1 {
				weight = float64(k) / p
			}
		}
		ranges = append(ranges, multihashScanRange{
			Start:   header,
			End:     end,
			Weight:  weight,
			Uniform: uniform,
		})
		lower = end
	}

	return ranges, nil
}

// rangeHeader returns the range that a multihash key starts during sampling.
// header is a copy of the key's range header, end is the first key after
// every key with that header, and uniform reports whether the code is a
// uniform hash. A key that does not decode is its own header, and its end is
// the key followed by a zero byte, so the range holds only that key: valid
// keys that extend it stay outside.
func rangeHeader(key []byte) (header, end []byte, uniform bool) {
	h, uniform, ok := multihashKeyHeader(key)
	if !ok {
		header = slices.Clone(key)
		return header, slices.Concat(key, []byte{0}), false
	}
	header = slices.Clone(h)
	return header, incrementBytes(header), uniform
}

// multihashKeyHeader returns the start of a multihash key that names its
// range. The key is the store prefix followed by the multihash. For a uniform
// code the header is the prefix, code, and digest length. For any other code
// it is the prefix and code. ok is false when the code, or the length of a
// uniform code, does not decode.
func multihashKeyHeader(key []byte) (header []byte, uniform, ok bool) {
	// key[1:] needs at least the prefix byte. Keys from the probe's iterator
	// always have it: the lower bound is the multihash prefix, and an empty
	// key sorts below that. A key of the prefix alone leaves nothing to
	// decode, so it is a key that does not decode.
	code, n, err := varint.FromUvarint(key[1:])
	if err != nil {
		return nil, false, false
	}
	if !isUniformMultihash(code) {
		// For non-uniform multihashes put all the keys into one range
		// no matter the length parameter.
		return key[:1+n], false, true
	}
	_, m, err := varint.FromUvarint(key[1+n:])
	if err != nil {
		return nil, false, false
	}
	return key[:1+n+m], true, true
}

// isUniformMultihash reports whether digests of this multihash code spread
// evenly over the digest space, at any digest length. These are cryptographic
// hash functions. Identity and unknown codes are not uniform.
func isUniformMultihash(code uint64) bool {
	switch code {
	case multihash.MD5,
		multihash.SHA1,
		mhcore.SHA2_224,
		multihash.SHA2_256,
		mhcore.SHA2_384,
		multihash.SHA2_512,
		mhcore.SHA2_512_224,
		mhcore.SHA2_512_256,
		multihash.SHA2_256_TRUNC254_PADDED,
		multihash.DBL_SHA2_256,
		multihash.SHA3_224,
		multihash.SHA3_256,
		multihash.SHA3_384,
		multihash.SHA3_512,
		multihash.SHAKE_128,
		multihash.SHAKE_256,
		multihash.KECCAK_224,
		multihash.KECCAK_256,
		multihash.KECCAK_384,
		multihash.KECCAK_512,
		multihash.BLAKE3:
		return true
	}
	return (code >= multihash.BLAKE2B_MIN && code <= multihash.BLAKE2B_MAX) ||
		(code >= multihash.BLAKE2S_MIN && code <= multihash.BLAKE2S_MAX)
}

// digestPrefixFraction is the digest's position in [0, 1) inside its multihash
// header, from the first prefixBytes digest bytes. A digest shorter than that
// is padded with zeros.
func digestPrefixFraction(digest []byte) float64 {
	// Digest bytes read as a big-endian number. scale is one past its largest
	// value, so the number maps into [0, 1).
	const prefixBytes = 4
	const scale = 1 << (8 * prefixBytes)

	v := uint64(0)
	for i := range prefixBytes {
		v <<= 8
		if i < len(digest) {
			v |= uint64(digest[i])
		}
	}
	return float64(v) / scale
}

// rangeContainsFraction is how far cursor has moved through rng, in [0, 1].
// A uniform range uses the cursor's digest prefix. Any other range, including
// the last range of a capped probe, reports 0 until the cursor leaves it, so
// the estimate never goes backwards.
func rangeContainsFraction(cursor []byte, rng multihashScanRange) float64 {
	if !rng.Uniform {
		return 0
	}
	digest, ok := bytes.CutPrefix(cursor, rng.Start)
	if !ok {
		return 0
	}
	return digestPrefixFraction(digest)
}

// scanProgressFraction is the share of the dataset the cursor has finished,
// from 0 to 1, using the range table on progress. It divides the weight of
// finished ranges, plus the done part of the range holding the cursor, by the
// total weight. An empty cursor or an empty table is 0.
func scanProgressFraction(progress *multihashScanProgressRecord) float64 {
	if progress == nil || len(progress.Cursor) == 0 {
		return 0
	}
	cursor := progress.Cursor
	var done, total float64
	for _, rng := range progress.Ranges {
		total += rng.Weight
		switch {
		case bytes.Compare(cursor, rng.End) >= 0:
			done += rng.Weight
		case bytes.Compare(cursor, rng.Start) >= 0:
			done += rng.Weight * rangeContainsFraction(cursor, rng)
		}
	}
	if total == 0 {
		return 0
	}
	return done / total
}

// incrementBytes returns the smallest byte string that sorts after every key
// that starts with b. An all-0xff input yields the exclusive end of the
// multihash key space.
func incrementBytes(b []byte) []byte {
	next := slices.Clone(b)
	for i := len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			return next
		}
	}
	return multihashScanUpper
}

// rangeSummary describes a scan's range table for the scan gauges. The capped
// last range is reported only as capped: its weight is a guess, not a count.
type rangeSummary struct {
	uniformRanges    int64
	nonUniformRanges int64
	uniformWeight    float64
	nonUniformWeight float64
	capped           bool
}

// summarizeRanges counts the ranges and their weights by kind.
func summarizeRanges(ranges []multihashScanRange) rangeSummary {
	var s rangeSummary
	for _, rng := range ranges {
		switch {
		case rng.Tail:
			s.capped = true
		case rng.Uniform:
			s.uniformRanges++
			s.uniformWeight += rng.Weight
		default:
			s.nonUniformRanges++
			s.nonUniformWeight += rng.Weight
		}
	}
	return s
}
