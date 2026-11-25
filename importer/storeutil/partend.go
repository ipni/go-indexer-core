// Package storeutil holds helpers for stores that implement the import's
// Source and Target. PlanPart picks the end of an open part for a source
// whose cursors are keys in ascending byte order. It needs only reads, so
// any store can use it, and it counts data that the store has not flushed.
package storeutil

import (
	"bytes"
	"iter"
	"math/big"

	"github.com/ipni/go-indexer-core/importer"
)

// EntriesFunc returns a kind's entries in ascending key order from the key
// from, inclusive. A store may skip keys that it does not send. An entry's
// slices are valid until the next iteration.
type EntriesFunc func(from []byte) iter.Seq2[importer.Entry, error]

// ClusterLenFunc returns how many leading bytes of key name its cluster. Keys
// of one cluster spread their data evenly over the bytes after it.
type ClusterLenFunc func(key []byte) int

// maxSampleBytes is the most bytes of entry keys and values that PlanPart
// reads as a part's sample
const maxSampleBytes = 1 << 20

// PlanPart picks the end of an open part that starts at start and holds
// about sizeHint bytes of entry keys and values. It reads the kind's
// entries with entries, and copies the ones it keeps. clusterLen names the
// cluster of each key: the store's data spreads evenly over the keys of one
// cluster, and clusters can hold very different amounts.
//
// It reads a sample of about min(sizeHint, 1 MiB) bytes from start, and
// then picks the end by the first rule that applies:
//   - The kind ends inside the sample: the part is the sample, and partEnd
//     is nil.
//   - The sample holds sizeHint bytes or more: the part is the sample.
//   - The sample's keys belong to more than one cluster: the part is the
//     whole sample. The data was already read, and the next part then
//     starts inside the last cluster and can estimate it.
//   - The sample's keys belong to one cluster: it reads keys as big-endian
//     numbers and estimates the end as first + (last - first) × sizeHint /
//     sample bytes, where first and last are the sample's first and last
//     keys. When the estimate is not past the last key, the part is the
//     sample. The part ends at the estimate, or at the cluster's end when
//     the estimate is past it or past the largest key of its length. The
//     cluster's end is the cluster's bytes incremented by one; a cluster of
//     only 0xff bytes, or of no bytes, ends with the kind, and partEnd is
//     then nil.
//
// When the part is the sample alone, partEnd is the smallest key after the
// sample's last key.
//
// head holds the entries that the part yields first, and afterHead is the
// cursor to read the rest of the part from, up to partEnd; afterHead is nil
// when head is the whole part. A non-nil partEnd is strictly after start
// and after every key in head.
func PlanPart(
	entries EntriesFunc,
	start []byte,
	sizeHint uint64,
	clusterLen ClusterLenFunc,
) (
	partEnd []byte,
	head []importer.Entry,
	afterHead []byte,
	err error,
) {
	// Sample: read entries from start until they hold sampleBytesBoundary
	sampleBytesBoundary := int(max(min(sizeHint, maxSampleBytes), 1))
	got, allEntriesRead := 0, true
	for e, err := range entries(start) {
		if err != nil {
			return nil, nil, nil, err
		}
		head = append(head, importer.Entry{Key: bytes.Clone(e.Key), Value: bytes.Clone(e.Value)})
		if got += len(e.Key) + len(e.Value); got >= sampleBytesBoundary {
			allEntriesRead = false
			break
		}
	}

	if allEntriesRead {
		// The sample covers the rest of that keyspace
		return nil, head, nil, nil
	}

	// The cursor after the sample: its last key followed by 0x00
	afterHead = append(bytes.Clone(head[len(head)-1].Key), 0)

	if uint64(got) >= sizeHint {
		// The sample is large enough to contain all the entries in this part
		return afterHead, head, nil, nil
	}

	// The sample crosses into another cluster when its first and last keys
	// are in different clusters. Keys ascend, so that is enough. The data
	// was already read, so the part is the whole sample; the next part then
	// starts inside the last cluster and can estimate it.
	if !bytes.Equal(cluster(head[0].Key, clusterLen), cluster(head[len(head)-1].Key, clusterLen)) {
		return afterHead, head, nil, nil
	}

	// Estimate the end, assuming the data is spread evenly over the keys:
	// first + (last - first) × sizeHint / got, with the keys read as big-endian
	// numbers. Keys of different lengths are compared as numbers of the
	// longer length, the shorter one padded with 0x00.
	first, last := head[0].Key, head[len(head)-1].Key
	n := max(len(first), len(last))
	lo, hi := keyToNumber(first, n), keyToNumber(last, n)
	end := new(big.Int).Sub(hi, lo)
	end.Mul(end, new(big.Int).SetUint64(sizeHint))
	end.Quo(end, big.NewInt(int64(got)))
	end.Add(end, lo)

	// An estimate that is not past the last key: the part is the sample.
	// This happens with a sample of one key, and when rounding keeps the
	// estimate at the last key.
	if end.Cmp(hi) <= 0 {
		return afterHead, head, nil, nil
	}

	// The data spreads evenly only inside the cluster, so the part ends at
	// the cluster's end at the latest
	clusterEnd := incremented(cluster(first, clusterLen))

	// An estimate past the largest key of n bytes: by the sample's density
	// the rest of the cluster holds less than sizeHint, so the part runs to
	// the cluster's end
	if end.BitLen() > 8*n {
		return clusterEnd, head, afterHead, nil
	}

	// The part ends at the estimate, or at the cluster's end when that
	// comes first, and the rest of it is read after the sample
	partEnd = end.FillBytes(make([]byte, n))
	if clusterEnd != nil && bytes.Compare(partEnd, clusterEnd) > 0 {
		partEnd = clusterEnd
	}
	return partEnd, head, afterHead, nil
}

// cluster returns the leading bytes of key that clusterLen names as its
// cluster, at most the whole key
func cluster(key []byte, clusterLen ClusterLenFunc) []byte {
	return key[:min(max(clusterLen(key), 0), len(key))]
}

// incremented returns the smallest key of len(prefix) bytes after every key
// that starts with prefix: prefix incremented by one as a big-endian number,
// with carry. It returns nil when there is none, for a prefix of only 0xff
// bytes or of no bytes.
func incremented(prefix []byte) []byte {
	next := bytes.Clone(prefix)
	for i := len(next) - 1; i >= 0; i-- {
		if next[i]++; next[i] != 0 {
			return next
		}
	}
	return nil
}

// keyToNumber reads key, padded with 0x00 to n bytes, as a big-endian number
func keyToNumber(key []byte, n int) *big.Int {
	p := make([]byte, n)
	copy(p, key)
	return new(big.Int).SetBytes(p)
}
