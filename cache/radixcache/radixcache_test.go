package radixcache

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/libp2p/go-libp2p-core/peer"
)

const peerID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"

var provID peer.ID
var ctxID []byte

func init() {
	var err error
	provID, err = peer.Decode(peerID)
	if err != nil {
		panic(err)
	}

	ctxID = []byte("test-ctx-1")
}

func TestPutGetRemove(t *testing.T) {
	s := New(1000000)
	mhs := test.RandomMultihashes(15)

	provID, err := peer.Decode(peerID)
	if err != nil {
		t.Fatal(err)
	}
	value1 := indexer.Value{
		ProviderID:    provID,
		ContextID:     ctxID,
		MetadataBytes: []byte("metadata1"),
	}
	value2 := indexer.Value{
		ProviderID:    provID,
		ContextID:     ctxID,
		MetadataBytes: []byte("metadata2"),
	}

	single := mhs[0]
	noadd := mhs[1]
	batch := mhs[2:]

	// Put a single multihash
	t.Log("Put/Get a single multihash in primary storage")
	s.Put(value1, single)
	stats := s.Stats()
	if stats.Indexes != 1 || stats.Values != 1 {
		t.Fatal("Did not put new single multihash")
	}
	ents, found := s.Get(single)
	if !found {
		t.Error("Error finding single multihash")
	}
	if !ents[0].Equal(value1) {
		t.Error("Got wrong value for single multihash")
	}

	t.Log("Put existing multihash value again")
	s.Put(value1, single)
	stats = s.Stats()
	if stats.Indexes != 1 {
		t.Fatalf("should not have put new index, count: %d", stats.Indexes)
	}
	if stats.Values != 1 {
		t.Fatalf("should not have put new value, count: %d", stats.Values)
	}

	t.Log("Put existing multihash and provider with new metadata")
	s.Put(value2, single)
	stats = s.Stats()
	if stats.Indexes != 1 {
		t.Fatalf("should not have put new index, count: %d", stats.Indexes)
	}
	// Values should still be 1, since context ID of value 1 and 2 is the same.
	if stats.Values != 1 {
		t.Fatalf("should not have put new value, count: %d", stats.Values)
	}
	ents, found = s.Get(single)
	if !found {
		t.Error("Error finding single multihash")
	}
	if !ents[0].Equal(value2) {
		t.Error("Got wrong value for single multihash")
	}

	ctxID2 := []byte("test-ctx-2")
	value3 := indexer.Value{
		ProviderID:    provID,
		ContextID:     ctxID2,
		MetadataBytes: []byte(mhs[1]),
	}
	s.Put(value3, single)
	stats = s.Stats()
	if stats.Indexes != 1 {
		t.Fatalf("expected index count 1, got %d", stats.Indexes)
	}
	if stats.Values != 2 {
		t.Fatalf("expected value count 2, got %d", stats.Values)
	}

	t.Log("Check for all valuess for single multihash")
	ents, found = s.Get(single)
	if !found {
		t.Error("Error finding a multihash from the batch")
	}
	if len(ents) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if !ents[1].Equal(value3) {
		t.Error("Got wrong value for single multihash")
	}

	// Put a batch of multihashes
	t.Log("Put/Get a batch of multihashes in primary storage")
	prevStats := s.Stats()
	s.Put(value1, batch...)
	curStats := s.Stats()
	if curStats.Indexes != prevStats.Indexes+len(batch) {
		t.Fatalf("Did not get expected index count of %d, got %d", prevStats.Indexes+len(batch), curStats.Indexes)
	}
	t.Logf("Stored %d new values out of %d total", len(batch), curStats.Indexes)

	ents, found = s.Get(mhs[5])
	if !found {
		t.Error("did not find a multihash from the batch")
	}
	if !ents[0].Equal(value1) {
		t.Error("Got wrong value for single multihash")
	}

	// Get a key that is not set
	t.Log("Get non-existing key")
	_, found = s.Get(noadd)
	if found {
		t.Error("Error, the key for the multihash shouldn't be set")
	}

	t.Log("Remove valuey for multihash")
	removed := s.Remove(value3, single)
	if removed != 1 {
		t.Fatalf("should have removed 1 value, removed %d", removed)
	}

	t.Log("Check for all values for single multihash")
	ents, found = s.Get(single)
	if !found {
		t.Error("Error finding a multihash from the batch")
	}
	if len(ents) != 1 {
		t.Fatal("Update over existing key not correct")
	}
	if !ents[0].Equal(value1) {
		t.Error("Got wrong value for single multihash")
	}

	t.Log("Remove only value for multihash")
	if s.Remove(value1, single) == 0 {
		t.Fatal("should have removed value")
	}
	_, found = s.Get(single)
	if found {
		t.Fatal("Should not have found multihash with no values")
	}

	t.Log("Remove value for non-existent multihash")
	if s.Remove(value1, single) != 0 {
		t.Fatal("should not have removed non-existent value")
	}

	stats = s.Stats()
	t.Log("Remove provider")
	removed = s.RemoveProvider(provID)
	if removed < stats.Indexes {
		t.Fatalf("should have removed %d indexes, only removed %d", stats.Indexes, removed)
	}
	stats = s.Stats()
	if stats.Indexes != 0 || stats.Values != 0 {
		t.Fatalf("should have no indexes or values after removing only provider")
	}
}

func TestRotate(t *testing.T) {
	const maxSize = 10

	mhs := test.RandomMultihashes(2)

	value1 := indexer.Value{
		ProviderID:    provID,
		ContextID:     []byte("test-ctx-1"),
		MetadataBytes: []byte(mhs[0]),
	}
	value2 := indexer.Value{
		ProviderID:    provID,
		ContextID:     []byte("test-ctx-2"),
		MetadataBytes: []byte(mhs[1]),
	}

	s := New(maxSize * 2)
	mhs = test.RandomMultihashes(maxSize + 5)

	s.Put(value1, mhs...)
	stats := s.Stats()
	if stats.Indexes == 0 || stats.Values == 0 {
		t.Fatal("did not put batch of multihashes")
	}

	_, found := s.Get(mhs[0])
	if !found {
		t.Error("Error finding a multihash from previous cache")
	}

	_, found = s.Get(mhs[maxSize+2])
	if !found {
		t.Error("Error finding a multihash from new cache")
	}

	mhs2 := test.RandomMultihashes(maxSize)

	if s.Put(value2, mhs2...) != len(mhs2) {
		t.Fatal("did not put batch of multihashes")
	}

	// Should find this because it was moved to new cache after 1st rotation
	_, found = s.Get(mhs[0])
	if !found {
		t.Error("Error finding a multihash from previous cache")
	}

	// Should find this because it should be in old cache after 2nd rotation
	_, found = s.Get(mhs[maxSize+2])
	if !found {
		t.Error("Error finding a multihash from new cache")
	}

	// Should not find this because it was only in old cache after 1st rotation
	_, found = s.Get(mhs[2])
	if found {
		t.Error("multihash should have been rotated out of cache")
	}
}

func TestUnboundedGrowth(t *testing.T) {
	const maxSize = 4
	s := New(maxSize)
	mhs := test.RandomMultihashes(11)

	mhash := mhs[0]
	mhs = mhs[1:]
	value := indexer.Value{
		ProviderID:    provID,
		MetadataBytes: []byte("metadata"),
	}

	for i := range mhs {
		value.ContextID = []byte(mhs[i])
		s.Put(value, mhash)
		s.Remove(value, mhash)
	}

	st := s.Stats()
	if st.Values > maxSize {
		t.Fatal("Unbounded memory growth")
	}

	s = New(maxSize)

	for i := 0; i < maxSize; i++ {
		value.ContextID = []byte(mhs[i])
		s.Put(value, mhash)
		s.Remove(value, mhash)
	}

	value.ContextID = []byte(mhs[4])
	s.Put(value, mhash)
	value.ContextID = []byte(mhs[5])
	s.Put(value, mhash)

	st = s.Stats()
	if st.Values > 2*maxSize {
		t.Fatal("Unbounded memory growth")
	}
}

func TestMemoryUse(t *testing.T) {
	skipUnlessMemUse(t)

	mhs := test.RandomMultihashes(1)

	ctxID := []byte("test-ctx-1")
	value := indexer.Value{
		ProviderID:    provID,
		ContextID:     ctxID,
		MetadataBytes: []byte(mhs[0]),
	}
	var prevAlloc uint64
	var prevIndexes int

	for count := 1; count <= 1024; count *= 2 {
		t.Run(fmt.Sprintf("MemoryUse %d multihashes", count*1024), func(t *testing.T) {
			s := New(1024 * count)
			for i := 0; i < count; i++ {
				mhs = test.RandomMultihashes(1024)
				s.Put(value, mhs...)
			}
			mhs = nil
			runtime.GC()
			m := runtime.MemStats{}
			runtime.ReadMemStats(&m)
			stats := s.Stats()
			t.Log("Items in cache:", stats.Indexes)
			t.Log("Alloc after GC: ", m.Alloc)
			t.Log("Items delta:", stats.Indexes-prevIndexes)
			t.Log("Alloc delta:", m.Alloc-prevAlloc)
			t.Log("Evictions:", stats.Evictions)
			t.Log("Values:", stats.Values)
			prevAlloc = m.Alloc
			prevIndexes = stats.Indexes
		})
	}

}

func TestMemSingleVsMany(t *testing.T) {
	skipUnlessMemUse(t)

	mhs := test.RandomMultihashes(1)

	value := indexer.Value{
		ProviderID:    provID,
		ContextID:     ctxID,
		MetadataBytes: []byte(mhs[0]),
	}

	t.Run(fmt.Sprintf("Put %d Single multihashes", 1024*1024), func(t *testing.T) {
		s := New(1024 * 1024)
		for i := 0; i < 1024; i++ {
			mhs = test.RandomMultihashes(1024)
			for j := range mhs {
				s.Put(value, mhs[j])
			}
		}
		runtime.GC()
		m := runtime.MemStats{}
		runtime.ReadMemStats(&m)
		t.Log("Alloc after GC: ", m.Alloc)
	})

	t.Run(fmt.Sprintf("Put %d multihashes in groups of 1024", 1024*1024), func(t *testing.T) {
		s := New(1024 * 1024)
		for i := 0; i < 1024; i++ {
			mhs = test.RandomMultihashes(1024)
			s.Put(value, mhs...)
		}
		runtime.GC()
		m := runtime.MemStats{}
		runtime.ReadMemStats(&m)
		t.Log("Alloc after GC: ", m.Alloc)
	})
}

func BenchmarkPut(b *testing.B) {
	mhs := test.RandomMultihashes(1)
	value := indexer.Value{
		ProviderID:    provID,
		ContextID:     ctxID,
		MetadataBytes: []byte(mhs[0]),
	}

	mhs = test.RandomMultihashes(10240)

	b.Run("Put single", func(b *testing.B) {
		s := New(8192)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			elem := i % len(mhs)
			s.Put(value, mhs[elem])
		}
	})

	for testCount := 1024; testCount < len(mhs); testCount *= 2 {
		b.Run(fmt.Sprint("Put", testCount), func(b *testing.B) {
			s := New(8192)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < testCount; j++ {
					s.Put(value, mhs[j])
				}
			}
		})

		b.Run(fmt.Sprint("PutMany", testCount), func(b *testing.B) {
			s := New(8192)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Put(value, mhs[:testCount]...)
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	mhs := test.RandomMultihashes(1)
	value := indexer.Value{
		ProviderID:    provID,
		ContextID:     ctxID,
		MetadataBytes: []byte(mhs[0]),
	}

	s := New(8192)
	mhs = test.RandomMultihashes(4096)
	s.Put(value, mhs...)

	b.Run("Get single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, ok := s.Get(mhs[i%len(mhs)])
			if !ok {
				panic("missing multihash")
			}
		}
	})

	for testCount := 1024; testCount < 10240; testCount *= 2 {
		b.Run(fmt.Sprint("Get", testCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < testCount; j++ {
					_, ok := s.Get(mhs[j%len(mhs)])
					if !ok {
						panic("missing multihash")
					}
				}
			}
		})
	}
}

func skipUnlessMemUse(t *testing.T) {
	if os.Getenv("TEST_MEM_USE") == "" {
		t.SkipNow()
	}
}
