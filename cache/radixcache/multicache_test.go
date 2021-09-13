package radixcache

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/test"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var p peer.ID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
var proto uint64

func TestPutGetRemove(t *testing.T) {
	s := New(1000000)
	mhs, err := test.RandomMultihashes(15)
	if err != nil {
		t.Fatal(err)
	}

	value1 := indexer.MakeValue(p, proto, []byte(mhs[0]))
	value2 := indexer.MakeValue(p, proto, []byte(mhs[1]))

	single := mhs[2]
	noadd := mhs[3]
	batch := mhs[4:]

	// Put a single multihash
	t.Log("Put/Get a single multihash in primary storage")
	if !s.PutCheck(single, value1) {
		t.Fatal("Did not put new single multihash")
	}
	ents, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding single multihash")
	}
	if !ents[0].Equal(value1) {
		t.Error("Got wrong value for single multihash")
	}

	t.Log("Put existing multihash value")
	if s.PutCheck(single, value1) {
		t.Fatal("should not have put new value")
	}

	t.Log("Put existing multihash and provider with new metadata")
	if !s.PutCheck(single, value2) {
		t.Fatal("should have put new value")
	}

	t.Log("Check for all valuess for single multihash")
	ents, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a multihash from the batch")
	}
	if len(ents) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if !ents[1].Equal(value2) {
		t.Error("Got wrong value for single multihash")
	}

	// Put a batch of multihashes
	t.Log("Put/Get a batch of multihashes in primary storage")
	count := s.PutManyCount(batch, value1)
	if count == 0 {
		t.Fatal("Did not put batch of multihashes")
	}
	t.Logf("Stored %d new values out of %d total", count, len(batch))

	ents, found, err = s.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("did not find a multihash from the batch")
	}
	if !ents[0].Equal(value1) {
		t.Error("Got wrong value for single multihash")
	}

	// Get a key that is not set
	t.Log("Get non-existing key")
	_, found, err = s.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("Error, the key for the multihash shouldn't be set")
	}

	t.Log("Remove valuey for multihash")
	if !s.RemoveCheck(single, value2) {
		t.Fatal("should have removed value")
	}

	t.Log("Check for all values for single multihash")
	ents, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
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
	if !s.RemoveCheck(single, value1) {
		t.Fatal("should have removed value")
	}
	_, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("Should not have found multihash with no values")
	}
	t.Log("Remove value for non-existent multihash")
	if s.RemoveCheck(single, value1) {
		t.Fatal("should not have removed non-existent value")
	}

	stats := s.Stats()
	t.Log("Remove provider")
	removed := s.RemoveProviderCount(p)
	if removed < stats.Indexes {
		t.Fatalf("should have removed at least %d values, only removed %d", stats.Indexes, removed)
	}
	stats = s.Stats()
	if stats.Indexes != 0 {
		t.Fatal("should have 0 size after removing only provider")
	}
}

func TestRotate(t *testing.T) {
	const maxSize = 10

	mhs, err := test.RandomMultihashes(2)
	if err != nil {
		t.Fatal(err)
	}
	value1 := indexer.MakeValue(p, proto, []byte(mhs[0]))
	value2 := indexer.MakeValue(p, proto, []byte(mhs[1]))

	s := New(maxSize * 2)
	mhs, err = test.RandomMultihashes(maxSize + 5)
	if err != nil {
		t.Fatal(err)
	}

	if s.PutManyCount(mhs, value1) == 0 {
		t.Fatal("did not put batch of multihashes")
	}

	_, found, err := s.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a multihash from previous cache")
	}

	_, found, err = s.Get(mhs[maxSize+2])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a multihash from new cache")
	}

	mhs2, err := test.RandomMultihashes(maxSize)
	if err != nil {
		t.Fatal(err)
	}

	if s.PutManyCount(mhs2, value2) == 0 {
		t.Fatal("did not put batch of multihashes")
	}

	// Should find this because it was moved to new cache after 1st rotation
	_, found, err = s.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a multihash from previous cache")
	}

	// Should find this because it should be in old cache after 2nd rotation
	_, found, err = s.Get(mhs[maxSize+2])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a multihash from new cache")
	}

	// Should not find this because it was only in old cache after 1st rotation
	_, found, err = s.Get(mhs[2])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("multihash should have been rotated out of cache")
	}
}

func TestMemoryUse(t *testing.T) {
	skipUnlessMemUse(t)

	mhs, err := test.RandomMultihashes(1)
	if err != nil {
		panic(err)
	}
	value := indexer.MakeValue(p, proto, []byte(mhs[0]))
	var prevAlloc, prevIndexes uint64

	for count := 1; count <= 1024; count *= 2 {
		t.Run(fmt.Sprintf("MemoryUse %d multihashes", count*1024), func(t *testing.T) {
			s := New(1202 * count)
			for i := 0; i < count; i++ {
				mhs, _ = test.RandomMultihashes(1024)
				err = s.PutMany(mhs, value)
				if err != nil {
					t.Fatal(err)
				}
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
			t.Log("Rotations:", stats.Rotations)
			t.Log("Values:", stats.Values)
			t.Log("Unique Values:", stats.UniqueValues)
			t.Log("Interned Values:", stats.InternedValues)
			prevAlloc = m.Alloc
			prevIndexes = stats.Indexes
		})
	}

}

func TestMemSingleVsMany(t *testing.T) {
	skipUnlessMemUse(t)

	mhs, err := test.RandomMultihashes(1)
	if err != nil {
		panic(err)
	}
	value := indexer.MakeValue(p, proto, []byte(mhs[0]))

	t.Run(fmt.Sprintf("Put %d Single multihashes", 1024*1024), func(t *testing.T) {
		s := New(1024 * 1064)
		for i := 0; i < 1024; i++ {
			mhs, _ = test.RandomMultihashes(1024)
			for j := range mhs {
				s.PutCheck(mhs[j], value)
			}
		}
		runtime.GC()
		m := runtime.MemStats{}
		runtime.ReadMemStats(&m)
		t.Log("Alloc after GC: ", m.Alloc)
	})

	t.Run(fmt.Sprintf("Put %d multihashes in groups of 1024", 1024*1024), func(t *testing.T) {
		s := New(1024 * 1064)
		for i := 0; i < 1024; i++ {
			mhs, _ = test.RandomMultihashes(1024)
			s.PutManyCount(mhs, value)
		}
		runtime.GC()
		m := runtime.MemStats{}
		runtime.ReadMemStats(&m)
		t.Log("Alloc after GC: ", m.Alloc)
	})
}

func BenchmarkPut(b *testing.B) {
	mhs, err := test.RandomMultihashes(1)
	if err != nil {
		panic(err)
	}
	value := indexer.MakeValue(p, proto, []byte(mhs[0]))

	mhs, _ = test.RandomMultihashes(10240)

	b.Run("Put single", func(b *testing.B) {
		s := New(8192)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err = s.Put(mhs[i%len(mhs)], value)
			if err != nil {
				panic(err)
			}
		}
	})

	for testCount := 1024; testCount < len(mhs); testCount *= 2 {
		b.Run(fmt.Sprint("Put", testCount), func(b *testing.B) {
			s := New(8192)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < testCount; j++ {
					_, err = s.Put(mhs[j], value)
					if err != nil {
						panic(err)
					}
				}
			}
		})

		b.Run(fmt.Sprint("PutMany", testCount), func(b *testing.B) {
			s := New(8192)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err = s.PutMany(mhs[:testCount], value)
				if err != nil {
					panic(err)
				}
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	mhs, err := test.RandomMultihashes(1)
	if err != nil {
		panic(err)
	}
	value := indexer.MakeValue(p, proto, []byte(mhs[0]))

	s := New(8192)
	mhs, _ = test.RandomMultihashes(4096)
	s.PutManyCount(mhs, value)

	b.Run("Get single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, ok, _ := s.Get(mhs[i%len(mhs)])
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
					_, ok, _ := s.Get(mhs[j%len(mhs)])
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
