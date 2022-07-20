package test

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

func E2ETest(t *testing.T, s indexer.Interface) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := RandomMultihashes(15)

	ctxid1 := []byte(mhs[0])
	metadata1 := []byte("test-meta-1")
	ctxid2 := []byte(mhs[1])
	metadata2 := []byte("test-meta-2")

	value1 := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxid1,
		MetadataBytes: metadata1,
	}
	value2 := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxid2,
		MetadataBytes: metadata2,
	}

	single := mhs[2]
	noadd := mhs[3]
	batch := mhs[4:]
	remove := mhs[4]

	// Check for err when putting a multihash with nil metadata
	t.Log("Put bad value")
	badValue := indexer.Value{
		ProviderID: p,
		ContextID:  ctxid1,
	}
	err = s.Put(badValue, single)
	if err == nil {
		t.Fatal("expected error putting value missing metadata")
	}

	// Put a single multihash
	t.Log("Put/Get a single multihash")
	err = s.Put(value1, single)
	if err != nil {
		t.Fatalf("Error putting single multihash: %s", err)
	}

	// Put same value again.
	t.Log("Put/Get single multihash again")
	err = s.Put(value1, single)
	if err != nil {
		t.Fatalf("Error putting single multihash again: %s", err)
	}

	vals, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding single multihash")
	}
	if !vals[0].Equal(value1) {
		t.Error("Got wrong value for single multihash")
	}

	// Put a batch of multihashes
	t.Log("Put/Get a batch of multihashes")
	err = s.Put(value1, batch...)
	if err != nil {
		t.Fatalf("Error putting batch of multihashes: %s", err)
	}

	vals, found, err = s.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a multihash from the batch")
	}
	if !vals[0].Equal(value1) {
		t.Error("Got wrong value for single multihash")
	}

	// Put on an existing key
	t.Log("Put/Get on existing key")
	err = s.Put(value2, single)
	if err != nil {
		t.Fatalf("Error putting single multihash: %s", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	vals, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a multihash from the batch")
	}
	if len(vals) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if !vals[1].Equal(value2) {
		t.Error("Got wrong value for single multihash")
	}

	// Iterate values
	t.Log("Itertaing values")
	var indexCount int
	seen := make(map[string]struct{})
	iter, err := s.Iter()
	if err != nil {
		t.Fatal(err)
	}

	for {
		m, _, err := iter.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Iteration error: %s", err)
		}

		mb58 := m.B58String()
		t.Logf("Visited: %s", mb58)
		_, already := seen[mb58]
		if already {
			t.Errorf("Error: multihash already seen: %q", mb58)
		} else {
			seen[mb58] = struct{}{}
		}
		indexCount++
	}
	t.Logf("Visited %d multihashes", indexCount)
	if indexCount != len(batch)+1 {
		t.Errorf("Wrong iteration count: expected %d, got %d", len(batch)+1, indexCount)
	}
	for i := range batch {
		b58 := batch[i].B58String()
		_, ok := seen[b58]
		if !ok {
			t.Fatalf("Did not iterate multihash %s", b58)
		}
	}

	_, _, err = iter.Next()
	if err != io.EOF {
		t.Fatal("caling iter.Next() after iteration finished should yield same result")
	}

	// Get a key that is not set
	t.Log("Get non-existing key")
	_, found, err = s.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("Error, the key for the multihash should not be set")
	}

	// Check that a v1 CID hash can be stored.
	c, err := cid.Decode("baguqeeqqskyz3yh4jxnsdj57v5blazexyy")
	if err != nil {
		t.Fatal(err)
	}
	v1mh := c.Hash()
	err = s.Put(value2, v1mh)
	if err != nil {
		t.Fatal(err)
	}

	vals, found, err = s.Get(v1mh)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Error finding single multihash from v1 CID")
	}
	if !vals[0].Equal(value2) {
		t.Error("Got wrong value for single multihash from v1 CID")
	}

	// Update a value's metadata
	metadata3 := []byte("test-meta-3")
	value1a := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxid1,
		MetadataBytes: metadata3,
	}
	err = s.Put(value1a, v1mh)
	if err != nil {
		t.Fatal(err)
	}
	// Getrieve value using different multihash
	vals, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding single multihash")
	}
	if !vals[0].Equal(value1a) {
		t.Error("Expected updated value")
	}

	// Remove a key
	t.Log("Remove key")
	err = s.Remove(value1, remove)
	if err != nil {
		t.Fatalf("Error putting single multihash: %s", err)
	}

	_, found, err = s.Get(remove)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("multihash should have been removed")
	}

	// Remove a value from the key
	err = s.Remove(value1, single)
	if err != nil {
		t.Fatalf("Error putting single multihash: %s", err)
	}
	vals, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("multihash should still have one value")
	}
	if len(vals) != 1 {
		t.Error("wrong number of values after remove")
	}

}

func SizeTest(t *testing.T, s indexer.Interface) {
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := RandomMultihashes(151)

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("test-metadata"),
	}
	for _, c := range mhs[1:] {
		err = s.Put(value, c)
		if err != nil {
			t.Fatal(err)
		}
	}

	size, err := s.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size == int64(0) {
		t.Error("failed to compute storage size")
	}
}

func RemoveTest(t *testing.T, s indexer.Interface) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	mhs := RandomMultihashes(15)

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     []byte(mhs[0]),
		MetadataBytes: []byte("test-metadata"),
	}
	batch := mhs[1:]

	// Put a batch of multihashes
	t.Log("Put/Get a batch of multihashes")
	err = s.Put(value, batch...)
	if err != nil {
		t.Fatal("Error putting batch of multihashes:", err)
	}

	// Put a single multihash
	t.Log("Remove key")
	err = s.Remove(value, mhs[2:]...)
	if err != nil {
		t.Fatal("Error removing single multihash:", err)
	}

	i, found, err := s.Get(mhs[1])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("multihash should not have been removed")
	}
	if len(i) != 1 {
		t.Error("wrong number of multihashes removed")
	}
}

func RemoveProviderContextTest(t *testing.T, s indexer.Interface) {
	// Create new valid peer.ID
	prov1, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}
	prov2, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	if err != nil {
		t.Fatal(err)
	}

	mhs := RandomMultihashes(2)

	ctx1id := []byte(mhs[0])
	ctx2id := []byte(mhs[1])
	value1 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx1-metadata"),
	}
	value2 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx2id,
		MetadataBytes: []byte("ctx2-metadata"),
	}
	value3 := indexer.Value{
		ProviderID:    prov2,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx3-metadata"),
	}

	mhs = RandomMultihashes(15)

	batch1 := mhs[:5]
	batch2 := mhs[5:10]
	batch3 := mhs[10:15]

	// Put a batches of multihashes
	t.Log("Put batch1 value (provider1 context1)")
	if err = s.Put(value1, batch1...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch2 values (provider1 context1), (provider1 context2)")
	if err = s.Put(value1, batch2...); err != nil {
		t.Fatal(err)
	}
	if err = s.Put(value2, batch2...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch3 values (provider1 context2), (provider2 context1)")
	if err = s.Put(value2, batch3...); err != nil {
		t.Fatal(err)
	}
	if err = s.Put(value3, batch3...); err != nil {
		t.Fatal(err)
	}

	// Verify starting with correct values
	vals, found, err := s.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatalf("wrong number of multihashes, expected 1 got %d", len(vals))
	}
	vals, found, err = s.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 2 {
		t.Fatalf("wrong number of multihashes, expected 2 got %d", len(vals))
	}
	vals, found, err = s.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 2 {
		t.Fatalf("wrong number of multihashes, expected 2 got %d", len(vals))
	}

	t.Log("Removing provider1 context1")
	if err = s.RemoveProviderContext(prov1, ctx1id); err != nil {
		t.Fatalf("Error removing provider context: %s", err)
	}
	_, found, err = s.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	_, found, err = s.Get(mhs[1])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	vals, found, err = s.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatalf("wrong number of multihashes removed for bathc2, expected 2 got %d", len(vals))
	}
	if !vals[0].Equal(value2) {
		t.Fatal("Wrong value removed")
	}
	vals, found, err = s.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 2 {
		t.Fatalf("wrong number of multihashes removed for batch3, expected 2 got %d", len(vals))
	}

	t.Log("Removing provider1 context2")
	if err = s.RemoveProviderContext(prov1, ctx2id); err != nil {
		t.Fatalf("Error removing provider context: %s", err)
	}
	_, found, err = s.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	vals, found, err = s.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatal("wrong number of multihashes removed")
	}
	if !vals[0].Equal(value3) {
		t.Fatal("Wrong value removed")
	}

	t.Log("Removing provider2 context1")
	if err = s.RemoveProviderContext(prov2, ctx1id); err != nil {
		t.Fatalf("Error removing provider context: %s", err)
	}
	_, found, err = s.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should not have been found")
	}
}

func RemoveProviderTest(t *testing.T, s indexer.Interface) {
	// Create new valid peer.ID
	prov1, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}
	prov2, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	if err != nil {
		t.Fatal(err)
	}

	ctx1id := []byte("ctxid-1")
	ctx2id := []byte("ctxid-2")
	value1 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx1-metadata"),
	}
	value2 := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctx2id,
		MetadataBytes: []byte("ctx2-metadata"),
	}
	value3 := indexer.Value{
		ProviderID:    prov2,
		ContextID:     ctx1id,
		MetadataBytes: []byte("ctx3-metadata"),
	}

	mhs := RandomMultihashes(15)

	batch1 := mhs[:5]
	batch2 := mhs[5:10]
	batch3 := mhs[10:15]

	// Put a batches of multihashes
	t.Log("Put batch1 value (provider1 context1)")
	if err = s.Put(value1, batch1...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch2 values (provider1 context1), (provider1 context2)")
	if err = s.Put(value1, batch2...); err != nil {
		t.Fatal(err)
	}
	if err = s.Put(value2, batch2...); err != nil {
		t.Fatal(err)
	}
	t.Log("Put batch3 values (provider1 context2), (provider2 context1)")
	if err = s.Put(value2, batch3...); err != nil {
		t.Fatal(err)
	}
	if err = s.Put(value3, batch3...); err != nil {
		t.Fatal(err)
	}

	t.Log("Removing provider1")
	if err = s.RemoveProvider(context.Background(), prov1); err != nil {
		t.Fatalf("Error removing provider: %s", err)
	}
	_, found, err := s.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	_, found, err = s.Get(mhs[1])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	_, found, err = s.Get(mhs[5])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
	vals, found, err := s.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("multihash should have been found")
	}
	if len(vals) != 1 {
		t.Fatalf("wrong number of values removed for batch3, expected 1 got %d", len(vals))
	}

	t.Log("Removing provider2")
	if err = s.RemoveProvider(context.Background(), prov2); err != nil {
		t.Fatalf("Error removing provider: %s", err)
	}
	_, found, err = s.Get(mhs[10])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("multihash should have been removed")
	}
}

func ParallelUpdateTest(t *testing.T, s indexer.Interface) {
	mhs := RandomMultihashes(15)

	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	single := mhs[14]
	metadata := []byte("test-metadata")

	var wg sync.WaitGroup

	// Test parallel writes over same multihash
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Log("Put/Get different multihash")
			value := indexer.Value{
				ProviderID:    p,
				ContextID:     []byte(mhs[i]),
				MetadataBytes: metadata,
			}
			if err := s.Put(value, single); err != nil {
				t.Error("Error putting single multihash:", err)
			}
			wg.Done()
		}(&wg, i)
	}
	wg.Wait()
	x, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding single multihash")
	}
	if len(x) != 5 {
		t.Error("Value has not been updated by routines correctly", len(x))
	}

	// Test remove for all except one
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			t.Log("Remove multihash")
			value := indexer.Value{
				ProviderID:    p,
				ContextID:     []byte(mhs[i]),
				MetadataBytes: metadata,
			}
			if err := s.Remove(value, single); err != nil {
				t.Error("Error removing single multihash:", err)
			}
			wg.Done()
		}(&wg, i)
	}
	wg.Wait()
	x, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding single multihash")
	}
	if len(x) != 1 {
		t.Error("Value has not been removed by routines correctly", len(x))
	}
}
