package indexer

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
)

var p1 peer.ID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
var p2 peer.ID = "12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV"
var testCtxID = []byte("QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB")

func TestEqual(t *testing.T) {
	metaBytes := []byte("dummy-metadata")

	value1 := Value{p1, testCtxID, metaBytes}
	value2 := Value{p1, testCtxID, metaBytes}
	if !value1.Equal(value2) {
		t.Fatal("values are not equal")
	}
	if !value1.Match(value1) {
		t.Fatal("values do not match")
	}

	// Changing provider ID should make values unequal
	value2 = Value{p2, testCtxID, metaBytes}
	if value1.Equal(value2) {
		t.Fatal("values are equal")
	}
	// Changing provider ID should make values not match
	if value1.Match(value2) {
		t.Fatal("values match")
	}

	// Changing context ID should make values unequal
	value2 = Value{p1, []byte("some-context-id"), metaBytes}
	if value1.Equal(value2) {
		t.Fatal("values are equal")
	}
	// Changing context ID should make values not match
	if value1.Match(value2) {
		t.Fatal("values match")
	}

	// Changing metadata should make values unequal
	value2 = Value{p1, testCtxID, []byte("other-metadata")}
	if value1.Equal(value2) {
		t.Fatal("values are equal")
	}
	// Changing metadata should not affect match
	if !value1.Match(value2) {
		t.Fatal("values do not match")
	}
}
