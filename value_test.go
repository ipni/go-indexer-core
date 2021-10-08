package indexer

import (
	"bytes"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
)

const testProtoID = 999

var p1 peer.ID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
var p2 peer.ID = "12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV"
var testData = []byte("some data")
var testCtxID = []byte("QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB")

func TestPutGetData(t *testing.T) {
	var value Value

	value.PutData(testProtoID, testData)
	if len(value.MetadataBytes) == 0 {
		t.Fatal("did not encode metadata")
	}

	protoID, decodedData, err := value.GetData()
	if err != nil {
		t.Fatal(err)
	}
	if protoID != testProtoID {
		t.Fatal("got wrong protocol ID")
	}
	if !bytes.Equal(decodedData, testData) {
		t.Fatal("did not get expected data")
	}
}

func TestEqual(t *testing.T) {
	value1 := MakeValue(p1, testCtxID, testProtoID, testData)
	value2 := MakeValue(p1, testCtxID, testProtoID, testData)
	if !value1.Equal(value2) {
		t.Fatal("values are not equal")
	}
	if !value1.Match(value1) {
		t.Fatal("values do not match")
	}

	// Changing provider ID should make values unequal
	value2 = MakeValue(p2, testCtxID, testProtoID, testData)
	if value1.Equal(value2) {
		t.Fatal("values are equal")
	}
	// Changing provider ID should make values not match
	if value1.Match(value2) {
		t.Fatal("values match")
	}

	// Changing context ID should make values unequal
	value2 = MakeValue(p1, []byte("some-context-id"), testProtoID, testData)
	if value1.Equal(value2) {
		t.Fatal("values are equal")
	}
	// Changing context ID should make values not match
	if value1.Match(value2) {
		t.Fatal("values match")
	}

	// Changing protocol ID should make values unequal
	value2 = MakeValue(p1, testCtxID, testProtoID+1, testData)
	if value1.Equal(value2) {
		t.Fatal("values are equal")
	}
	// Changing protocol ID should not affect match
	if !value1.Match(value2) {
		t.Fatal("values do not match")
	}

	// Changing metadata should make values unequal
	value2 = MakeValue(p1, testCtxID, testProtoID, []byte("some dataX"))
	if value1.Equal(value2) {
		t.Fatal("values are equal")
	}
	// Changing metadata should not affect match
	if !value1.Match(value2) {
		t.Fatal("values do not match")
	}
}
