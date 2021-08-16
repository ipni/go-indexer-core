package indexer

import (
	"bytes"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
)

const testProtoID = 999

var p peer.ID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
var testData = []byte("some data")

func TestPutGetData(t *testing.T) {
	var value Value

	value.PutData(testProtoID, testData)
	if len(value.Metadata) == 0 {
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
	value1 := MakeValue(p, testProtoID, testData)
	value2 := MakeValue(p, testProtoID, testData)
	if !value1.Equal(value2) {
		t.Fatal("values are not equal")
	}

	value2 = MakeValue(p, testProtoID+1, testData)
	if value1.Equal(value2) {
		t.Fatal("values not equal")
	}

	value2 = MakeValue(p, testProtoID, []byte("some dataX"))
	if value1.Equal(value2) {
		t.Fatal("values not equal")
	}
}
