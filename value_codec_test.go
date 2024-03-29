package indexer_test

import (
	"bytes"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/bench"
	"github.com/multiformats/go-varint"
)

func TestValueCodec_MarshalUnmarshal(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	wantValues, _ := bench.GenerateRandomValues(t, rng, bench.GeneratorConfig{})
	wantValueKeys := generateRandomValueKeys(43)

	tests := []struct {
		name    string
		subject indexer.ValueCodec
	}{
		{
			name:    "json",
			subject: indexer.JsonValueCodec{},
		},
		{
			name:    "binary",
			subject: indexer.BinaryValueCodec{},
		},
		{
			name:    "binary-json",
			subject: indexer.BinaryWithJsonFallbackCodec{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, wantValue := range wantValues {
				gotSer, err := test.subject.MarshalValue(wantValue.Value)
				if err != nil {
					t.Fatal(err)
				}
				gotValue, err := test.subject.UnmarshalValue(gotSer)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(wantValue.Value, gotValue) {
					t.Fatalf("value mismatch; wanted %v but got %v", wantValue.Value, gotValue)
				}
			}
			gotSer, err := test.subject.MarshalValueKeys(wantValueKeys)
			if err != nil {
				t.Fatal(err)
			}
			gotValueKeys, err := test.subject.UnmarshalValueKeys(gotSer)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(wantValueKeys, gotValueKeys) {
				t.Fatalf("valuekeys mismatch; wanted %v but got %v", wantValueKeys, gotValueKeys)
			}
		})
	}
}

func TestBinaryValueCodec_MarshalValueMalformedBytes(t *testing.T) {
	tests := []struct {
		name    string
		value   func(buf *bytes.Buffer)
		wantErr string
	}{
		{
			name: "large pid len",
			value: func(buf *bytes.Buffer) {
				buf.Write(varint.ToUvarint(1<<63 - 1))
			},
			wantErr: "overflow",
		},
		{
			name: "wrong context ID len",
			value: func(buf *bytes.Buffer) {
				pid := []byte("fish")
				buf.Write(varint.ToUvarint(uint64(len(pid))))
				buf.Write(pid)
				buf.Write(varint.ToUvarint(51))
				buf.WriteByte(2)
				buf.Write(varint.ToUvarint(1))
				buf.WriteByte(0)
			},
			wantErr: "overflow",
		},
		{
			name: "wrong metadata len",
			value: func(buf *bytes.Buffer) {
				pid := []byte("fish")
				buf.Write(varint.ToUvarint(uint64(len(pid))))
				buf.Write(pid)
				buf.Write(varint.ToUvarint(1))
				buf.WriteByte(2)
				buf.Write(varint.ToUvarint(41))
				buf.WriteByte(0)
			},
			wantErr: "overflow",
		},
		{
			name: "bytes leftover",
			value: func(buf *bytes.Buffer) {
				pid := []byte("fish")
				buf.Write(varint.ToUvarint(uint64(len(pid))))
				buf.Write(pid)
				ctxID := []byte("lobster")
				buf.Write(varint.ToUvarint(uint64(len(ctxID))))
				buf.Write(ctxID)
				md := []byte("barreleye")
				buf.Write(varint.ToUvarint(uint64(len(md))))
				buf.Write(md)
				buf.Write([]byte("undadasea"))
			},
			wantErr: "too many bytes",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			subject := indexer.BinaryValueCodec{}
			buf := bytes.Buffer{}
			test.value(&buf)
			_, err := subject.UnmarshalValue(buf.Bytes())
			if err == nil {
				t.Fatalf("expected error '%s' but got no error", test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("expected error '%s' but got: %v", test.wantErr, err)
			}
		})
	}
}

func TestBinaryValueCodec_MarshalUnmarshalEmptyValues(t *testing.T) {
	subject := indexer.BinaryValueCodec{}
	sv, err := subject.MarshalValue(indexer.Value{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sv, []byte{0, 0, 0}) {
		t.Fatal()
	}
	var emptyVK [][]byte
	svk, err := subject.MarshalValueKeys(emptyVK)
	if err != nil {
		t.Fatal(err)
	}
	if len(svk) != 0 {
		t.Fatal()
	}

	emptyVKWithEmptyK := [][]byte{{}}
	svk, err = subject.MarshalValueKeys(emptyVKWithEmptyK)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(svk, []byte{0}) {
		t.Fatal()
	}
}

func TestValueCodec_BinaryWithJsonUnmarshalFallsBackOnJson(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	wantGenValues, _ := bench.GenerateRandomValues(t, rng, bench.GeneratorConfig{})
	wantValueKeys := generateRandomValueKeys(43)

	subject := indexer.BinaryWithJsonFallbackCodec{}

	for _, wantGenValue := range wantGenValues {
		wantValue := wantGenValue.Value
		gotJson, err := indexer.JsonValueCodec{}.MarshalValue(wantValue)
		if err != nil {
			t.Fatal(err)
		}
		gotValue, err := subject.UnmarshalValue(gotJson)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(wantValue, gotValue) {
			t.Fatal()
		}
	}

	gotJson, err := indexer.JsonValueCodec{}.MarshalValueKeys(wantValueKeys)
	if err != nil {
		t.Fatal(err)
	}
	gotValueKeys, err := subject.UnmarshalValueKeys(gotJson)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantValueKeys, gotValueKeys) {
		t.Fatal()
	}
}

func TestValueCodec_BinaryWithJsonAlwaysMarshalsAsBinary(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	wantGenValues, _ := bench.GenerateRandomValues(t, rng, bench.GeneratorConfig{})
	wantValueKeys := generateRandomValueKeys(43)

	binCodec := indexer.BinaryValueCodec{}
	subject := indexer.BinaryWithJsonFallbackCodec{}

	for _, wantGenValue := range wantGenValues {
		wantValue := wantGenValue.Value
		wantBin, err := binCodec.MarshalValue(wantValue)
		if err != nil {
			t.Fatal(err)
		}
		gotBin, err := subject.MarshalValue(wantValue)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(wantBin, gotBin) {
			t.Fatal()
		}
	}

	wantBinVK, err := binCodec.MarshalValueKeys(wantValueKeys)
	if err != nil {
		t.Fatal(err)
	}
	gotBinVK, err := subject.MarshalValueKeys(wantValueKeys)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantBinVK, gotBinVK) {
		t.Fatal()
	}
}

func generateRandomValueKeys(count int) [][]byte {
	var vks [][]byte
	rng := rand.New(rand.NewSource(1413))
	for i := 0; i < count; i++ {
		vk := make([]byte, rng.Intn(127)+1)
		vks = append(vks, vk)
	}
	return vks
}
