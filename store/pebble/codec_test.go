package pebble

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/ipni/go-indexer-core"
)

func TestCodec_MarshalledValueKeyLength(t *testing.T) {
	p := newPool()
	bk := p.leaseBlake3Keyer()
	vk := indexer.NewKeyer()
	k, err := vk.Key(value1)
	if err != nil {
		t.Fatal(err)
	}

	subject := newCodec(p)
	valueKey := bk.valueKey(k, false)
	gotKyes, _, err := subject.marshalValueKeys([][]byte{valueKey.buf})
	if err != nil {
		t.Fatal(err)
	}
	if len(gotKyes) != subject.valKeyLen {
		t.Fatal()
	}
}

func TestCodec_ValueKeysMarshalling(t *testing.T) {
	p := newPool()
	bk := p.leaseBlake3Keyer()
	subject := newCodec(p)
	vk := indexer.NewKeyer()

	k, err := vk.Key(value1)
	if err != nil {
		t.Fatal(err)
	}
	vk1 := bk.valueKey(k, false)

	k, err = vk.Key(value2)
	if err != nil {
		t.Fatal(err)
	}
	vk2 := bk.valueKey(k, false)

	k, err = vk.Key(value3)
	if err != nil {
		t.Fatal(err)
	}
	vk3 := bk.valueKey(k, false)

	vks := [][]byte{vk1.buf, vk2.buf, vk3.buf}
	gotKeys, _, err := subject.marshalValueKeys(vks)
	if err != nil {
		t.Fatal(err)
	}

	gotKeyBinCodec, err := indexer.BinaryValueCodec{}.MarshalValueKeys(vks)
	if err != nil {
		t.Fatal()
	}
	if !bytes.Equal(gotKeys, gotKeyBinCodec) {
		t.Fatal()
	}

	gotKeyList, err := subject.unmarshalValueKeys(gotKeys)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotKeyList.keys) != 3 {
		t.Fatal()
	}
	if !bytes.Equal(gotKeyList.keys[0].buf, vk1.buf) {
		t.Fatal()
	}
	if !bytes.Equal(gotKeyList.keys[1].buf, vk2.buf) {
		t.Fatal()
	}
	if !bytes.Equal(gotKeyList.keys[2].buf, vk3.buf) {
		t.Fatal()
	}
}

func TestCodec_ValueMarshalling(t *testing.T) {
	binCodec := indexer.BinaryValueCodec{}
	subject := newCodec(newPool())
	tests := []struct {
		name  string
		value *indexer.Value
	}{
		{
			"value1",
			value1,
		},
		{
			"value2",
			value2,
		},
		{
			"value3",
			value3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotMarshalled, _, err := subject.marshalValue(test.value)
			if err != nil {
				t.Fatal(err)
			}
			gotValue, err := subject.unmarshalValue(gotMarshalled)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.value, gotValue) {
				t.Fatal()
			}
			gotMarshalledBinCodec, err := binCodec.MarshalValue(*test.value)
			if err != nil {
				t.Fatal()
			}
			if !bytes.Equal(gotMarshalled, gotMarshalledBinCodec) {
				t.Fatal()
			}
		})
	}
}
