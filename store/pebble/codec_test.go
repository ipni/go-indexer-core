package pebble

import (
	"slices"
	"sync"
	"testing"

	"github.com/ipni/go-indexer-core"
	"github.com/stretchr/testify/require"
)

func TestCodec_MarshalledValueKeyLength(t *testing.T) {
	p := newPool()
	bk := p.leaseBlake3Keyer()
	subject := codec{
		p: p,
	}
	valueKey, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	gotKeys, _, err := subject.marshalValueKeys([][]byte{valueKey.buf})
	require.NoError(t, err)
	require.Len(t, gotKeys, marshalledValueKeyLength)
}

func TestCodec_ValueKeysMarshalling(t *testing.T) {
	p := newPool()
	bk := p.leaseBlake3Keyer()
	subject := codec{
		p: p,
	}
	vk1, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	vk2, err := bk.valueKey(value2, false)
	require.NoError(t, err)
	vk3, err := bk.valueKey(value3, false)
	require.NoError(t, err)

	vks := [][]byte{vk1.buf, vk2.buf, vk3.buf}
	gotKeys, _, err := subject.marshalValueKeys(vks)
	require.NoError(t, err)

	gotKeyBinCodec, err := indexer.BinaryValueCodec{}.MarshalValueKeys(vks)
	require.NoError(t, err)
	require.Equal(t, gotKeyBinCodec, gotKeys)

	gotKeyList, err := subject.unmarshalValueKeys(gotKeys)
	require.NoError(t, err)
	require.Len(t, gotKeyList.keys, 3)
	require.Equal(t, vk1.buf, gotKeyList.keys[0].buf)
	require.Equal(t, vk2.buf, gotKeyList.keys[1].buf)
	require.Equal(t, vk3.buf, gotKeyList.keys[2].buf)
}

func TestCodec_ValueMarshalling(t *testing.T) {
	binCodec := indexer.BinaryValueCodec{}
	subject := codec{
		p: newPool(),
	}
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
			require.NoError(t, err)
			gotValue, err := subject.unmarshalValue(gotMarshalled)
			require.NoError(t, err)
			require.Equal(t, test.value, gotValue)
			gotMarshalledBinCodec, err := binCodec.MarshalValue(*test.value)
			require.NoError(t, err)
			require.Equal(t, gotMarshalledBinCodec, gotMarshalled)
		})
	}
}

// A packed value whose length is not a whole number of value keys is
// rejected without leasing a key list or keys from the pool.
func TestCodec_UnmarshalValueKeysOverflow(t *testing.T) {
	// Nothing is put back into these pools until the end of the test, so
	// each lease calls New
	p := newPool()
	leased := 0
	for _, sp := range []*sync.Pool{&p.keyPool, &p.keyListPool} {
		newFn := sp.New
		sp.New = func() any {
			leased++
			return newFn()
		}
	}

	bk := p.leaseBlake3Keyer()
	subject := codec{p: p}
	vk, err := bk.valueKey(value1, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, vk.Close()) })

	marshalled, closer, err := subject.marshalValueKeys([][]byte{vk.buf, vk.buf})
	require.NoError(t, err)
	packed := slices.Clone(marshalled)
	require.NoError(t, closer.Close())

	leased = 0
	kl, err := subject.unmarshalValueKeys(append(packed, 0))
	require.ErrorIs(t, err, indexer.ErrCodecOverflow)
	require.Nil(t, kl)
	require.Zero(t, leased)

	// A valid value leases one key list and one key per value key
	kl, err = subject.unmarshalValueKeys(packed)
	require.NoError(t, err)
	require.Equal(t, 3, leased)
	require.NoError(t, kl.Close())
}
