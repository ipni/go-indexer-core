package test

import (
	"bufio"
	"io"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

var prefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   multihash.SHA2_256,
	MhLength: -1, // default length
}

// RandomMultihashes generates the specified number of random cids.
func RandomMultihashes(n int) []multihash.Multihash {
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))

	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n+16)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err.Error)
		}
		mhashes[i] = c.Hash()
	}
	return mhashes
}

// ReadCids reads cids from an io.Reader and outputs their multihashes on a
// channel.  Malformed cids are ignored.
func ReadCids(in io.Reader, out chan multihash.Multihash, done chan error) {
	defer close(out)
	defer close(done)

	r := bufio.NewReader(in)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				done <- err
			}
			return
		}
		c, err := cid.Decode(string(line))
		if err != nil {
			// Disregarding malformed CIDs for now
			continue
		}
		out <- c.Hash()
	}
}
