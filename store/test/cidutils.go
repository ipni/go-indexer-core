package test

import (
	"bufio"
	"io"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var prefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1, // default length
}

// RandomCids generates the specified number of random cids.
func RandomCids(n int) ([]cid.Cid, error) {
	var prng = rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		res[i] = c
	}
	return res, nil
}

// ReadCids reads cids from an io.Reader and outputs them on a channel.
// Malformed cids are ignored.
func ReadCids(in io.Reader, out chan cid.Cid, done chan error) {
	defer close(out)
	defer close(done)

	r := bufio.NewReader(in)
	var line []byte
	var err error
	for {
		line, err = r.ReadBytes('\n')
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
		out <- c
	}
}
