// Package dhstore forks the HTTP request models of dhtsotre in order to
// avoid forcing upstream projects into C-bindings required by Foundation DB.
// Note that this repo only needs the Write request models as it only ever
// persists data to dhstore. It does not look up records from it.
//
// The structs here are compatible with the API specification of dhstore.
// See: https://github.com/ipni/dhstore/blob/main/openapi.yaml
package dhstore

import (
	"github.com/multiformats/go-multihash"
)

type (
	Index struct {
		Key   multihash.Multihash `json:"key"`
		Value []byte              `json:"value"`
	}
	MergeIndexRequest struct {
		Merges []Index `json:"merges"`
	}
	PutMetadataRequest struct {
		Key   []byte `json:"key"`
		Value []byte `json:"value"`
	}
)
