package bench

import (
	"math/rand"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type (
	// GeneratorConfig captures the configuration to use for generating values for testing and
	// benchmarking purposes.
	GeneratorConfig struct {
		// NumProviders sets the number of unique providers to generate. Defaults to 1 if unset.
		NumProviders int
		// NumValuesPerProvider is the function that is called to determine the number of values
		// to generate for each provider. Defaults to 10 if unset.
		NumValuesPerProvider func() uint64
		// NumEntriesPerValue is the function that is called to determine the number of multihash
		// entries to generate for each value. Defaults to 100 if unset.
		NumEntriesPerValue func() uint64
		// DuplicateEntries is the function that is called prior to generating multihash entries,
		// in order to determine weather to reuse already generated entries from another provider.
		// This function effectively allows the user to set the probability of providers providing
		// the same content in the network.
		// Note that same entries are never duplicated for the same provider.
		// Defaults to no duplicates if unset.
		DuplicateEntries func() bool
		// MultihashLength is the function that is called to determine the multihash length to
		// generate for each entry. Defaults to 32 if unset.
		MultihashLength func() uint64
		// ContextIDLength is the function that is called to determine the context ID length to
		// generate for each value. Defaults to 32 if unset.
		ContextIDLength func() uint64
		// MetadataLength is the function that is called to determine the metadata length to
		// generate for each value. Defaults to 32 if unset.
		MetadataLength func() uint64
		// ShuffleValues specifies weather to shuffle the generated values. Defaults to unshuffled.
		ShuffleValues bool
	}
	// GeneratedValue captures a synthetically created indexer.Value along with the entries
	// associated to it.
	GeneratedValue struct {
		indexer.Value
		Entries []multihash.Multihash
	}
)

func generate(rng *rand.Rand, l uint64) []byte {
	buf := make([]byte, l)
	rng.Read(buf)
	return buf
}

func (gc GeneratorConfig) withDefaults() GeneratorConfig {
	if gc.NumProviders == 0 {
		gc.NumProviders = 1
	}
	if gc.NumValuesPerProvider == nil {
		gc.NumValuesPerProvider = func() uint64 { return 10 }
	}
	if gc.NumEntriesPerValue == nil {
		gc.NumEntriesPerValue = func() uint64 { return 100 }
	}
	if gc.DuplicateEntries == nil {
		gc.DuplicateEntries = func() bool { return false }
	}
	len32 := func() uint64 { return 32 }
	if gc.MultihashLength == nil {
		gc.MultihashLength = len32
	}
	if gc.ContextIDLength == nil {
		gc.ContextIDLength = len32
	}
	if gc.MetadataLength == nil {
		gc.MetadataLength = len32
	}
	return gc
}

// GenerateRandomValues generates a set of random GeneratedValue and returns them along with their
// total size in bytes.
func GenerateRandomValues(b testing.TB, rng *rand.Rand, cfg GeneratorConfig) ([]GeneratedValue, int) {
	cfg = cfg.withDefaults()
	var gvs []GeneratedValue
	var totalSize int
	for i := 0; i < cfg.NumProviders; i++ {
		_, pub, err := crypto.GenerateEd25519Key(rng)
		if err != nil {
			b.Fatal(err)
		}
		provId, err := peer.IDFromPublicKey(pub)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < int(cfg.NumValuesPerProvider()); j++ {
			var gv GeneratedValue
			gv.Value.ProviderID = provId
			totalSize += provId.Size()

			gv.Value.ContextID = generate(rng, cfg.ContextIDLength())
			totalSize += len(gv.Value.ContextID)

			gv.Value.MetadataBytes = generate(rng, cfg.MetadataLength())
			totalSize += len(gv.Value.MetadataBytes)

			if i > 0 && cfg.DuplicateEntries() {
				pi := rng.Intn(len(gvs) - j)
				gv.Entries = gvs[pi].Entries
			}

			if len(gv.Entries) == 0 {
				mhCount := cfg.NumEntriesPerValue()
				gv.Entries = make([]multihash.Multihash, mhCount)
				var err error
				for i := 0; i < int(mhCount); i++ {
					gv.Entries[i], err = multihash.Sum(generate(rng, cfg.MultihashLength()), multihash.IDENTITY, -1)
					if err != nil {
						b.Fatal(err)
					}
					totalSize += len(gv.Entries[i])
				}
			}

			gvs = append(gvs, gv)
		}
	}

	if cfg.ShuffleValues {
		rng.Shuffle(len(gvs), func(one, other int) {
			gvs[one], gvs[other] = gvs[other], gvs[one]
		})
	}

	return gvs, totalSize
}
