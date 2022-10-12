package vsinfo

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/go-indexer-core"
)

const (
	fileName = "vstore.info"
	version  = 1
)

// VStoreInfo contains information about the valuestore.
type VStoreInfo struct {
	// Version is the version number of this file.
	Version int
	// Type is type of value store.
	Type string
	// Codec is the type of codec used for the value store.
	Codec string
}

func Load(dir string) (VStoreInfo, error) {
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		return VStoreInfo{}, err
	}

	var vsinfo VStoreInfo
	err = json.Unmarshal(data, &vsinfo)
	if err != nil {
		return VStoreInfo{}, err
	}

	return vsinfo, nil
}

func (v VStoreInfo) Save(dir string) error {
	v.Version = version
	data, err := json.Marshal(&v)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, fileName), data, 0o666)
}

func (v VStoreInfo) MakeCodec() (indexer.ValueCodec, error) {
	switch v.Codec {
	case "binary":
		return indexer.BinaryValueCodec{}, nil
	case "binaryjson":
		return indexer.BinaryWithJsonFallbackCodec{}, nil
	case "json":
		return indexer.JsonValueCodec{}, nil
	}
	return nil, fmt.Errorf("unsupported codec: %s", v.Codec)
}
