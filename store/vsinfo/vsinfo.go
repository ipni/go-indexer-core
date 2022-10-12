package vsinfo

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/filecoin-project/go-indexer-core"
)

const (
	fileName = "vstore.info"
	version  = 1

	BinaryCodec     = "binary"
	BinaryJsonCodec = "binaryjson"
	JsonCodec       = "json"
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

func Load(dir, vsType string) (VStoreInfo, error) {
	data, err := os.ReadFile(Path(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return initVStoreInfo(dir, vsType)
		}
		return VStoreInfo{}, err
	}

	var vsinfo VStoreInfo
	err = json.Unmarshal(data, &vsinfo)
	if err != nil {
		return VStoreInfo{}, err
	}

	if vsinfo.Type != vsType {
		return VStoreInfo{}, fmt.Errorf("value store of type %s already exists in directory %s", vsinfo.Type, dir)
	}

	return vsinfo, nil
}

func (v VStoreInfo) Save(dir string) error {
	v.Version = version
	data, err := json.Marshal(&v)
	if err != nil {
		return err
	}
	return os.WriteFile(Path(dir), data, 0o666)
}

func Path(dir string) string {
	return filepath.Join(dir, fileName)
}

func (v VStoreInfo) MakeCodec() (indexer.ValueCodec, error) {
	switch v.Codec {
	case BinaryCodec:
		return indexer.BinaryValueCodec{}, nil
	case BinaryJsonCodec:
		return indexer.BinaryWithJsonFallbackCodec{}, nil
	case JsonCodec:
		return indexer.JsonValueCodec{}, nil
	}
	return nil, fmt.Errorf("unsupported codec: %s", v.Codec)
}

func initVStoreInfo(dir, vstype string) (VStoreInfo, error) {
	empty, err := dirEmpty(dir)
	if err != nil {
		return VStoreInfo{}, err
	}

	vsInfo := VStoreInfo{
		Version: version,
		Type:    vstype,
	}
	// Use BinaryWithJsonFallbackCodec codec if the valuestore already exists
	// and was not created using binary codec.
	if empty {
		vsInfo.Codec = BinaryCodec
	} else {
		vsInfo.Codec = BinaryJsonCodec
	}
	if err = vsInfo.Save(dir); err != nil {
		return VStoreInfo{}, err
	}
	return vsInfo, nil
}

func dirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}
