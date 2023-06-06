package vsinfo

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadNew(t *testing.T) {
	dir := t.TempDir()
	vsi, err := Load(dir, "somevalstore")
	if err != nil {
		t.Fatal(err)
	}

	if vsi.Codec != BinaryCodec {
		t.Fatal("new value store should have codec =", BinaryCodec)
	}

	_, err = Load(dir, "pebble")
	if err == nil {
		t.Fatal("ecpected error")
	}
}

func TestLoadExisting(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "somefile"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	vsi, err := Load(dir, "somevalstore")
	if err != nil {
		t.Fatal(err)
	}

	if vsi.Codec != BinaryJsonCodec {
		t.Fatal("new value store should have codec =", BinaryJsonCodec)
	}
}
