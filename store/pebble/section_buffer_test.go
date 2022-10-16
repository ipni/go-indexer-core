package pebble

import (
	"bytes"
	"testing"
)

func TestSectionBuffer_WriteAndCopySection(t *testing.T) {
	p := newPool()
	subject := p.leaseSectionBuff()

	wantS1 := []byte("fish")
	subject.writeSection(wantS1)
	wantS2 := []byte("lobster")
	subject.writeSection(wantS2)
	wantS3 := []byte("barreleye")
	subject.writeSection(wantS3)

	s1, err := subject.copyNextSection()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantS1, s1) {
		t.Fatal()
	}
	s2, err := subject.copyNextSection()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantS2, s2) {
		t.Fatal()
	}
	s3, err := subject.copyNextSection()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantS3, s3) {
		t.Fatal()
	}

	if err := subject.Close(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(wantS1, s1) {
		t.Fatal()
	}
	if !bytes.Equal(wantS2, s2) {
		t.Fatal()
	}
	if !bytes.Equal(wantS3, s3) {
		t.Fatal()
	}
}
