package lsm

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestTableWriter_Basic(t *testing.T) {
	// Create temp file for SSTable
	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "SST-*.sst")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}

	opts := Options{BlockSize: 64, BloomFpRate: 0}
	tw, err := NewTableWriter(f, opts)
	if err != nil {
		f.Close()
		t.Fatalf("NewTableWriter: %v", err)
	}

	// Write a few ordered InternalKey entries (userKey asc, seq desc per key)
	entries := []struct {
		k InternalKey
		v []byte
	}{
		{InternalKey{UserKey: []byte("a"), Seq: 5, Kind: KindPut}, []byte("va5")},
		{InternalKey{UserKey: []byte("a"), Seq: 4, Kind: KindPut}, []byte("va4")},
		{InternalKey{UserKey: []byte("b"), Seq: 7, Kind: KindPut}, []byte("vb7")},
	}
	for _, e := range entries {
		if err := tw.Add(e.k, e.v); err != nil {
			t.Fatalf("Add(%v): %v", e.k, err)
		}
	}

	footer, err := tw.Finish()
	if err != nil {
		_ = tw.Close()
		t.Fatalf("Finish: %v", err)
	}

	// Footer sanity checks
	if footer.Magic != sstMagic {
		_ = tw.Close()
		t.Fatalf("footer magic mismatch: got %x want %x", footer.Magic, sstMagic)
	}
	// Index/filter handles should have non-zero length for this build
	if footer.IndexHandle.Length == 0 {
		_ = tw.Close()
		t.Fatalf("index handle length is zero")
	}
	if footer.FilterHandle.Length == 0 {
		_ = tw.Close()
		t.Fatalf("filter handle length is zero")
	}

	// Read back last 8 bytes of the file and verify magic
	st, err := f.Stat()
	if err != nil {
		_ = tw.Close()
		t.Fatalf("Stat: %v", err)
	}
	if st.Size() < 44 { // footer size in this skeleton
		_ = tw.Close()
		t.Fatalf("file too small: %d", st.Size())
	}
	buf := make([]byte, 8)
	if _, err := f.ReadAt(buf, st.Size()-8); err != nil {
		_ = tw.Close()
		t.Fatalf("ReadAt magic: %v", err)
	}
	magic := binary.LittleEndian.Uint64(buf)
	if magic != sstMagic {
		_ = tw.Close()
		t.Fatalf("trailer magic mismatch: got %x want %x", magic, sstMagic)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
