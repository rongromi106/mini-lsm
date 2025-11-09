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

func TestTableIter_CrossBlockIterationAndSeek(t *testing.T) {
	// Create temp file and a tiny block size to force multiple blocks.
	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "SST-*.sst")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	opts := Options{BlockSize: 64} // small to create multiple blocks
	tw, err := NewTableWriter(f, opts)
	if err != nil {
		f.Close()
		t.Fatalf("NewTableWriter: %v", err)
	}

	// Write enough ordered entries to span multiple blocks.
	// Keys: a1..a4, b1..b4 with decreasing seq per key root.
	type kv struct{ k, v string }
	var data []kv
	for _, root := range []string{"a", "b", "c"} {
		for i := 4; i >= 1; i-- {
			data = append(data, kv{root, root + string(rune('0'+i))})
		}
	}
	seq := uint64(1)
	for _, kv := range data {
		// InternalKey ordering requirement: userKey asc overall, and for same userKey seq desc.
		// We will group by root so this holds: same root inserted with decreasing seq.
		// Across roots, ascending: a < b < c.
		ik := InternalKey{UserKey: []byte(kv.k), Seq: ^uint64(0) - seq, Kind: KindPut}
		if err := tw.Add(ik, []byte(kv.v)); err != nil {
			t.Fatalf("Add: %v", err)
		}
		seq++
	}
	if _, err := tw.Finish(); err != nil {
		_ = tw.Close()
		t.Fatalf("Finish: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen for reading
	rf, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("Open for read: %v", err)
	}
	tr, err := OpenTable(rf, Options{Compression: "none"})
	if err != nil {
		_ = rf.Close()
		t.Fatalf("OpenTable: %v", err)
	}
	defer tr.Close()

	// Table-wide iterator starting at First should yield ascending keys across blocks.
	it := &tableIter{tr: tr}
	it.First()
	if !it.Valid() {
		t.Fatalf("iterator invalid at First()")
	}
	// Collect first few keys to ensure it advances across blocks (at least covers 'a' then 'b')
	var seen []string
	for i := 0; i < 6 && it.Valid(); i++ {
		seen = append(seen, string(it.Key()))
		it.Next()
	}
	if len(seen) == 0 {
		t.Fatalf("no keys produced")
	}
	// Expect non-decreasing by userKey (since within same userKey multiple versions may appear)
	for i := 1; i < len(seen); i++ {
		if seen[i] < seen[i-1] {
			t.Fatalf("iterator order violated: %q then %q", seen[i-1], seen[i])
		}
	}

	// Test Seek into the middle, e.g., seek to "b"
	it.Seek([]byte("b"))
	if !it.Valid() {
		t.Fatalf("iterator invalid after Seek(b)")
	}
	if string(it.Key()) < "b" {
		t.Fatalf("Seek(b) positioned before 'b': got %q", string(it.Key()))
	}
}
