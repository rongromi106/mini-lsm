package lsm

import (
	"bytes"
	"testing"
)

func TestMemTablePutUpdatesSizeAndEntries(t *testing.T) {
	m := newMemTable()

	key := []byte("a")
	val := []byte("v1")
	seq := uint64(100)

	if got := m.NumEntries(); got != 0 {
		t.Fatalf("NumEntries before put = %d, want 0", got)
	}
	baseSize := m.ApproxSize()

	if err := m.Put(key, val, seq); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	// Check counters
	if got := m.NumEntries(); got != 1 {
		t.Fatalf("NumEntries after first put = %d, want 1", got)
	}
	wantInc := int64(len(key)) + int64(len(val)) + memEntryOverhead
	if got := m.ApproxSize(); got != baseSize+wantInc {
		t.Fatalf("ApproxSize = %d, want %d", got, baseSize+wantInc)
	}

	// Validate presence in underlying skiplist
	e := m.list.Find(internalOrdKey{userKey: key, seq: seq})
	if e == nil {
		t.Fatalf("skiplist.Find returned nil for inserted key/seq")
	}
	ev := e.Value.(entryVal)
	if ev.kind != KindPut {
		t.Fatalf("entry kind = %d, want KindPut", ev.kind)
	}
	if !bytes.Equal(ev.value, val) {
		t.Fatalf("entry value = %q, want %q", ev.value, val)
	}

	// Insert another version of same key and verify counters accumulate.
	val2 := []byte("v2")
	seq2 := uint64(90)
	prevSize := m.ApproxSize()
	if err := m.Put(key, val2, seq2); err != nil {
		t.Fatalf("second Put returned error: %v", err)
	}
	if got := m.NumEntries(); got != 2 {
		t.Fatalf("NumEntries after second put = %d, want 2", got)
	}
	if got := m.ApproxSize(); got != prevSize+int64(len(key))+int64(len(val2))+memEntryOverhead {
		t.Fatalf("ApproxSize after second put = %d, want %d", got, prevSize+int64(len(key))+int64(len(val2))+memEntryOverhead)
	}
}

func TestMemTableDeleteAndGetVisibility(t *testing.T) {
	m := newMemTable()

	key := []byte("k")
	v1 := []byte("v1")
	baseSize := m.ApproxSize()

	if err := m.Put(key, v1, 100); err != nil {
		t.Fatalf("Put v1 err: %v", err)
	}
	if err := m.Delete(key, 110); err != nil {
		t.Fatalf("Delete err: %v", err)
	}

	if got := m.NumEntries(); got != 2 {
		t.Fatalf("NumEntries = %d, want 2", got)
	}
	wantSize := baseSize + int64(len(key)) + int64(len(v1)) + memEntryOverhead + int64(len(key)) + memEntryOverhead
	if m.ApproxSize() != wantSize {
		t.Fatalf("ApproxSize = %d, want %d", m.ApproxSize(), wantSize)
	}

	// Newest <= 200 is tombstone => not found
	if val, ok, err := m.Get(key, 200); err != nil || ok || val != nil {
		t.Fatalf("Get(k,200) = (%q,%v,%v), want (nil,false,nil)", val, ok, err)
	}

	// With seqLimit before tombstone, should see v1
	if val, ok, err := m.Get(key, 105); err != nil || !ok || !bytes.Equal(val, v1) {
		t.Fatalf("Get(k,105) = (%q,%v,%v), want (%q,true,nil)", val, ok, err, v1)
	}
}

func TestMemTableGetVersionSelection(t *testing.T) {
	m := newMemTable()
	key := []byte("k2")

	if err := m.Put(key, []byte("v1"), 100); err != nil {
		t.Fatal(err)
	}
	if err := m.Put(key, []byte("v2"), 200); err != nil {
		t.Fatal(err)
	}

	if val, ok, err := m.Get(key, 50); err != nil || ok || val != nil {
		t.Fatalf("Get(k2,50) = (%q,%v,%v), want (nil,false,nil)", val, ok, err)
	}
	if val, ok, err := m.Get(key, 150); err != nil || !ok || !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("Get(k2,150) = (%q,%v,%v), want (v1,true,nil)", val, ok, err)
	}
	if val, ok, err := m.Get(key, 200); err != nil || !ok || !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("Get(k2,200) = (%q,%v,%v), want (v2,true,nil)", val, ok, err)
	}

	// Add tombstone and verify visibility semantics
	if err := m.Delete(key, 250); err != nil {
		t.Fatal(err)
	}
	if val, ok, err := m.Get(key, 300); err != nil || ok || val != nil {
		t.Fatalf("Get(k2,300) after del = (%q,%v,%v), want (nil,false,nil)", val, ok, err)
	}
	if val, ok, err := m.Get(key, 225); err != nil || !ok || !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("Get(k2,225) after del = (%q,%v,%v), want (v2,true,nil)", val, ok, err)
	}
}
