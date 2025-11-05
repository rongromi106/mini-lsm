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

func TestMemTableFreezeSwap(t *testing.T) {
	m := newMemTable()

	// Populate with a few versions and a tombstone
	if err := m.Put([]byte("a"), []byte("va1"), 10); err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("a"), []byte("va2"), 20); err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("b"), []byte("vb1"), 15); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete([]byte("c"), 18); err != nil {
		t.Fatal(err)
	}

	prevSize := m.ApproxSize()
	prevNum := m.NumEntries()

	im, err := m.Freeze()
	if err != nil {
		t.Fatalf("Freeze error: %v", err)
	}
	imm := im.(*immutableMemTable)

	// Immutable captures previous counters
	if imm.ApproxSize() != prevSize {
		t.Fatalf("immutable ApproxSize=%d, want %d", imm.ApproxSize(), prevSize)
	}
	if imm.NumEntries() != prevNum {
		t.Fatalf("immutable NumEntries=%d, want %d", imm.NumEntries(), prevNum)
	}

	// Mutable reset
	if m.ApproxSize() != 0 || m.NumEntries() != 0 {
		t.Fatalf("mutable not reset: size=%d num=%d", m.ApproxSize(), m.NumEntries())
	}

	// Data moved to immutable: look up known entries in immutable skiplist
	if e := imm.list.Find(internalOrdKey{userKey: []byte("a"), seq: 20}); e == nil {
		t.Fatalf("immutable missing a@20")
	}
	if e := imm.list.Find(internalOrdKey{userKey: []byte("b"), seq: 15}); e == nil {
		t.Fatalf("immutable missing b@15")
	}
	if e := imm.list.Find(internalOrdKey{userKey: []byte("c"), seq: 18}); e == nil {
		t.Fatalf("immutable missing c tombstone@18")
	}

	// Mutable should not have old data
	if e := m.list.Find(internalOrdKey{userKey: []byte("a"), seq: 20}); e != nil {
		t.Fatalf("mutable unexpectedly has a@20 after freeze")
	}

	// Writes after freeze go to new mutable and don't affect immutable
	if err := m.Put([]byte("d"), []byte("vd1"), 30); err != nil {
		t.Fatal(err)
	}
	if m.NumEntries() != 1 {
		t.Fatalf("mutable entries=%d, want 1", m.NumEntries())
	}
	if e := imm.list.Find(internalOrdKey{userKey: []byte("d"), seq: 30}); e != nil {
		t.Fatalf("immutable unexpectedly has d@30")
	}
}

func TestInternalIteratorOrderAndSeek(t *testing.T) {
	m := newMemTable()
	// a: two versions
	if err := m.Put([]byte("a"), []byte("va1"), 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Put([]byte("a"), []byte("va2"), 2); err != nil {
		t.Fatal(err)
	}
	// b: one put, then tombstone
	if err := m.Put([]byte("b"), []byte("vb1"), 3); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete([]byte("b"), 4); err != nil {
		t.Fatal(err)
	}

	im, err := m.Freeze()
	if err != nil {
		t.Fatalf("Freeze error: %v", err)
	}
	i := im.NewInternalIterator()

	// First -> a@2
	i.First()
	if !i.Valid() {
		t.Fatalf("iterator not valid at First")
	}
	ik := i.InternalKey()
	if string(ik.UserKey) != "a" || ik.Seq != 2 || ik.Kind != KindPut {
		t.Fatalf("First IK = (%s,%d,%d), want (a,2,KindPut)", string(ik.UserKey), ik.Seq, ik.Kind)
	}
	if !bytes.Equal(i.Value(), []byte("va2")) {
		t.Fatalf("First Value = %q, want %q", i.Value(), []byte("va2"))
	}

	// Next -> a@1
	i.Next()
	if !i.Valid() {
		t.Fatalf("iterator not valid at a@1")
	}
	ik = i.InternalKey()
	if string(ik.UserKey) != "a" || ik.Seq != 1 || ik.Kind != KindPut {
		t.Fatalf("Next IK = (%s,%d,%d), want (a,1,KindPut)", string(ik.UserKey), ik.Seq, ik.Kind)
	}

	// Next -> b@4 (DEL)
	i.Next()
	if !i.Valid() {
		t.Fatalf("iterator not valid at b@4")
	}
	ik = i.InternalKey()
	if string(ik.UserKey) != "b" || ik.Seq != 4 || ik.Kind != KindDel {
		t.Fatalf("Next IK = (%s,%d,%d), want (b,4,KindDel)", string(ik.UserKey), ik.Seq, ik.Kind)
	}

	// SeekInternal to b@3
	i.SeekInternal(InternalKey{UserKey: []byte("b"), Seq: 3})
	if !i.Valid() {
		t.Fatalf("iterator not valid at Seek b@3")
	}
	ik = i.InternalKey()
	if string(ik.UserKey) != "b" || ik.Seq != 3 || ik.Kind != KindPut {
		t.Fatalf("Seek IK = (%s,%d,%d), want (b,3,KindPut)", string(ik.UserKey), ik.Seq, ik.Kind)
	}

	// Next -> nil
	i.Next()
	if i.Valid() {
		t.Fatalf("iterator should be invalid after last element")
	}
}
