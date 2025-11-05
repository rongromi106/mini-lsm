package lsm

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestBasicPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := db.Put(ctx, []byte("k1"), []byte("v1"), &WriteOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, ok, err := db.Get(ctx, []byte("k1"), &ReadOptions{})
	if err != nil || !ok || string(val) != "v1" {
		t.Fatalf("get mismatch: ok=%v err=%v val=%q", ok, err, string(val))
	}

	if err := db.Delete(ctx, []byte("k1"), &WriteOptions{}); err != nil {
		t.Fatalf("del: %v", err)
	}
	if _, ok, _ := db.Get(ctx, []byte("k1"), &ReadOptions{}); ok {
		t.Fatalf("expected tombstone not found")
	}
}

func BenchmarkPut(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Dir: dir, WALRollSize: 1 << 30, FsyncPolicy: "none"})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	val := []byte("value-xxxxxxxx")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		if err := db.Put(ctx, key, val, &WriteOptions{Sync: false}); err != nil {
			b.Fatal(err)
		}
	}
}

func TestReplay_CleanSingleFile(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(WalOptions{Dir: dir, RollSize: 1 << 30, FsyncPolicy: "none"})
	if err != nil {
		t.Fatal(err)
	}

	// write a few records with Sync=true for durability
	recs := []*WalRecord{
		{Seq: 1, Op: KindPut, Key: []byte("a"), Value: []byte("va")},
		{Seq: 2, Op: KindPut, Key: []byte("b"), Value: []byte("vb")},
		{Seq: 3, Op: KindDel, Key: []byte("a")},
	}
	for _, r := range recs {
		if err := w.Append(r, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// replay
	f, err := os.OpenFile(filepath.Join(dir, "WAL-000001.log"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	mem := map[string]valueVer{}
	apply := func(rec *WalRecord) error {
		switch rec.Op {
		case KindPut:
			mem[string(rec.Key)] = valueVer{seq: rec.Seq, kind: KindPut, val: append([]byte(nil), rec.Value...)}
		case KindDel:
			mem[string(rec.Key)] = valueVer{seq: rec.Seq, kind: KindDel}
		}
		return nil
	}
	maxSeq, err := ReplayFile(f, apply)
	if err != nil {
		t.Fatal(err)
	}

	if maxSeq != 3 {
		t.Fatalf("maxSeq=%d want=3", maxSeq)
	}
	if v, ok := mem["a"]; !ok || v.kind != KindDel {
		t.Fatalf("key a should be deleted: %+v", v)
	}
	if v, ok := mem["b"]; !ok || v.kind != KindPut || string(v.val) != "vb" {
		t.Fatalf("key b mismatch: %+v", v)
	}
}

func TestReplay_TruncatedTail(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(WalOptions{Dir: dir, RollSize: 1 << 30, FsyncPolicy: "none"})
	if err != nil {
		t.Fatal(err)
	}

	// One valid record
	if err := w.Append(&WalRecord{Seq: 1, Op: KindPut, Key: []byte("a"), Value: []byte("va")}, true); err != nil {
		t.Fatal(err)
	}
	// Prepare an incomplete second record: write header with length L but only half payload
	payload := encodePayload(&WalRecord{Seq: 2, Op: KindPut, Key: []byte("b"), Value: []byte("vb")})
	crc := crc32.Checksum(payload, crcTab)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc)
	if _, err := w.curBufw.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	// write only part of payload, then flush without sync
	half := len(payload) / 2
	if half == 0 {
		half = 1
	}
	if _, err := w.curBufw.Write(payload[:half]); err != nil {
		t.Fatal(err)
	}
	if err := w.curBufw.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.curFile.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Open file RW and replay; expect truncation at first valid record boundary
	f, err := os.OpenFile(filepath.Join(dir, "WAL-000001.log"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// compute expected size of first valid record
	p1 := encodePayload(&WalRecord{Seq: 1, Op: KindPut, Key: []byte("a"), Value: []byte("va")})
	expSize := int64(len(p1) + 8)

	var applied []string
	apply := func(rec *WalRecord) error {
		applied = append(applied, fmt.Sprintf("%d:%d:%s", rec.Seq, rec.Op, string(rec.Key)))
		return nil
	}
	_, err = ReplayFile(f, apply)
	if err != nil {
		t.Fatal(err)
	}

	// Only first record should apply
	if len(applied) != 1 || applied[0] != "1:1:a" {
		t.Fatalf("applied=%v", applied)
	}
	// File truncated to expSize
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != expSize {
		t.Fatalf("file size=%d want=%d", st.Size(), expSize)
	}
}

func TestReplay_BadCRCTail(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(WalOptions{Dir: dir, RollSize: 1 << 30, FsyncPolicy: "none"})
	if err != nil {
		t.Fatal(err)
	}

	// two valid records
	if err := w.Append(&WalRecord{Seq: 1, Op: KindPut, Key: []byte("a"), Value: []byte("va")}, true); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(&WalRecord{Seq: 2, Op: KindPut, Key: []byte("b"), Value: []byte("vb")}, true); err != nil {
		t.Fatal(err)
	}

	// craft a third record with corrupted payload (CRC will not match)
	good := encodePayload(&WalRecord{Seq: 3, Op: KindPut, Key: []byte("c"), Value: []byte("vc")})
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(good)))
	// write incorrect CRC (flip bits)
	badCRC := crc32.Checksum(good, crcTab) ^ 0xffffffff
	binary.LittleEndian.PutUint32(hdr[4:8], badCRC)
	if _, err := w.curBufw.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := w.curBufw.Write(good); err != nil {
		t.Fatal(err)
	}
	if err := w.curBufw.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.curFile.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(filepath.Join(dir, "WAL-000001.log"), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var appliedKeys []string
	apply := func(rec *WalRecord) error {
		appliedKeys = append(appliedKeys, string(rec.Key))
		return nil
	}
	_, err = ReplayFile(f, apply)
	if err != nil {
		t.Fatal(err)
	}

	if len(appliedKeys) != 2 || appliedKeys[0] != "a" || appliedKeys[1] != "b" {
		t.Fatalf("applied=%v", appliedKeys)
	}
}
