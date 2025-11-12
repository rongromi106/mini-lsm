package lsm

import (
	"os"
	"testing"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

func TestBloomPolicy_RoundTrip(t *testing.T) {
	b := &BloomPolicy{FpRate: 0.01, Filter: bloom.NewWithEstimates(100, 0.01)}
	keys := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}
	for _, k := range keys {
		b.Add(k)
	}
	buf, err := b.WriteToBuffer()
	if err != nil {
		t.Fatalf("WriteToBuffer: %v", err)
	}
	if len(buf) == 0 {
		t.Fatalf("serialized bloom is empty")
	}
	b2 := &BloomPolicy{FpRate: 0.01, Filter: bloom.New(1, 1)}
	if err := b2.ReadFromBuffer(buf); err != nil {
		t.Fatalf("ReadFromBuffer: %v", err)
	}
	for _, k := range keys {
		if !b2.MayContain(k) {
			t.Fatalf("restored bloom missing known key %q", string(k))
		}
	}
}

func TestTableReader_Get_WithBloom_HitAndMiss(t *testing.T) {
	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "SST-*.sst")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}

	opts := Options{BlockSize: 128, BloomFpRate: 0.01}
	tw, err := NewTableWriter(f, opts)
	if err != nil {
		f.Close()
		t.Fatalf("NewTableWriter: %v", err)
	}
	// Insert a couple of unique keys
	if err := tw.Add(InternalKey{UserKey: []byte("a"), Seq: 2, Kind: KindPut}, []byte("va")); err != nil {
		t.Fatalf("Add a: %v", err)
	}
	if err := tw.Add(InternalKey{UserKey: []byte("b"), Seq: 1, Kind: KindPut}, []byte("vb")); err != nil {
		t.Fatalf("Add b: %v", err)
	}
	if _, err := tw.Finish(); err != nil {
		_ = tw.Close()
		t.Fatalf("Finish: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	// Reopen and query
	rf, err := os.Open(f.Name())
	if err != nil {
		t.Fatalf("Open for read: %v", err)
	}
	defer rf.Close()
	tr, err := OpenTable(rf, Options{BloomFpRate: 0.01})
	if err != nil {
		t.Fatalf("OpenTable: %v", err)
	}
	defer tr.Close()

	// Hit
	if val, ok, err := tr.Get([]byte("a"), 0); err != nil {
		t.Fatalf("Get(a) error: %v", err)
	} else if !ok {
		t.Fatalf("Get(a) not found")
	} else if string(val) != "va" {
		t.Fatalf("Get(a) value mismatch: got %q want %q", string(val), "va")
	}

	// Miss (Bloom may help fast-negative; either way must return not found)
	if _, ok, err := tr.Get([]byte("z"), 0); err != nil {
		t.Fatalf("Get(z) error: %v", err)
	} else if ok {
		t.Fatalf("Get(z) unexpectedly found")
	}
}
