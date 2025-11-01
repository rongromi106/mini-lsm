package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"example.com/mini-lsm/pkg/lsm"
)

func main() {
	ctx := context.Background()

	// Example 1: every_sec fsync policy, observe background flush/sync
	dir := "./data" // current Open overrides non-empty Dir to ./data
	_ = os.MkdirAll(dir, 0o755)
	opts := lsm.Options{Dir: dir, WALRollSize: 1 << 20, FsyncPolicy: "every_sec"}
	db, err := lsm.Open(opts)
	if err != nil {
		panic(err)
	}

	walPath := filepath.Join(dir, "WAL-000001.log")
	statSize := func() int64 {
		fi, err := os.Stat(walPath)
		if err != nil {
			return -1
		}
		return fi.Size()
	}

	// Write without Sync: stays in bufio buffer until bgSync runs
	_ = db.Put(ctx, []byte("e1"), []byte("value-1"), &lsm.WriteOptions{Sync: false})
	szBefore := statSize()
	fmt.Printf("every_sec: size before sleep = %d bytes\n", szBefore)
	time.Sleep(1500 * time.Millisecond)
	szAfter := statSize()
	fmt.Printf("every_sec: size after 1.5s  = %d bytes\n", szAfter)

	// Multiple quick writes without Sync; bgSync should flush within ~1s
	for i := 0; i < 3; i++ {
		k := fmt.Sprintf("e1k%d", i)
		v := fmt.Sprintf("val-%d", i)
		_ = db.Put(ctx, []byte(k), []byte(v), &lsm.WriteOptions{Sync: false})
	}
	fmt.Printf("every_sec: size immediate  = %d bytes\n", statSize())
	time.Sleep(1100 * time.Millisecond)
	fmt.Printf("every_sec: size +1.1s      = %d bytes\n", statSize())

	// Force a Sync=true write; should reflect immediately
	_ = db.Put(ctx, []byte("e1sync"), []byte("force"), &lsm.WriteOptions{Sync: true})
	fmt.Printf("every_sec: size after Sync  = %d bytes\n", statSize())

	// Quick read to show data path still works
	val, ok, _ := db.Get(ctx, []byte("e1"), &lsm.ReadOptions{})
	fmt.Printf("Get(e1) => ok=%v, val=%s\n", ok, string(val))

	_ = db.Close()
}
