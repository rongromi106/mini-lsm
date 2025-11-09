package lsm

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// DB is the user-facing interface.
type DB interface {
	Get(ctx context.Context, key []byte, ro *ReadOptions) ([]byte, bool, error)
	Put(ctx context.Context, key, value []byte, wo *WriteOptions) error
	Delete(ctx context.Context, key []byte, wo *WriteOptions) error
	NewIterator(ro *ReadOptions) Iterator
	NewSnapshot() *Snapshot
	ReleaseSnapshot(*Snapshot)
	Close() error
}

// --- Minimal in-memory implementation (scaffolding) ---

type valueVer struct {
	seq  uint64
	kind uint8
	val  []byte
}

type dbImpl struct {
	mu     sync.RWMutex
	seq    atomic.Uint64
	kv     map[string]valueVer // latest visible version per key (for the MVP)
	closed bool

	// Placeholders for future WAL/SSTables/etc, Project 2 and 3
	opts Options
	wal  *Wal
	// current actvie memtable
	memTable    *memTable
	flusheQueue []ImmutableMemTable
	stopChan    chan struct{}
}

/*
Open 先不直接打开WAL文件开始写
1) 确保目录存在
2) 扫描目录里的 WAL-*.log
3) 定义 apply 闭包（把 WAL 记录应用到内存）
4) 逐文件回放（注意用 O_RDWR，方便截断坏尾）调用ReplayFile 函数
5) 打开当前 WAL 文件，准备写入
*/
func Open(opts Options) (DB, error) {
	if opts.Dir != "" {
		opts.Dir = "./data"
	}
	os.MkdirAll(opts.Dir, 0o755)
	db := &dbImpl{
		kv:          make(map[string]valueVer),
		opts:        opts,
		memTable:    newMemTable(),
		flusheQueue: make([]ImmutableMemTable, 0),
		stopChan:    make(chan struct{}),
	}

	entries, err := os.ReadDir(opts.Dir)
	if err != nil {
		return nil, err
	}
	walPaths := []string{}
	walIds := []int{}
	re := regexp.MustCompile(`^WAL-(\d{6})\.log$`)
	for _, entry := range entries {
		m := re.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		id, _ := strconv.Atoi(m[1])
		walIds = append(walIds, id)
		walPaths = append(walPaths, filepath.Join(opts.Dir, entry.Name()))
	}
	slices.Sort(walPaths)

	// 3) 定义 apply 闭包（把 WAL 记录应用到内存）
	var maxSeq uint64
	var maxWalId int
	apply := func(rec *WalRecord) error {
		switch rec.Op {
		case KindPut:
			db.kv[string(rec.Key)] = valueVer{seq: rec.Seq, kind: KindPut, val: append([]byte(nil), rec.Value...)}
			db.memTable.Put(rec.Key, rec.Value, rec.Seq)
		case KindDel:
			db.kv[string(rec.Key)] = valueVer{seq: rec.Seq, kind: KindDel}
			db.memTable.Delete(rec.Key, rec.Seq)
		}
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
		return nil
	}

	// 4) 逐文件回放（注意用 O_RDWR，方便截断坏尾）调用ReplayFile 函数
	for i, path := range walPaths {
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		fileMaxSeq, _ := ReplayFile(f, apply)
		if fileMaxSeq > maxSeq {
			maxSeq = fileMaxSeq
		}
		if walIds[i] > maxWalId {
			maxWalId = walIds[i]
		}
	}

	db.seq.Store(maxSeq)
	// 5) 打开当前 WAL 文件，准备写入
	w, err := OpenWAL(WalOptions{
		Dir: opts.Dir, RollSize: int64(opts.WALRollSize), FsyncPolicy: opts.FsyncPolicy, FileId: maxWalId + 1,
	})
	if err != nil {
		return nil, err
	}
	db.wal = w
	// is it ok to be like this? using one single go routine to flush.
	go db.flush()
	return db, nil
}

func (db *dbImpl) Get(ctx context.Context, key []byte, ro *ReadOptions) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, false, errors.New("db is closed")
	}

	vv, ok := db.kv[string(key)]
	if !ok || vv.kind == KindDel {
		return nil, false, nil
	}
	return append([]byte(nil), vv.val...), true, nil
}

func (db *dbImpl) Put(ctx context.Context, key, value []byte, wo *WriteOptions) error {

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("db is closed")
	}

	seq := db.seq.Add(1)
	if db.wal != nil {
		rec := &WalRecord{Seq: seq, Op: KindPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)}
		if err := db.wal.Append(rec, wo.Sync); err != nil {
			return err
		}
	}
	db.kv[string(key)] = valueVer{seq: seq, kind: KindPut, val: append([]byte(nil), value...)}
	if err := db.memTable.Put(key, value, seq); err != nil {
		return err
	}
	if db.opts.MemTableSize > 0 && db.memTable.ApproxSize() > int64(db.opts.MemTableSize) {
		immutableMemTable, err := db.memTable.Freeze()
		if err != nil {
			return err
		}
		db.memTable = newMemTable()
		// Flushing work is done in a background goroutine
		db.flusheQueue = append(db.flusheQueue, immutableMemTable)
	}
	return nil
}

func (db *dbImpl) Delete(ctx context.Context, key []byte, wo *WriteOptions) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("db is closed")
	}

	seq := db.seq.Add(1)
	if db.wal != nil {
		rec := &WalRecord{Seq: seq, Op: KindDel, Key: append([]byte(nil), key...), Value: nil}
		if err := db.wal.Append(rec, wo.Sync); err != nil {
			return err
		}
	}
	db.kv[string(key)] = valueVer{seq: seq, kind: KindDel}
	if err := db.memTable.Delete(key, seq); err != nil {
		return err
	}
	return nil
}

func (db *dbImpl) NewIterator(ro *ReadOptions) Iterator {
	db.mu.RLock()
	defer db.mu.RUnlock()
	vals := make(map[string][]byte, len(db.kv))
	for k, v := range db.kv {
		if v.kind == KindPut {
			vals[k] = append([]byte(nil), v.val...)
		}
	}
	return newMemIter(vals)
}

func (db *dbImpl) NewSnapshot() *Snapshot {
	seq := db.seq.Load()
	return &Snapshot{Seq: seq}
}

func (db *dbImpl) ReleaseSnapshot(_ *Snapshot) {}

func (db *dbImpl) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			return err
		}
	}
	db.closed = true
	return nil
}

// Flushing immutable memtable to sstable
func (db *dbImpl) flush() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Drain the queue safely: pop one at a time under lock, flush outside the lock.
			for {
				db.mu.Lock()
				if len(db.flusheQueue) == 0 {
					db.mu.Unlock()
					break
				}
				imm := db.flusheQueue[0]
				db.flusheQueue = db.flusheQueue[1:]
				db.mu.Unlock()

				if err := db.flushImmutableMemTable(imm); err != nil {
					log.Println("flush immutable memtable failed", err)
					// continue draining subsequent items; optionally add backoff/retry later
				}
			}
		case <-db.stopChan:
			return
		}
	}
}

func (db *dbImpl) flushImmutableMemTable(imm ImmutableMemTable) error {
	// Create a temporary SSTable file in the DB directory
	tmpFile, err := os.CreateTemp(db.opts.Dir, "SST-*.sst.tmp")
	if err != nil {
		return err
	}
	// The table writer owns only the writing, caller manages renames
	tw, err := NewTableWriter(tmpFile, db.opts)
	if err != nil {
		_ = tmpFile.Close()
		return err
	}
	defer tw.Close()

	// Iterate immutable memtable in InternalKey order and write entries
	it := imm.NewInternalIterator()
	defer it.Close()
	it.First()
	for it.Valid() {
		tw.Add(it.InternalKey(), it.Value())
		it.Next()
	}
	if _, err := tw.Finish(); err != nil {
		return err
	}
	// Caller may later rename tmpFile.Name() to a final SSTable name in MANIFEST flow
	return nil
}
