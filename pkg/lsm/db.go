package lsm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
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

type immItem struct {
	id  uint64
	tbl ImmutableMemTable
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
	flusheQueue []immItem
	stopChan    chan struct{}
	// tracked SSTables (L0 only for now), newest appended last
	sstReaders []*tableReader
	flusher    *Flusher
	// results from background flusher
	flushResultCh chan flushResult
	immIndex      map[uint64]int
	nextImmID     atomic.Uint64
}

type FlushJob struct {
	id  uint64
	imm ImmutableMemTable
}

type Flusher struct {
	jobs     chan FlushJob
	stopChan chan struct{}
	opts     Options
	resultCh chan flushResult
}

type flushResult struct {
	id uint64
	tr *tableReader
}

func NewFlusher(dbOpts Options, resultCh chan flushResult) *Flusher {
	f := &Flusher{
		jobs:     make(chan FlushJob, 8),
		stopChan: make(chan struct{}),
		opts:     dbOpts,
		resultCh: resultCh,
	}
	go f.run()
	return f
}

func (f *Flusher) run() {
	for {
		select {
		case <-f.stopChan:
			return
		case job := <-f.jobs:
			tr, err := f.flushImmutableMemTable(job.imm)
			if err != nil {
				log.Println("flush immutable memtable failed", err)
				// continue draining subsequent items; optionally add backoff/retry later
				continue
			}
			// publish result back to DB
			f.resultCh <- flushResult{id: job.id, tr: tr}
		}
	}
}

func (f *Flusher) Submit(job FlushJob) {
	f.jobs <- job
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
		flusheQueue: make([]immItem, 0),
		stopChan:    make(chan struct{}),
		sstReaders:  make([]*tableReader, 0),
		immIndex:    make(map[uint64]int),
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

	// Load existing SSTables (L0) by scanning directory; newest last
	// Left out manifest flow for now
	entries2, err := os.ReadDir(opts.Dir)
	if err == nil {
		reSST := regexp.MustCompile(`^SST-(\d{6})\.sst$`)
		type sstEnt struct {
			id   int
			path string
		}
		ssts := []sstEnt{}
		for _, ent := range entries2 {
			m := reSST.FindStringSubmatch(ent.Name())
			if m == nil {
				continue
			}
			id, _ := strconv.Atoi(m[1])
			ssts = append(ssts, sstEnt{id: id, path: filepath.Join(opts.Dir, ent.Name())})
		}
		slices.SortFunc(ssts, func(a, b sstEnt) int {
			return a.id - b.id
		})
		for _, se := range ssts {
			f, err := os.Open(se.path)
			if err != nil {
				continue
			}
			tr, err := OpenTable(f, opts)
			if err != nil {
				_ = f.Close()
				continue
			}
			db.sstReaders = append(db.sstReaders, tr)
		}
	}
	// setup background flusher and result listener
	db.flushResultCh = make(chan flushResult, 8)
	db.flusher = NewFlusher(opts, db.flushResultCh)
	go func() {
		for {
			select {
			case res := <-db.flushResultCh:
				db.mu.Lock()
				// remove flushed immutable from queue using id-index map (O(1))
				// first, swap the last item and update the index map
				if idx, ok := db.immIndex[res.id]; ok {
					last := len(db.flusheQueue) - 1
					db.flusheQueue[idx] = db.flusheQueue[last]
					db.flusheQueue = db.flusheQueue[:last]
					if idx < len(db.flusheQueue) {
						db.immIndex[db.flusheQueue[idx].id] = idx
					}
					delete(db.immIndex, res.id)
				}
				// append new table reader
				db.sstReaders = append(db.sstReaders, res.tr)
				db.mu.Unlock()
			case <-db.stopChan:
				return
			}
		}
	}()
	return db, nil
}

/*
Point-lookup workflow
Goal: find the newest visible version of key (seq <= snapshot_seq) and honor tombstones
Search order (newest -> oldest)
*/

func (db *dbImpl) Get(ctx context.Context, key []byte, ro *ReadOptions) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, false, errors.New("db is closed")
	}
	seqLimit := ^uint64(0)
	if ro != nil && ro.Snapshot != nil {
		seqLimit = ro.Snapshot.Seq
	}

	// 0. KV memory in db
	if cur, ok := db.kv[string(key)]; ok {
		if cur.kind != KindDel && cur.seq <= seqLimit {
			return append([]byte(nil), cur.val...), true, nil
		}
	}

	// 1. Active MemTable
	val, ok, err := db.memTable.Get(key, seqLimit)
	if err != nil {
		return nil, false, err
	}
	if ok {
		return val, true, nil
	}

	// 2. Immutable MemTables
	for _, item := range db.flusheQueue {
		val, ok, err := item.tbl.Get(key, seqLimit)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return val, true, nil
		}
	}

	// 3. SSTables (L0 only) - search newest to oldest
	for i := len(db.sstReaders) - 1; i >= 0; i-- {
		tr := db.sstReaders[i]
		val, ok, err := tr.Get(key, seqLimit)
		if err != nil {
			// on corruption/IO, skip to next
			continue
		}
		if ok {
			return append([]byte(nil), val...), true, nil
		}
	}

	return nil, false, nil
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
		// Track immutable memtable and submit background flush job
		id := db.nextImmID.Add(1)
		db.flusheQueue = append(db.flusheQueue, immItem{id: id, tbl: immutableMemTable})
		db.immIndex[id] = len(db.flusheQueue) - 1
		go db.flusher.Submit(FlushJob{id: id, imm: immutableMemTable})
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
// legacy flush loop removed in favor of Flusher + channels

func (f *Flusher) flushImmutableMemTable(imm ImmutableMemTable) (*tableReader, error) {
	// Create a temporary SSTable file in the DB directory
	tmpFile, err := os.CreateTemp(f.opts.Dir, "SST-*.sst.tmp")
	if err != nil {
		return nil, err
	}
	// The table writer owns only the writing, caller manages renames
	tw, err := NewTableWriter(tmpFile, f.opts)
	if err != nil {
		_ = tmpFile.Close()
		return nil, err
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
		return nil, err
	}

	// Rename tmp to final SST-000xxx.sst (compute next id by scanning dir)
	nextId := 1
	ents, _ := os.ReadDir(f.opts.Dir)
	reSST := regexp.MustCompile(`^SST-(\d{6})\.sst$`)
	for _, ent := range ents {
		m := reSST.FindStringSubmatch(ent.Name())
		if m == nil {
			continue
		}
		id, _ := strconv.Atoi(m[1])
		if id >= nextId {
			nextId = id + 1
		}
	}
	finalName := filepath.Join(f.opts.Dir, fmt.Sprintf("SST-%06d.sst", nextId))
	if err := os.Rename(tmpFile.Name(), finalName); err != nil {
		return nil, err
	}
	// Open reader and track it
	rf, err := os.Open(finalName)
	if err != nil {
		return nil, err
	}
	tr, err := OpenTable(rf, f.opts)
	if err != nil {
		_ = rf.Close()
		return nil, err
	}
	return tr, nil
}
