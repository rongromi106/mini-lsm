package lsm

import (
	"context"
	"errors"
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

type dbImpl struct {
	mu     sync.RWMutex
	seq    atomic.Uint64
	kv     map[string]valueVer // latest visible version per key (for the MVP)
	closed bool

	// Placeholders for future WAL/SSTables/etc, Project 2 and 3
	opts Options
	wal  *Wal
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
		kv:   make(map[string]valueVer),
		opts: opts,
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
		case KindDel:
			db.kv[string(rec.Key)] = valueVer{seq: rec.Seq, kind: KindDel}
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
