package lsm

import (
	"bytes"
	"sync"

	"github.com/huandu/skiplist"
)

// MemTable: 可变内存表接口（跳表Implementation）
type MemTable interface {
	// 写入：由上层保证已写 WAL
	Put(userKey, value []byte, seq uint64) error
	Delete(userKey []byte, seq uint64) error

	// 点查（仅限当前表，遵循 snapshot 可见性：seqLimit）
	Get(userKey []byte, seqLimit uint64) (val []byte, ok bool, err error)

	// 迭代器：按 userKey 升序，内部多版本择首（<= seqLimit 的最新非 DEL）
	NewIterator(seqLimit uint64, prefix []byte) Iterator

	// 内存占用与项数
	ApproxSize() int64
	NumEntries() int64

	// 冻结：返回不可变视图，当前表清零重建由上层负责
	Freeze() (ImmutableMemTable, error)
}

// 不可变 MemTable：仅读 + flush 输入
type ImmutableMemTable interface {
	// 与 MemTable 的读接口相同
	Get(userKey []byte, seqLimit uint64) (val []byte, ok bool, err error)
	NewIterator(seqLimit uint64, prefix []byte) Iterator

	// 提供 InternalKey 顺序的流（flush 用）
	// （等价于 NewIterator 的一个“内部版”，暴露 InternalKey 与原始值）
	NewInternalIterator() InternalIterator

	// 元信息
	ApproxSize() int64
	NumEntries() int64
}

// Internal 迭代器：按 InternalKey 升序（UserKey 升序，Seq 降序）
type InternalIterator interface {
	First()
	SeekInternal(ikey InternalKey) // 常用于 flush 的起点
	Next()
	Valid() bool

	InternalKey() InternalKey
	Value() []byte
	Close() error
}

// --- Internal storage types (Step 1: core structures) ---

// internalOrdKey defines the ordering in the skiplist: userKey asc, seq desc.
type internalOrdKey struct {
	userKey []byte
	seq     uint64
}

// entryVal stores the value kind and payload for an internal entry.
type entryVal struct {
	kind  uint8
	value []byte
}

// memTable is the mutable in-memory table backed by a skiplist.
type memTable struct {
	mu         sync.RWMutex
	list       *skiplist.SkipList
	approxSize int64
	numEntries int64
}

// immutableMemTable is a read-only snapshot used for flush.
type immutableMemTable struct {
	list       *skiplist.SkipList
	approxSize int64
	numEntries int64
}

// --- Core comparator and constructor (mutable table) ---

// compareInternal defines composite ordering: userKey asc, then seq desc.
func compareInternal(a, b interface{}) int {
	ka := a.(internalOrdKey)
	kb := b.(internalOrdKey)
	if c := bytes.Compare(ka.userKey, kb.userKey); c != 0 {
		if c > 0 {
			return 1
		}
		return -1
	}
	if ka.seq > kb.seq {
		return -1
	}
	if ka.seq < kb.seq {
		return 1
	}
	return 0
}

func newMemTable() *memTable {
	return &memTable{
		list: skiplist.New(skiplist.GreaterThanFunc(compareInternal)),
	}
}

/*
This is needed because:
Purpose: Account for non-payload memory so ApproxSize() tracks real RAM use and triggers Freeze()
near Options.MemTableSize.
Includes Interface boxing, Allocator/GC Overhead, etc.
*/
const memEntryOverhead = 32 // approximate per-entry overhead in bytes

// --- Mutable memTable operations ---

func (m *memTable) Put(userKey, value []byte, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.Set(internalOrdKey{userKey: userKey, seq: seq}, entryVal{kind: KindPut, value: value})
	m.approxSize += int64(len(userKey)) + int64(len(value)) + memEntryOverhead
	m.numEntries++
	return nil
}

func (m *memTable) Delete(userKey []byte, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.Set(internalOrdKey{userKey: userKey, seq: seq}, entryVal{kind: KindDel})
	m.approxSize += int64(len(userKey)) + memEntryOverhead
	m.numEntries++
	return nil
}

func (m *memTable) Get(userKey []byte, seqLimit uint64) (val []byte, ok bool, err error) {
	m.mu.RLock()
	if m.list == nil {
		m.mu.RUnlock()
		return nil, false, nil
	}
	res := m.list.Find(internalOrdKey{userKey: userKey, seq: seqLimit})
	if res == nil {
		m.mu.RUnlock()
		return nil, false, nil
	}
	k := res.Key().(internalOrdKey)
	if !bytes.Equal(k.userKey, userKey) {
		m.mu.RUnlock()
		return nil, false, nil
	}
	if k.seq > seqLimit {
		m.mu.RUnlock()
		return nil, false, nil
	}
	val = res.Value.(entryVal).value

	// Handling tombstone
	if res.Value.(entryVal).kind == KindDel {
		m.mu.RUnlock()
		return nil, false, nil
	}
	m.mu.RUnlock()
	return val, true, nil
}

func (m *memTable) ApproxSize() int64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	sz := m.approxSize
	m.mu.RUnlock()
	return sz
}

func (m *memTable) NumEntries() int64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	n := m.numEntries
	m.mu.RUnlock()
	return n
}

func (m *memTable) Freeze() (ImmutableMemTable, error) {
	return newImmutableMemTable(m), nil
}

// --- Immutable memTable operations ---
// freeze swap approach to avoid deep copy and stop the world
func newImmutableMemTable(mutableMemTable *memTable) *immutableMemTable {
	mutableMemTable.mu.Lock()
	defer mutableMemTable.mu.Unlock()
	immutableMemTable := &immutableMemTable{list: mutableMemTable.list}
	immutableMemTable.approxSize = mutableMemTable.approxSize
	immutableMemTable.numEntries = mutableMemTable.numEntries
	mutableMemTable.list = skiplist.New(skiplist.GreaterThanFunc(compareInternal))
	mutableMemTable.approxSize = 0
	mutableMemTable.numEntries = 0
	return immutableMemTable
}

func (m *immutableMemTable) ApproxSize() int64 {
	return m.approxSize
}

func (m *immutableMemTable) NumEntries() int64 {
	return m.numEntries
}

// No Locking, because it is read-only, list is stable for immutableMemTable
func (m *immutableMemTable) Get(userKey []byte, seqLimit uint64) (val []byte, ok bool, err error) {
	if m.list == nil {
		return nil, false, nil
	}
	res := m.list.Find(internalOrdKey{userKey: userKey, seq: seqLimit})
	if res == nil {
		return nil, false, nil
	}
	k := res.Key().(internalOrdKey)
	if !bytes.Equal(k.userKey, userKey) {
		return nil, false, nil
	}

	val = res.Value.(entryVal).value

	// Handling tombstone
	if res.Value.(entryVal).kind == KindDel {
		return nil, false, nil
	}
	return val, true, nil
}

/*
This is for Range query
Purpose: Produce a user-visible snapshot over the immutable table.
What it yields: At most one KV per userKey — the newest Put whose seq ≤ seqLimit. Keys with only tombstones visible at seqLimit are skipped.
Order: userKey ascending.
Prefix: If prefix is provided, only return keys starting with prefix; stop when the next key loses the prefix.

Example Usage
it := db.NewIterator(snapshotSeq)
it.Seek([]byte("apple"))
*/
func (m *immutableMemTable) NewIterator(seqLimit uint64, prefix []byte) Iterator {
	vals := make(map[string][]byte)
	if m == nil || m.list == nil {
		return newMemIter(vals)
	}
	var e *skiplist.Element
	if len(prefix) > 0 {
		// bitwise Not 0, find the newest version of the key
		e = m.list.Find(internalOrdKey{userKey: prefix, seq: ^uint64(0)})
	} else {
		e = m.list.Front()
	}
	// need to handle two scenarios here: (1) k is no longer the preifx (2) k's seq is greater than seqLimit
	for e != nil {
		k := e.Key().(internalOrdKey)
		if len(prefix) > 0 && !bytes.HasPrefix(k.userKey, prefix) {
			break
		}
		// scan through all user key with the same prefix to find the first visible value
		userKey := k.userKey
		for cur := e; cur != nil; cur = cur.Next() {
			if !bytes.Equal(cur.Key().(internalOrdKey).userKey, userKey) {
				break
			}
			if cur.Key().(internalOrdKey).seq > seqLimit {
				break
			}
			val := cur.Value.(entryVal)
			if val.kind == KindDel {
				continue
			}
			if val.kind == KindPut {
				vals[string(userKey)] = val.value
				break
			}
		}
		// Advance e to next distinct userKey
		for e = e.Next(); e != nil; e = e.Next() {
			nk := e.Key().(internalOrdKey)
			if !bytes.Equal(nk.userKey, k.userKey) {
				break
			}
		}
	}

	return newMemIter(vals)
}

func (m *immutableMemTable) NewInternalIterator() InternalIterator {
	return &internalIter{list: m.list}
}

// internalIter iterates immutable memtable in InternalKey order (userKey asc, seq desc).
type internalIter struct {
	list *skiplist.SkipList
	elem *skiplist.Element
}

func (it *internalIter) First() {
	it.elem = it.list.Front()
}

func (it *internalIter) SeekInternal(ikey InternalKey) {
	it.elem = it.list.Find(internalOrdKey{userKey: ikey.UserKey, seq: ikey.Seq})
}

func (it *internalIter) Next() {
	// TODO: advance it.elem to the next element
	it.elem = it.elem.Next()
}

func (it *internalIter) Valid() bool {
	return it.elem != nil
}

func (it *internalIter) InternalKey() InternalKey {
	return InternalKey{UserKey: it.elem.Key().(internalOrdKey).userKey,
		Seq:  it.elem.Key().(internalOrdKey).seq,
		Kind: it.elem.Value.(entryVal).kind}
}

func (it *internalIter) Value() []byte {
	return it.elem.Value.(entryVal).value
}

func (it *internalIter) Close() error {
	// TODO: release any resources if needed (usually no-op)
	return nil
}
