package lsm

import (
	"bytes"
	"slices"
)

// Iterator is a simple iterator over the in-memory view (for now).
type Iterator interface {
	Seek(key []byte)
	First()
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Close() error
}

type memIter struct {
	keys [][]byte
	vals map[string][]byte
	pos  int
}

func newMemIter(vals map[string][]byte) *memIter {
	keys := make([][]byte, 0, len(vals))
	for k := range vals {
		keys = append(keys, []byte(k))
	}
	slices.SortFunc(keys, func(a, b []byte) int { return bytes.Compare(a, b) })
	return &memIter{keys: keys, vals: vals, pos: -1}
}

func (it *memIter) Seek(key []byte) {
	it.pos = -1
	for i, k := range it.keys {
		if bytes.Compare(k, key) >= 0 {
			it.pos = i
			return
		}
	}
}

func (it *memIter) First() {
	if len(it.keys) == 0 {
		it.pos = -1
		return
	}
	it.pos = 0
}

func (it *memIter) Next() {
	if it.pos >= 0 && it.pos < len(it.keys) {
		it.pos++
		if it.pos >= len(it.keys) {
			it.pos = -1
		}
	}
}

func (it *memIter) Valid() bool { return it.pos >= 0 && it.pos < len(it.keys) }

func (it *memIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.pos]
}

func (it *memIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.vals[string(it.keys[it.pos])]
}

func (it *memIter) Close() error { return nil }
