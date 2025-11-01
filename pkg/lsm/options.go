package lsm

type Options struct {
	Dir               string
	MemTableSize      int
	WALRollSize       int
	BlockSize         int
	BloomFpRate       float64
	Compression       string // "snappy"|"zstd"|"none"
	MaxOpenFiles      int
	CompactionThreads int
	FlushThreads      int
	FsyncPolicy       string // "always"|"every_sec"|"none"
}

type ReadOptions struct {
	Snapshot *Snapshot
	Prefix   []byte // optional
}

type WriteOptions struct {
	Sync bool // override fsync policy
}

type Snapshot struct{ Seq uint64 }
