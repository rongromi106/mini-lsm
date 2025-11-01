# Mini LSM (Initial Design Skeleton)

This is a **minimal, runnable skeleton** for a Mini LSM Engine in Go. It currently
implements the public API with an **in-memory memtable** so you can compile, run,
and write tests immediately. The structure is laid out to evolve into a real
WAL + SSTable + Manifest + Compaction design.

## Layout

```
mini-lsm/
├── cmd/
│   └── example/
│       └── main.go          # Small demo that uses the API
├── pkg/
│   └── lsm/
│       ├── db.go            # DB interface + minimal in-memory impl
│       ├── internalkey.go   # InternalKey (user_key + seq + kind)
│       ├── iterator.go      # Basic iterator over in-memory memtable
│       ├── options.go       # Options and read/write options
│       ├── manifest.go      # Placeholders for future versioning/manifest
│       ├── sstable.go       # Placeholders for SSTable writer/reader
│       ├── wal.go           # Placeholders for WAL
│       ├── compaction.go    # Placeholders for compaction
│       └── db_test.go       # Sanity tests (pass with in-mem engine)
├── go.mod
└── README.md
```

## Quick start

```bash
cd mini-lsm
go run ./cmd/example
go test ./...
```

## Next steps

- Replace the in-memory store with:
  - WAL append on write (fsync policy).
  - MemTable (skiplist) + Immutable flush to SSTable.
  - Simple L0-only SSTable reading.
- Add Bloom filters, block cache, and leveling compaction.
# mini-lsm
