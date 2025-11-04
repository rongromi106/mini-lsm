# Mini LSM (Initial Design Skeleton)

This is a **minimal, runnable skeleton** for a Mini LSM Engine in Go. It currently
implements the public API with an **in-memory memtable** so you can compile, run,
and write tests immediately. The structure is laid out to evolve into a real
WAL + SSTable + Manifest + Compaction design. This project is designed by chatGPT
based on DDIA Chapter 5.

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
  - WAL append on write (fsync policy). -- Done
  - MemTable (skiplist) + Immutable flush to SSTable.

### MemTable
1. 目标与范围
	•	维护按 key 有序的可变视图，承接前台写入（Put/Delete）。
	•	支持点查、前缀/范围扫描、多版本可见性（基于 seq/snapshot）。
	•	达到阈值后转为 Immutable MemTable，参与后台 flush → SST。

2. 数据模型
	•	条目：InternalKey = user_key + seq + kind
	•	seq: 全局递增 uint64；kind: PUT(1)/DEL(2)（DEL 为 tombstone）
	•	值：[]byte；DEL 不保存值。
	•	有序性：按 user_key 升序；同 key 内按 seq 降序（新版本优先）。
	•	允许同 key 多版本共存，直至 flush/compaction 清理。

3. 写入语义
	•	Put(key,value,seq)：插入 <key, seq, PUT, value>。
	•	Delete(key,seq)：插入 <key, seq, DEL> tombstone。
	•	写入先 WAL 成功后才能对 MemTable 可见（由上层保证顺序）。
	•	写入并发：前台写串行化（或基于内部细粒度锁）。
读不阻塞写（读路径尽量 lock-free 或短锁）。

4. 读取语义
	•	Get(key, snapshot_seq)：
	•	在 Active MemTable 与所有 Immutable MemTable 中查找；
	•	对同 key，选择 seq ≤ snapshot_seq 的最新一条；
	•	若最新是 DEL → 返回不存在。
	•	Iterate(range|prefix, snapshot_seq)：
	•	产生合并迭代器（Active + 所有 Immutable + 上层 L0…）的一份 MemTable 端输入；
	•	按 user_key 升序输出；同 key 仅暴露首个可见版本；
	•	支持 Seek、First、Next，并能被上层合并器融合。

5. 容量管理与转为不可变
	•	配置项：MemTableSize（如 64MB）。
	•	当 active 使用量 ≥ 阈值：
	•	将其原子替换为新建的 Active；
	•	旧表标记为 Immutable，加入 flush 队列；
	•	写入继续进入新的 Active，读路径需同时可见 Active + 所有 Immutable。
	•	Backpressure：当排队的 Immutable 个数超阈值（如 2–3）时，前台写需短暂阻塞，直到 flush 进度恢复。

6. Flush 触发与交互
	•	触发条件：
	•	容量阈值；
	•	后台主动（例如关库/冷却）。
	•	Flush 输出：
	•	将该 Immutable MemTable 的内容按 SSTable Block 规则写盘；
	•	生成 SSTableMeta：文件号、层级（初始 L0）、key 范围、大小、统计信息；
	•	更新 MANIFEST（编辑记录），原子发布 Version；
	•	成功后释放 Immutable 内存。
	•	一致性点：flush 前 WAL 不删除；当该 WAL 覆盖的所有数据都已落入 SST（并且之前的 WAL 不再被任何未 flush 的 memtable 引用）→ 标记可删除。

7. 并发与可见性
	•	写入：单 writer（或内部串行化）；读多线程安全。
	•	迭代器：获取时固定一个 snapshot_seq，确保读到的版本稳定。
	•	与后台线程：flush/compaction 不影响前台可见性（版本原子切换）。 


  - Simple L0-only SSTable reading.
- Add Bloom filters, block cache, and leveling compaction.
# mini-lsm
