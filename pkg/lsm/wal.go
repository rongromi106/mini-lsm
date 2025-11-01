// Placeholder for Write-Ahead Log implementation.
package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Target: append-only records [crc][seq][op][klen][vlen][key][value] with fsync policy.
/*
总览：WAL 的职责
	1.	顺序记录：把每次写（Put/Delete）变成一条记录，顺序追加到日志文件。
	2.	崩溃恢复：重启时顺序读取日志，只重放最后一条完整记录之前的内容。
	3.	滚动切换：文件大到阈值时新建下一个 WAL 文件继续写。
	4.	同步策略：控制何时 fsync（每条、每秒、或不主动）。

我们先把“写入一条记录”做出来，再补滚动，再补恢复。
*/

var crcTab = crc32.MakeTable(crc32.Castagnoli)

func walFileName(id int) string { return fmt.Sprintf("WAL-%06d.log", id) }

// helper function to encode the payload
func encodePayload(rec *WalRecord) []byte {
	// [seq u64][op u8][klen u32][vlen u32][key][value]
	n := 8 + 1 + 4 + 4 + len(rec.Key) + len(rec.Value)
	buf := make([]byte, n)
	off := 0
	binary.LittleEndian.PutUint64(buf[off:off+8], rec.Seq)
	off += 8
	buf[off] = rec.Op
	off++
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(rec.Key)))
	off += 4
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(rec.Value)))
	off += 4
	copy(buf[off:], rec.Key)
	off += len(rec.Key)
	copy(buf[off:], rec.Value)
	return buf
}

func decodePayload(p []byte) *WalRecord {
	off := 0
	seq := binary.LittleEndian.Uint64(p[off : off+8])
	off += 8
	op := p[off]
	off++
	klen := int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	vlen := int(binary.LittleEndian.Uint32(p[off : off+4]))
	off += 4
	key := append([]byte(nil), p[off:off+klen]...)
	off += klen
	val := append([]byte(nil), p[off:off+vlen]...)
	return &WalRecord{Seq: seq, Op: op, Key: key, Value: val}
}

type WalOptions struct {
	Dir         string
	FileId      int
	RollSize    int64  // wal maximum file size
	FsyncPolicy string // "always"|"every_sec"|"none" => flush data from memory to disk, always is the safest but lowest performance, every_sec is a balance between safety and performance, none is the lowest performance but highest safety
}

type Wal struct {
	dir      string
	rollSize int64
	policy   string

	curFile *os.File
	curSize int64
	// It accumulates small writes in memory and flushes them to the underlying writer when the buffer fills or when you call Flush
	curBufw *bufio.Writer
	fileId  int

	// For Fsync policy
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
}

/*
WAL Record Format:
[ len   : 4 bytes ]      ← 记录 payload 的长度
[ crc32 : 4 bytes ]      ← payload 的校验值
[ payload : len bytes ]  ← 真正的内容（seq/op/key/value）
*/
type WalRecord struct {
	Seq   uint64
	Op    uint8 // 1=PUT, 2=DEL（和你的 KindPut/KindDel 对齐）
	Key   []byte
	Value []byte // DEL 时空
}

type WalReader struct{ r *bufio.Reader }

func NewWalReader(f *os.File) *WalReader { return &WalReader{r: bufio.NewReader(f)} }

func OpenWAL(opts WalOptions) (*Wal, error) {
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	w := &Wal{
		dir:      opts.Dir,
		rollSize: opts.RollSize,
		policy:   opts.FsyncPolicy,
		fileId:   1,
		curSize:  0,
	}
	// 先打开一个当前文件
	path := filepath.Join(w.dir, walFileName(w.fileId))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w.curFile = f
	w.curBufw = bufio.NewWriterSize(f, 1<<20) // 1MB
	if w.policy == "every_sec" {
		// channel是共享信号所有监听的go routine收到信号都会退出
		if w.stopChan == nil {
			w.stopChan = make(chan struct{})
			w.wg.Add(1)
			go w.bgSync()
		}
	}
	return w, nil
}

func (w *Wal) Close() error {
	if w.stopChan != nil {
		close(w.stopChan)
		w.wg.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	// 最后一次flush
	var firstErr error
	if w.curBufw != nil {
		if err := w.curBufw.Flush(); err != nil {
			firstErr = err
		}
	}
	// 最后一次sync
	if w.curFile != nil {
		if err := w.curFile.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := w.curFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.curFile = nil
	}
	return firstErr
}

func (w *Wal) Append(record *WalRecord, forceSync bool) error {
	payload := encodePayload(record)
	crc := crc32.Checksum(payload, crcTab)
	// construct header
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc)

	need := int64(len(payload) + 8)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.rollSize > 0 && w.curSize+need >= w.rollSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	// write header and payload
	if _, err := w.curBufw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.curBufw.Write(payload); err != nil {
		return err
	}

	w.curSize += need

	if forceSync || w.policy == "always" {
		if err := w.curBufw.Flush(); err != nil {
			return err
		}
		if err := w.curFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (w *Wal) rotate() error {
	if w.curBufw != nil {
		if err := w.curBufw.Flush(); err != nil {
			return err
		}
	}
	if w.curFile != nil {
		if err := w.curFile.Sync(); err != nil {
			return err
		}
		_ = w.curFile.Close()
	}
	w.fileId++
	path := filepath.Join(w.dir, walFileName(w.fileId))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.curFile = f
	w.curBufw = bufio.NewWriterSize(f, 1<<20) // 1MB
	w.curSize = 0
	return nil
}

func (rd *WalReader) Next() (*WalRecord, int64, error) {
	// 按照len, crc, payload的顺序读取
	var hdr [8]byte
	if _, err := io.ReadFull(rd.r, hdr[:]); err != nil {
		return nil, 0, err // 可能是 io.EOF
	}
	length := binary.LittleEndian.Uint32(hdr[0:4])
	wantCRC := binary.LittleEndian.Uint32(hdr[4:8])
	payload := make([]byte, length)
	if _, err := io.ReadFull(rd.r, payload); err != nil {
		return nil, 0, err
	}
	gotCRC := crc32.Checksum(payload, crcTab)
	if gotCRC != wantCRC {
		return nil, 0, fmt.Errorf("crc mismatch: got %x, want %x", gotCRC, wantCRC)
	}
	result := decodePayload(payload)
	// length + 8 because length is the size of the payload, and 8 is the size of the header
	return result, int64(length + 8), nil
}

func ReplayFile(f *os.File, apply func(*WalRecord) error) (maxSeq uint64, err error) {
	rd := NewWalReader(f)
	var offset int64
	for {
		rec, n, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			_ = f.Truncate(offset)
			break
		}
		offset += n
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
		if err := apply(rec); err != nil {
			_ = f.Truncate(offset)
			return 0, err
		}
	}
	return maxSeq, nil
}

// Implements fsync every second
func (w *Wal) bgSync() {
	defer w.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopChan:
			return
		case <-ticker.C:
			w.mu.Lock()

			if w.curBufw != nil {
				w.curBufw.Flush()
			}
			if w.curFile != nil {
				w.curFile.Sync()
			}
			w.mu.Unlock()
		}
	}
}
