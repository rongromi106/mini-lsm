package lsm

// --- On-disk basics ---

const (
	// sstMagic is a placeholder magic number for SSTable files.
	sstMagic   uint64 = 0x626c6b537354626c
	sstVersion uint32 = 1
)

// BlockHandle represents a [offset, length] region in the SSTable file.
type BlockHandle struct {
	Offset uint64
	Length uint64
}

// Footer is placed at the end of the file and references the index and filter blocks.
type Footer struct {
	IndexHandle  BlockHandle
	FilterHandle BlockHandle
	Version      uint32
	Magic        uint64
}

// --- Compression interfaces (stubs) ---

// Compressor defines an interface for optional block compression.
type Compressor interface {
	Name() string
	Compress(in []byte) ([]byte, error)
	Decompress(in []byte) ([]byte, error)
}

// noCompression is a stub that performs no compression.
type noCompression struct{}

func (n *noCompression) Name() string { return "none" }

func (n *noCompression) Compress(in []byte) ([]byte, error) { return in, nil }

func (n *noCompression) Decompress(in []byte) ([]byte, error) { return in, nil }

// --- Filter policy interfaces (Bloom stub) ---

// FilterPolicy provides a per-table filter (e.g., Bloom) to avoid unnecessary IO.
type FilterPolicy interface {
	Name() string
	MayContain(key []byte, filter []byte) bool
	Build(keys [][]byte) []byte
}

// BloomPolicy is a stub implementation with configurable false positive rate.
type BloomPolicy struct{ FpRate float64 }

func (b *BloomPolicy) Name() string { return "bloom" }

func (b *BloomPolicy) MayContain(_ []byte, _ []byte) bool { return true }

func (b *BloomPolicy) Build(_ [][]byte) []byte { return nil }

// --- Block builders/readers (skeleton) ---

// blockBuilder accumulates KV pairs into a data block using prefix/delta encoding (stubbed).
type blockBuilder struct {
	buf      []byte
	restarts []uint32
	counter  int
	lastKey  []byte
}

func (b *blockBuilder) Reset() {}

func (b *blockBuilder) Add(_ []byte, _ []byte) {}

func (b *blockBuilder) Finish() []byte { return nil }

// blockReader reads a single data block (stubbed).
type blockReader struct{ data []byte }

func (br blockReader) Get(_ []byte) (val []byte, ok bool) { return nil, false }

// blockIter iterates entries within a single block (stubbed).
type blockIter struct{}

// --- Index and filter builders (skeleton) ---

type indexEntry struct {
	SepKey []byte
	Hdl    BlockHandle
}

type indexBuilder struct{ entries []indexEntry }

func (ib *indexBuilder) Add(_ []byte, _ BlockHandle) {}

func (ib *indexBuilder) Finish() []byte { return nil }

// filterBuilder accumulates keys per data-block for filter construction (stubbed).
type filterBuilder struct {
	policy FilterPolicy
	buf    []byte
}

func (fb *filterBuilder) StartBlock(_ uint64) {}

func (fb *filterBuilder) AddKey(_ []byte) {}

func (fb *filterBuilder) Finish() []byte { return nil }

// --- Table writer (skeleton) ---

type tableWriter struct {
	// destination (e.g., *os.File) will be added in a later step
	opts        Options
	cmp         func(a, b []byte) int
	blockSize   int
	compressor  Compressor
	filter      *filterBuilder
	dataBuilder *blockBuilder
	index       *indexBuilder

	// rolling state
	pendingFirstKey []byte
	offset          uint64
}

// Add expects InternalKey ordered by userKey asc and seq desc.
func (tw *tableWriter) Add(_ InternalKey, _ []byte) error { return nil }

func (tw *tableWriter) Finish() (Footer, error) { return Footer{}, nil }

func (tw *tableWriter) Close() error { return nil }

// --- Table reader (skeleton) ---

type tableReader struct {
	// source (e.g., *os.File) will be added in a later step
	opts       Options
	compressor Compressor
	filter     FilterPolicy
	indexData  []byte
	footer     Footer
}

func OpenTable(_ string, opts Options) (*tableReader, error) {
	tr := &tableReader{
		opts:       opts,
		compressor: pickCompressor(opts.Compression),
		filter:     defaultFilter(opts.BloomFpRate),
	}
	return tr, nil
}

func (tr *tableReader) Get(_ []byte, _ uint64) ([]byte, bool, error) { return nil, false, nil }

func (tr *tableReader) NewIterator(_ *ReadOptions) Iterator { return nil }

func (tr *tableReader) Close() error { return nil }

// --- Option wiring helpers ---

func pickCompressor(_ string) Compressor { return &noCompression{} }

func defaultFilter(fpRate float64) FilterPolicy { return &BloomPolicy{FpRate: fpRate} }
