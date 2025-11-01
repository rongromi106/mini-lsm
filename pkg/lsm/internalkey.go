package lsm

const (
	KindPut uint8 = 1
	KindDel uint8 = 2
)

type InternalKey struct {
	UserKey []byte
	Seq     uint64
	Kind    uint8 // 1: put, 2: del
}
