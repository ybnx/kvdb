package kvdb

import (
	"bytes"
	"encoding/binary"
)

const hintHeadSize = 8

type Hint struct {
	offset, keySize uint32 // 是kv写入文件时的txid， offset 记录db文件末尾位置
	key             []byte
}

func NewHint(key []byte, offset uint32) *Hint {
	return &Hint{
		offset:  offset,
		keySize: uint32(len(key)),
		key:     key,
	}
} //txId:    txId,

func (h *Hint) EncodeHintHeader() []byte {
	buf := make([]byte, hintHeadSize)
	binary.LittleEndian.PutUint32(buf[0:4], h.offset)
	binary.LittleEndian.PutUint32(buf[4:8], h.keySize)
	return buf
}

func (h *Hint) EncodeHint() []byte {
	buf := make([]byte, h.Len())
	binary.LittleEndian.PutUint32(buf[0:4], h.offset)
	binary.LittleEndian.PutUint32(buf[4:8], h.keySize)
	copy(buf[hintHeadSize:hintHeadSize+h.keySize], h.key)
	return buf
}

func (h *Hint) Len() int64 {
	return int64(hintHeadSize + len(h.key))
}

func DecodeHintHeader(buf []byte) *Hint {
	offset := binary.LittleEndian.Uint32(buf[0:4])
	keySize := binary.LittleEndian.Uint32(buf[4:8])
	return &Hint{
		offset:  offset,
		keySize: keySize,
	}
}

func hintLess(a, b interface{}) bool {
	i1, i2 := a.(*Hint), b.(*Hint)
	return bytes.Compare(i1.key, i2.key) < 0
}
