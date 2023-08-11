package kvdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	Add           = 6
	Delete        = 7
	entryHeadSize = 34 //4 + 4 + 4 + 4 + 2 + 8 + 8 = 34
)

type Entry struct { //header定长，能不能变成指针
	entryCrc, keySize, valueSize, bucketSize uint32
	entryType                                uint16
	ttl, timeStamp                           uint64
	key, value, bucket                       []byte //如果value为nil怎么办
}

func NewEntry(key, value []byte, bucket string, entryType uint16, ttl uint64) *Entry {
	return &Entry{ //需要在这里计算crc32吗
		keySize:    uint32(len(key)),
		valueSize:  uint32(len(value)),
		bucketSize: uint32(len(bucket)),
		entryType:  entryType,
		ttl:        ttl, //ttl设置为0表示不设置ttl
		timeStamp:  uint64(time.Now().Unix()),
		key:        key,
		value:      value,
		bucket:     StringToBytes(bucket),
	}
}

func (e *Entry) EncodeEntry() []byte {
	buf := make([]byte, e.Size())
	binary.LittleEndian.PutUint32(buf[4:8], e.keySize)
	binary.LittleEndian.PutUint32(buf[8:12], e.valueSize)
	binary.LittleEndian.PutUint32(buf[12:16], e.bucketSize)
	binary.LittleEndian.PutUint16(buf[16:18], e.entryType)
	binary.LittleEndian.PutUint64(buf[18:26], e.ttl)
	binary.LittleEndian.PutUint64(buf[26:34], e.timeStamp)
	copy(buf[entryHeadSize:entryHeadSize+e.keySize], e.key)
	copy(buf[entryHeadSize+e.keySize:entryHeadSize+e.keySize+e.valueSize], e.value)
	copy(buf[entryHeadSize+e.keySize+e.valueSize:], e.bucket)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf
}

func (e *Entry) DataSize() int64 {
	return int64(e.keySize + e.valueSize + e.bucketSize)
}

func (e *Entry) Size() int64 {
	return int64(entryHeadSize + e.keySize + e.valueSize + e.bucketSize)
}

func DecodeEntryHeader(buf []byte) *Entry {
	entryCrc := binary.LittleEndian.Uint32(buf[0:4])
	keySize := binary.LittleEndian.Uint32(buf[4:8])
	valueSize := binary.LittleEndian.Uint32(buf[8:12])
	bucketSize := binary.LittleEndian.Uint32(buf[12:16])
	entryType := binary.LittleEndian.Uint16(buf[16:18])
	ttl := binary.LittleEndian.Uint64(buf[18:26])
	timeStamp := binary.LittleEndian.Uint64(buf[18:26])
	return &Entry{
		entryCrc:   entryCrc,
		keySize:    keySize,
		valueSize:  valueSize,
		bucketSize: bucketSize,
		entryType:  entryType,
		ttl:        ttl,
		timeStamp:  timeStamp,
	}
}

func (e *Entry) Crc32(head []byte) uint32 {
	crc := crc32.ChecksumIEEE(head[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.key)
	crc = crc32.Update(crc, crc32.IEEETable, e.value)
	crc = crc32.Update(crc, crc32.IEEETable, e.bucket)
	return crc
}

func (e *Entry) IsExpired() bool {
	if e.ttl > 0 && e.timeStamp+e.ttl < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

func entryLess(a, b interface{}) bool { //只需要key bytes和tx id就可以找到entry
	i1, i2 := a.(*Entry), b.(*Entry)
	if bytes.Compare(i1.key, i2.key) < 0 {
		return true
	} else if bytes.Compare(i1.key, i2.key) > 0 {
		return false
	} else {
		return bytes.Compare(i1.bucket, i2.bucket) < 0
	}
}
