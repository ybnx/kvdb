package kvdb

import "encoding/binary"

const bucketHeadSize = 20

type Bucket struct {
	nameSize          uint32
	offset, hintsSize uint64
	name              []byte
}

func (b *Bucket) EncodeBucket() []byte {
	length := 20 + b.nameSize
	buf := make([]byte, length)
	binary.LittleEndian.PutUint32(buf[:4], b.nameSize)
	binary.LittleEndian.PutUint64(buf[4:12], b.offset)
	binary.LittleEndian.PutUint64(buf[12:bucketHeadSize], b.hintsSize)
	copy(buf[bucketHeadSize:], b.name)
	return buf
}

func DecodeBucketHeader(buf []byte) *Bucket {
	nameSize := binary.LittleEndian.Uint32(buf[:4])
	offset := binary.LittleEndian.Uint64(buf[4:12])
	hintSize := binary.LittleEndian.Uint64(buf[12:])
	return &Bucket{
		nameSize:  nameSize,
		offset:    offset,
		hintsSize: hintSize,
	}
}
