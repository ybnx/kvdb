package kvdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"hash/adler32"
	"hash/crc32"
	"runtime"
	"unsafe"
)

func EncodeKey(key any) ([]byte, error) {
	var (
		keyBytes []byte
		reterr   error
	)
	switch key.(type) {
	case []byte:
		keyBytes, reterr = key.([]byte), nil
	case string:
		keyBytes, reterr = StringToBytes(key.(string)), nil
	case bool, float32, float64, complex64, complex128, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, key)
		keyBytes, reterr = buf.Bytes(), err
	default:
		keyBytes, reterr = msgpack.Marshal(key)
	}
	if len(keyBytes) == 0 {
		return nil, errors.New("key is empty")
	}
	return keyBytes, reterr
}

func EncodeValue(value any) ([]byte, error) {
	var (
		valueBytes []byte
		reterr     error
	)
	switch value.(type) {
	case []byte:
		valueBytes, reterr = value.([]byte), nil
	case string:
		valueBytes, reterr = StringToBytes(value.(string)), nil
	default:
		valueBytes, reterr = msgpack.Marshal(value)
	}
	return valueBytes, reterr
}

func DecodeValue(data []byte, dst any) error {
	var err error
	switch dst.(type) {
	case *[]byte:
		*(dst.(*[]byte)) = data
	case *string:
		*(dst.(*string)) = BytesToString(data)
	default:
		err = msgpack.Unmarshal(data, dst)
	}
	return err
}

func StringToBytes(str string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{str, len(str)},
	))
}

func BytesToString(bts []byte) string {
	return *(*string)(unsafe.Pointer(&bts))
}

func HashKey(key []byte) uint32 {
	hash1 := adler32.Checksum(key)
	hash2 := crc32.ChecksumIEEE(key)
	return (hash1 + hash2) % 1024
}

func programInfo() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("Allocated memory: %d bytes\n", mem.Alloc)
	fmt.Printf("Total memory: %d bytes\n", mem.TotalAlloc)
	fmt.Printf("Number of mallocs: %d\n", mem.Mallocs)
	fmt.Printf("Number of frees: %d\n", mem.Frees)
	fmt.Println(runtime.NumGoroutine())
}
