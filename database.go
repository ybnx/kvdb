package kvdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/tidwall/btree"
	"golang.org/x/exp/mmap"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	_ = 1 << (10 * iota)
	KIB
	MIB
	GIB
	DefaultBucket = "plz_dont_use"
)

type Option struct {
	MaxMemoryUsage int64
	MaxRecordPreTx int
	DbPath         string
}

func DefaultOption() *Option {
	return &Option{
		MaxMemoryUsage: GIB,
		MaxRecordPreTx: 10000,
		DbPath:         "E:\\golangProject\\demo2\\dbtest\\db.txt",
	}
}

type DB struct {
	fileMu, hintMu              sync.RWMutex
	activeFile                  *os.File
	writer                      *bufio.Writer
	hints                       map[string]*btree.BTree
	buckets                     map[string]*Bucket
	option                      *Option
	logger                      *Logger
	dbDir, hintPath, bucketPath string
	updated, closed             bool
	txId, walId                 uint32
	offset                      int64
}

func Open(option *Option) (*DB, error) {
	if option == nil {
		return nil, errors.New("please set option or use DefaultOption()")
	}
	db := &DB{
		hints:   make(map[string]*btree.BTree, 1),
		buckets: make(map[string]*Bucket, 1),
		option:  option,
		dbDir:   filepath.Dir(option.DbPath),
		updated: false,
		closed:  false,
		txId:    1,
	}
	err := db.initFileIo()
	if err != nil {
		reterr := fmt.Errorf("kvdb: init file io failed %v", err)
		log.Println(reterr)
		return nil, reterr
	}
	log.Println("kvdb: database open successsfully")
	return db, nil
}

func (d *DB) Close() {
	if d.closed {
		log.Println("kvdb: database had closed")
	}
	d.closed = true //使数据库无法开启新事务
	if d.updated {
		err := d.buildHints()
		if err != nil {
			log.Println("kvdb: build hints file failed: ", err)
		}
	}
	_ = d.activeFile.Close()
	d.activeFile = nil
	d.writer = nil
	d.hints = nil
	d.option = nil
	d.logger = nil
}

func (d *DB) Put(key, value any, buckets ...string) error {
	_, err := d.put(key, value, Add, 0, false, buckets...)
	return err
}

func (d *DB) GetPut(key, value any, buckets ...string) ([][]byte, error) { // 返回之前的值
	return d.put(key, value, Add, 0, true, buckets...)
}

func (d *DB) PutTTL(key, value any, ttl uint64, buckets ...string) error {
	_, err := d.put(key, value, Add, ttl, false, buckets...)
	return err
}

func (d *DB) Get(key any, buckets ...string) ([][]byte, error) {
	return d.get(key, buckets...)
}

func (d *DB) Delete(key any, buckets ...string) error {
	_, err := d.put(key, nil, Delete, 0, false, buckets...)
	return err
}

func (d *DB) GetDelete(key any, buckets ...string) ([][]byte, error) {
	return d.put(key, nil, Delete, 0, true, buckets...)
}

func (d *DB) Prefix(prefix any, buckets ...string) ([][]byte, error) {
	prefixBytes, err := EncodeKey(prefix)
	if err != nil {
		return nil, err
	}
	condition := func(key []byte) bool {
		return bytes.HasPrefix(key, prefixBytes)
	}
	return d.iterator(condition, buckets...)
}

func (d *DB) Suffix(suffix any, buckets ...string) ([][]byte, error) {
	suffixBytes, err := EncodeKey(suffix)
	if err != nil {
		return nil, err
	}
	condition := func(key []byte) bool {
		return bytes.HasSuffix(key, suffixBytes)
	}
	return d.iterator(condition, buckets...)
}

func (d *DB) Range(start, end any, buckets ...string) ([][]byte, error) {
	startBytes, err := EncodeKey(start)
	if err != nil {
		return nil, err
	}
	endBytes, err := EncodeKey(end)
	if err != nil {
		return nil, err
	}
	condition := func(key []byte) bool {
		return bytes.Compare(key, startBytes) >= 0 && bytes.Compare(key, endBytes) <= 0
	}
	return d.iterator(condition, buckets...)
}

func (d *DB) CreateOrder(condition func([]byte, []byte) bool, buckets ...string) error {
	return d.createOrder(condition, buckets...)
}

func (d *DB) put(key, value any, entryType uint16, ttl uint64, prev bool, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		prevVal, err := d.singlePut(key, value, DefaultBucket, entryType, ttl, prev)
		return [][]byte{prevVal}, err
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		prevVal, err := d.singlePut(key, value, bucket, entryType, ttl, prev)
		if err != nil {
			return nil, err
		}
		res = append(res, prevVal)
	}
	return res, nil
}

func (d *DB) singlePut(key, value any, bucket string, entryType uint16, ttl uint64, prev bool) ([]byte, error) {
	d.fileMu.Lock()
	d.hintMu.Lock()
	defer d.fileMu.Unlock()
	defer d.hintMu.Unlock()
	if _, ok := d.buckets[bucket]; !ok {
		return nil, BucketNotExist
	}
	if _, ok := d.hints[bucket]; !ok {
		err := d.loadHinds(bucket)
		if err != nil {
			return nil, err
		}
	}
	entry, err := d.createEntry(key, value, bucket, entryType, ttl)
	if err != nil {
		return nil, err
	}
	d.updated = true
	_, err = d.writer.Write(entry.EncodeEntry())
	if err != nil {
		d.writer.Reset(d.activeFile)
		return nil, err
	}
	err = d.writer.Flush()
	if err != nil {
		d.writer.Reset(d.activeFile)
		return nil, err
	}
	prevItem := d.hints[bucket].Set(NewHint(entry.key, uint32(d.offset)))
	d.offset += entry.Size()
	if !prev || prevItem == nil {
		return nil, nil
	}
	hint := prevItem.(*Hint)
	prevEntry, err := d.readEntry(int64(hint.offset))
	if err != nil {
		return nil, err
	}
	if prevEntry.entryType != Delete && !prevEntry.IsExpired() {
		return prevEntry.value, nil
	}
	return nil, nil
}

func (d *DB) get(key any, buckets ...string) ([][]byte, error) {
	keyBytes, err := EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		val, err := d.singleGet(keyBytes, DefaultBucket)
		return [][]byte{val}, err
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		val, err := d.singleGet(keyBytes, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, nil
}

func (d *DB) singleGet(key []byte, bucket string) ([]byte, error) {
	d.fileMu.Lock()
	d.hintMu.Lock()
	defer d.fileMu.Unlock()
	defer d.hintMu.Unlock()
	if _, ok := d.buckets[bucket]; !ok {
		return nil, BucketNotExist
	}
	if _, ok := d.hints[bucket]; !ok {
		err := d.loadHinds(bucket)
		if err != nil {
			return nil, err
		}
	}
	hintItem := d.hints[bucket].Get(&Hint{key: key})
	if hintItem == nil {
		return nil, nil
	}
	hint := hintItem.(*Hint)
	entry, err := d.readEntry(int64(hint.offset))
	if err != nil {
		return nil, err
	}
	if entry.entryType != Delete && !entry.IsExpired() {
		return entry.value, nil
	}
	return nil, nil
}

func (d *DB) iterator(condition func([]byte) bool, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		return d.singleIterator(condition, DefaultBucket)
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		vals, err := d.singleIterator(condition, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, vals...)
	}
	return res, nil
}

func (d *DB) singleIterator(condition func([]byte) bool, bucket string) ([][]byte, error) {
	res := make([][]byte, 0)
	d.fileMu.Lock()
	d.hintMu.Lock()
	defer d.fileMu.Unlock()
	defer d.hintMu.Unlock()
	if _, ok := d.buckets[bucket]; !ok {
		return nil, BucketNotExist
	}
	if _, ok := d.hints[bucket]; !ok {
		err := d.loadHinds(bucket)
		if err != nil {
			return nil, err
		}
	}
	var err error
	d.hints[bucket].Ascend(nil, func(item any) bool {
		hint := item.(*Hint)
		if condition(hint.key) {
			var entry *Entry
			entry, err = d.readEntry(int64(hint.offset))
			if err != nil {
				return false
			}
			if entry.entryType != Delete && !entry.IsExpired() {
				res = append(res, entry.value)
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *DB) createOrder(condition func([]byte, []byte) bool, buckets ...string) error {
	if len(buckets) == 0 {
		return d.singleCreateOrder(condition, DefaultBucket)
	} else {
		for _, bucket := range buckets {
			err := d.singleCreateOrder(condition, bucket)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *DB) singleCreateOrder(condition func([]byte, []byte) bool, bucket string) error {
	d.fileMu.Lock()
	if _, ok := d.buckets[bucket]; !ok {
		return BucketNotExist
	}
	if _, ok := d.hints[bucket]; !ok {
		err := d.loadHinds(bucket)
		if err != nil {
			return err
		}
	}
	d.fileMu.Unlock()
	less := func(a, b interface{}) bool {
		i1, i2 := a.(*Hint), b.(*Hint)
		return condition(i1.key, i2.key)
	}
	newTree := btree.New(less)
	d.hintMu.Lock()
	d.hints[bucket].Ascend(nil, func(item any) bool {
		hint := item.(*Hint)
		newTree.Set(hint)
		return true
	})
	d.hints[bucket] = newTree
	d.hintMu.Unlock()
	return nil
}

func (d *DB) Count(buckets ...string) []int {
	if len(buckets) == 0 {
		return []int{d.hints[DefaultBucket].Len()}
	}
	res := make([]int, len(buckets))
	for _, bucket := range buckets {
		res = append(res, d.hints[bucket].Len())
	}
	return res
}

func (d *DB) createEntry(key, value any, bucket string, entryType uint16, ttl uint64) (*Entry, error) {
	keyBytes, err := EncodeKey(key)
	if err != nil {
		return nil, err
	}
	valueBytes, err := EncodeValue(value)
	if err != nil {
		return nil, err
	}
	entry := NewEntry(keyBytes, valueBytes, bucket, entryType, ttl)
	return entry, nil
}

func (d *DB) NewBucket(name string) error {
	if _, ok := d.buckets[name]; ok {
		return errors.New("bucket exist")
	}
	d.hints[name] = btree.New(hintLess)
	return nil
}

func (d *DB) Update(fn func(tx *TX) error) error {
	d.updated = true
	return d.Managed(true, fn)
}

func (d *DB) View(fn func(tx *TX) error) error {
	return d.Managed(false, fn)
}

func (d *DB) Managed(writable bool, fn func(tx *TX) error) error {
	tx, err := d.Begin(writable)
	if err != nil {
		return err
	}
	err = fn(tx)
	defer tx.Close()
	if writable {
		if err != nil || tx.failed {
			return fmt.Errorf("execute tx function failed: %v", err)
		}
		err = tx.Commit()
		if err != nil || tx.failed {
			return fmt.Errorf("commit tx failed: %v", err)
		}
	}
	return err
}

func (d *DB) initFileIo() error {
	hintPath := d.dbDir + BytesToString([]byte{os.PathSeparator}) + "hint.txt"
	d.hintPath = hintPath
	bucketPath := d.dbDir + BytesToString([]byte{os.PathSeparator}) + "bucket.txt"
	d.bucketPath = bucketPath
	var err error
	d.activeFile, err = os.OpenFile(d.option.DbPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	dbFileInfo, err := d.activeFile.Stat()
	if err != nil {
		_ = d.activeFile.Close()
		return err
	}
	d.offset = dbFileInfo.Size()
	d.writer = bufio.NewWriter(d.activeFile)
	err = d.loadBuckets()
	if err != nil {
		_ = d.activeFile.Close()
		return err
	}
	return nil
}

func (d *DB) loadBuckets() error {
	if _, err := os.Stat(d.bucketPath); os.IsNotExist(err) {
		d.buckets[DefaultBucket] = &Bucket{}
		d.hints[DefaultBucket] = btree.New(hintLess)
		return nil
	} else if err != nil {
		return err
	}
	bucketFile, err := mmap.Open(d.bucketPath)
	if err != nil {
		return err
	}
	defer bucketFile.Close()
	offset := int64(0)
	for {
		buf := make([]byte, bucketHeadSize)
		_, err = bucketFile.ReadAt(buf, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		bucket := DecodeBucketHeader(buf)
		offset += bucketHeadSize
		name := make([]byte, bucket.nameSize)
		_, err = bucketFile.ReadAt(name, offset)
		if err != nil {
			return err
		}
		bucket.name = name
		nameStr := BytesToString(name)
		d.buckets[nameStr] = bucket
		offset += int64(bucket.nameSize)
	}
	return nil
}

func (d *DB) loadHinds(name string) error {
	d.hints[name] = btree.New(hintLess)
	if _, err := os.Stat(d.bucketPath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	bucket := d.buckets[name]
	if bucket == nil {
		return BucketNotExist
	}
	offset := int64(bucket.offset)
	start := offset
	for {
		hintReader, err := mmap.Open(d.hintPath)
		if err != nil {
			return err
		}
		buf := make([]byte, hintHeadSize)
		_, err = hintReader.ReadAt(buf, offset)
		if err != nil {
			return err
		}
		err = hintReader.Close()
		if err != nil {
			return err
		}
		hint := DecodeHintHeader(buf)
		if hint.keySize > 0 {
			reader, err := mmap.Open(d.option.DbPath)
			if err != nil {
				return err
			}
			key := make([]byte, hint.keySize)
			_, err = reader.ReadAt(key, int64(hint.offset+entryHeadSize))
			if err != nil {
				return err
			}
			err = reader.Close()
			if err != nil {
				return err
			}
			hint.key = key
		}
		d.hints[name].Set(hint)
		offset += hintHeadSize
		if offset == start+int64(bucket.hintsSize) {
			break
		}
	}
	return nil
}

func (d *DB) readEntry(offset int64) (*Entry, error) {
	reader, err := mmap.Open(d.option.DbPath)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, entryHeadSize)
	_, err = reader.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	entry := DecodeEntryHeader(buf)
	offset += entryHeadSize
	dataSize := entry.DataSize()
	data := make([]byte, dataSize)
	_, err = reader.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	entry.key = data[:entry.keySize]
	entry.value = data[entry.keySize : entry.keySize+entry.valueSize]
	entry.bucket = data[entry.keySize+entry.valueSize:]
	if entry.Crc32(buf) != entry.entryCrc {
		return nil, fmt.Errorf("entry which key is %v crc32 check fail", BytesToString(entry.key))
	}
	err = reader.Close()
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (d *DB) buildHints() error {

	hintFile, err := os.OpenFile(d.hintPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	bucketFile, err := os.OpenFile(d.bucketPath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	info, err := hintFile.Stat()
	if err != nil {
		_ = hintFile.Close()
		return err
	}
	offset := uint64(info.Size())
	hintWritter := bufio.NewWriter(hintFile)
	bucketWritter := bufio.NewWriter(bucketFile)
	for name, hints := range d.hints {
		start := offset
		bucket := &Bucket{nameSize: uint32(len(name)), offset: start, name: StringToBytes(name)}
		hints.Ascend(nil, func(item any) bool {
			hint := item.(*Hint)
			_, err = hintWritter.Write(hint.EncodeHintHeader())
			offset += hintHeadSize
			if err != nil {
				return false
			}
			return true
		})
		bucket.hintsSize = offset - start
		_, err = bucketWritter.Write(bucket.EncodeBucket())
		if err != nil {
			break
		}
	}
	if err != nil {
		hintWritter.Reset(hintFile)
		bucketWritter.Reset(bucketFile)
		_ = hintFile.Close()
		_ = bucketFile.Close()
		_ = os.Remove(d.hintPath)
		_ = os.Remove(d.bucketPath)
		return err
	}
	err = hintWritter.Flush()
	if err != nil {
		hintWritter.Reset(hintFile)
		bucketWritter.Reset(bucketFile)
		_ = hintFile.Close()
		_ = bucketFile.Close()
		_ = os.Remove(d.hintPath)
		_ = os.Remove(d.bucketPath)
		return err
	}
	err = bucketWritter.Flush()
	if err != nil {
		hintWritter.Reset(hintFile)
		bucketWritter.Reset(bucketFile)
		_ = hintFile.Close()
		_ = bucketFile.Close()
		_ = os.Remove(d.hintPath)
		_ = os.Remove(d.bucketPath)
		return err
	}
	_ = hintFile.Sync()
	_ = bucketFile.Sync()
	_ = hintFile.Close()
	_ = bucketFile.Close()
	return nil
}
