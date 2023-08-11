package kvdb

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/tidwall/btree"
)

type TX struct {
	db                         *DB
	writable, commited, failed bool
	commitEntrys               map[string]*btree.BTree
	commitHints                map[string]map[string]*Hint
}

var (
	NotWritable    = errors.New("not writable")
	DatabaseClosed = errors.New("database had closed")
	BucketNotExist = errors.New("bucket not exist")
)

func (t *TX) Put(key, value any, buckets ...string) error {
	if t.db == nil {
		t.failed = true
		return DatabaseClosed
	}
	if !t.writable {
		t.failed = true
		return NotWritable
	}
	_, err := t.put(key, value, Add, 0, false, buckets...)
	return err
}

func (t *TX) GetPut(key, value any, buckets ...string) ([][]byte, error) {
	if t.db == nil {
		t.failed = true
		return nil, DatabaseClosed
	}
	if !t.writable {
		t.failed = true
		return nil, NotWritable
	}
	return t.put(key, value, Add, 0, true, buckets...)
}

func (t *TX) PutTTL(key, value any, ttl uint64, buckets ...string) error {
	if t.db == nil {
		t.failed = true
		return DatabaseClosed
	}
	if !t.writable {
		t.failed = true
		return NotWritable
	}
	_, err := t.put(key, value, Add, ttl, false, buckets...)
	return err
}

func (t *TX) Get(key any, buckets ...string) ([][]byte, error) {
	if t.db == nil {
		return nil, DatabaseClosed
	}
	return t.get(key, buckets...)

}

func (t *TX) Delete(key any, buckets ...string) error {
	if t.db == nil {
		t.failed = true
		return DatabaseClosed
	}
	if !t.writable {
		t.failed = true
		return NotWritable
	}
	_, err := t.put(key, nil, Delete, 0, false, buckets...)
	return err
}

func (t *TX) GetDelete(key any, buckets ...string) ([][]byte, error) {
	if t.db == nil {
		t.failed = true
		return nil, DatabaseClosed
	}
	if !t.writable {
		t.failed = true
		return nil, NotWritable
	}
	return t.put(key, nil, Delete, 0, true, buckets...)
}

func (t *TX) Prefix(prefix any, buckets ...string) ([][]byte, error) {
	if t.db == nil {
		return nil, DatabaseClosed
	}
	prefixBytes, err := EncodeKey(prefix)
	if err != nil {
		return nil, fmt.Errorf("encode prefix failed: %v", err)
	}
	condition := func(key []byte) bool {
		return bytes.HasPrefix(key, prefixBytes)
	}
	return t.iterator(condition, buckets...)
}

func (t *TX) Suffix(suffix any, buckets ...string) ([][]byte, error) {
	if t.db == nil {
		return nil, DatabaseClosed
	}
	suffixBytes, err := EncodeKey(suffix)
	if err != nil {
		return nil, fmt.Errorf("encode suffix failed: %v", err)
	}
	condition := func(key []byte) bool {
		return bytes.HasSuffix(key, suffixBytes)
	}
	return t.iterator(condition, buckets...)
}

func (t *TX) Range(start, end any, buckets ...string) ([][]byte, error) {
	if t.db == nil {
		return nil, DatabaseClosed
	}
	startBytes, err := EncodeKey(start)
	if err != nil {
		return nil, fmt.Errorf("encode start failed: %v", err)
	}
	endBytes, err := EncodeKey(end)
	if err != nil {
		return nil, fmt.Errorf("encode end failed: %v", err)
	}
	condition := func(key []byte) bool {
		return bytes.Compare(key, startBytes) >= 0 && bytes.Compare(key, endBytes) <= 0
	}
	return t.iterator(condition, buckets...)
}

func (t *TX) put(key, value any, entryType uint16, ttl uint64, prev bool, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		prevVal, err := t.singlePut(key, value, DefaultBucket, entryType, ttl, prev)
		return [][]byte{prevVal}, err
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		prevVal, err := t.singlePut(key, value, bucket, entryType, ttl, prev)
		if err != nil {
			t.failed = true
			return nil, err
		}
		res = append(res, prevVal)
	}
	return res, nil
}

func (t *TX) singlePut(key, value any, bucket string, entryType uint16, ttl uint64, prev bool) ([]byte, error) {
	if _, ok := t.db.buckets[bucket]; !ok {
		t.failed = true
		return nil, BucketNotExist
	}
	if _, ok := t.db.hints[bucket]; !ok {
		err := t.db.loadHinds(bucket)
		if err != nil {
			t.failed = true
			return nil, err
		}
	}
	entry, err := t.db.createEntry(key, value, bucket, entryType, ttl)
	if err != nil {
		t.failed = true
		return nil, err
	}
	if _, ok := t.commitEntrys[bucket]; !ok {
		t.commitEntrys[bucket] = btree.New(entryLess)
	}
	prevItem := t.commitEntrys[bucket].Set(entry)
	if !prev {
		return nil, nil
	}
	if prevItem == nil {
		return t.db.singleGet(entry.key, bucket)
	}
	prevEntry := prevItem.(*Entry)
	if prevEntry.entryType != Delete && prevEntry.IsExpired() {
		return prevEntry.value, nil
	}
	return nil, nil
}

func (t *TX) get(key any, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		val, err := t.singleGet(key, DefaultBucket)
		return [][]byte{val}, err
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		val, err := t.singleGet(key, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, nil
}

func (t *TX) singleGet(key any, bucket string) ([]byte, error) {
	if _, ok := t.db.buckets[bucket]; !ok {
		return nil, BucketNotExist
	}
	if _, ok := t.db.hints[bucket]; !ok {
		err := t.db.loadHinds(bucket)
		if err != nil {
			return nil, err
		}
	}
	keyBytes, err := EncodeKey(key)
	if err != nil {
		return nil, err
	}
	if _, ok := t.commitEntrys[bucket]; !ok {
		t.commitEntrys[bucket] = btree.New(entryLess)
	}
	item := t.commitEntrys[bucket].Get(&Entry{key: keyBytes, bucket: StringToBytes(bucket)})
	if item == nil {
		return t.db.singleGet(keyBytes, bucket)
	}
	entry := item.(*Entry)
	if entry.entryType != Delete && !entry.IsExpired() {
		return entry.value, nil
	}
	return nil, nil
}

func (t *TX) iterator(condition func([]byte) bool, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		return t.singleIterator(condition, DefaultBucket)
	}
	res := make([][]byte, 0)
	for _, bucket := range buckets {
		vals, err := t.singleIterator(condition, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, vals...)
	}
	return res, nil
}

func (t *TX) singleIterator(condition func([]byte) bool, bucket string) ([][]byte, error) {
	if _, ok := t.db.buckets[bucket]; !ok {
		return nil, BucketNotExist
	}
	if _, ok := t.db.hints[bucket]; !ok {
		err := t.db.loadHinds(bucket)
		if err != nil {
			return nil, err
		}
	}
	var err error
	res := make([][]byte, 0)
	t.db.fileMu.Lock()
	t.db.hints[bucket].Ascend(nil, func(item any) bool {
		hint := item.(*Hint)
		if _, ok := t.commitEntrys[bucket]; !ok {
			t.commitEntrys[bucket] = btree.New(entryLess)
		}
		entryItem := t.commitEntrys[bucket].Get(&Entry{key: hint.key})
		if entryItem == nil && condition(hint.key) {
			var entry *Entry
			entry, err = t.db.readEntry(int64(hint.offset))
			if err != nil {
				return false
			}
			if entry.entryType != Delete && !entry.IsExpired() {
				res = append(res, entry.value)
			}
		}
		return true
	})
	t.db.fileMu.Unlock()
	if err != nil {
		return nil, err
	}
	if _, ok := t.commitEntrys[bucket]; !ok {
		t.commitEntrys[bucket] = btree.New(entryLess)
	}
	t.commitEntrys[bucket].Ascend(nil, func(item any) bool {
		entry := item.(*Entry)
		if condition(entry.key) {
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

func (d *DB) Begin(writable bool) (*TX, error) {
	if d.closed {
		return nil, DatabaseClosed
	}
	tx := &TX{
		db:           d,
		writable:     writable,
		commited:     false,
		failed:       false,
		commitEntrys: make(map[string]*btree.BTree, len(d.buckets)),
		commitHints:  make(map[string]map[string]*Hint),
	}
	return tx, nil
}

func (t *TX) Commit() error {
	if t.db == nil {
		t.failed = true
		return DatabaseClosed
	}
	var err error
	t.commited = true
	t.db.fileMu.Lock()
	txOffset := t.db.offset
	for bucket, entrys := range t.commitEntrys {
		entrys.Ascend(nil, func(item any) bool {
			entry := item.(*Entry)
			if !entry.IsExpired() {
				_, err = t.db.writer.Write(entry.EncodeEntry())
				if err != nil {
					return false
				}
				if _, ok := t.commitHints[bucket]; !ok {
					t.commitHints[bucket] = make(map[string]*Hint)
				}
				t.commitHints[bucket][BytesToString(entry.key)] = NewHint(entry.key, uint32(txOffset))
				txOffset += entry.Size()
			}
			return true
		})
	}
	if err != nil {
		t.failed = true
		t.db.writer.Reset(t.db.activeFile)
		return err
	}
	err = t.db.writer.Flush()
	if err != nil {
		t.failed = true
		t.db.writer.Reset(t.db.activeFile)
		return err
	}
	t.db.offset = txOffset
	t.db.fileMu.Unlock()
	t.db.hintMu.Lock()
	for bucket, hints := range t.commitHints {
		if _, ok := t.db.buckets[bucket]; !ok {
			t.failed = true
			return BucketNotExist
		}
		for _, hint := range hints {
			t.db.hints[bucket].Set(hint)
		}
	}
	t.db.hintMu.Unlock()
	return nil
}

func (t *TX) Close() {
	t.db = nil
	t.commitEntrys = nil
	t.commitHints = nil
}
