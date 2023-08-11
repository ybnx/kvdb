package kvdb

import "bytes"

type Operator struct{}

func (o *Operator) Put(key, value any, buckets ...string) error {
	return o.PutTTL(key, value, 0, buckets...)
}

func (o *Operator) GetPut(key, value any, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		prev, err := o.put(key, value, DefaultBucket, Add, 0, true)
		return [][]byte{prev}, err
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		prev, err := o.put(key, value, bucket, Add, 0, true)
		if err != nil {
			return nil, err
		}
		res = append(res, prev)
	}
	return res, nil
}

func (o *Operator) PutTTL(key, value any, ttl uint64, buckets ...string) error {
	if len(buckets) == 0 {
		_, err := o.put(key, value, DefaultBucket, Add, ttl, false)
		return err
	}
	for _, bucket := range buckets {
		_, err := o.put(key, value, bucket, Add, ttl, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Operator) Get(key any, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		val, err := o.get(key, DefaultBucket)
		return [][]byte{val}, err
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		val, err := o.get(key, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, nil
}

func (o *Operator) Delete(key any, buckets ...string) error {
	if len(buckets) == 0 {
		_, err := o.put(key, nil, DefaultBucket, Delete, 0, false)
		return err
	}
	for _, bucket := range buckets {
		_, err := o.put(key, nil, bucket, Delete, 0, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Operator) GetDelete(key any, buckets ...string) ([][]byte, error) {
	if len(buckets) == 0 {
		prev, err := o.put(key, nil, DefaultBucket, Delete, 0, true)
		return [][]byte{prev}, err
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		prev, err := o.put(key, nil, bucket, Delete, 0, true)
		if err != nil {
			return nil, err
		}
		res = append(res, prev)
	}
	return res, nil
}

func (o *Operator) Prefix(prefix any, buckets ...string) ([][]byte, error) {
	prefixBytes, err := EncodeKey(prefix)
	if err != nil {
		return nil, err
	}
	condition := func(key []byte) bool {
		return bytes.HasPrefix(key, prefixBytes)
	}
	if len(buckets) == 0 {
		return o.iterator(condition, DefaultBucket)
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		vals, err := o.iterator(condition, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, vals...)
	}
	return res, nil
}

func (o *Operator) Suffix(suffix any, buckets ...string) ([][]byte, error) {
	suffixBytes, err := EncodeKey(suffix)
	if err != nil {
		return nil, err
	}
	condition := func(key []byte) bool {
		return bytes.HasSuffix(key, suffixBytes)
	}
	if len(buckets) == 0 {
		return o.iterator(condition, DefaultBucket)
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		vals, err := o.iterator(condition, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, vals...)
	}
	return res, nil
}

func (o *Operator) Range(start, end any, buckets ...string) ([][]byte, error) {
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
	if len(buckets) == 0 {
		return o.iterator(condition, DefaultBucket)
	}
	res := make([][]byte, 0, len(buckets))
	for _, bucket := range buckets {
		vals, err := o.iterator(condition, bucket)
		if err != nil {
			return nil, err
		}
		res = append(res, vals...)
	}
	return res, nil
}

func (o *Operator) put(key, value any, bucket string, entryType uint16, ttl uint64, prev bool) ([]byte, error) {
	return nil, nil
}

func (o *Operator) get(key any, bucket string) ([]byte, error) {
	return nil, nil
}

func (o *Operator) iterator(condition func([]byte) bool, bucket string) ([][]byte, error) {
	return nil, nil
}
