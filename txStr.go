package kvdb

func (t *TX) PutString(key, value string) error {
	return t.Put(key, value)
}

func (t *TX) PutTTLString(key, value string, ttl uint64) error {
	return t.PutTTL(key, value, ttl)
}

func (t *TX) GetPutString(key, value string) (string, error) {
	data, err := t.GetPut(key, value)
	return string(data[0]), err
}

func (t *TX) GetString(key string) (string, error) {
	data, err := t.Get(key)
	return string(data[0]), err
}

func (t *TX) DeleteString(key string) error {
	return t.Delete(key)
}

// GetDeleteString 删除并获取key对应value
func (t *TX) GetDeleteString(key string) (string, error) {
	data, err := t.Get(key)
	err = t.Delete(key)
	if err != nil {
		return "", err
	}
	return string(data[0]), err
}

func (t *TX) RangeString(start, end string) ([]string, error) {
	return nil, nil
}
