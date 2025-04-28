package cache

import (
	"errors"
	"sync"
)

type KVStore struct {
	store map[int]struct{}
	mu    sync.Mutex
}

func NewCache() *KVStore {
	return &KVStore{
		store: make(map[int]struct{}),
	}
}

func (kv *KVStore) Set(key int) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.store[key]; exists {
		return errors.New("specified key already exists in the cache")
	}

	kv.store[key] = struct{}{}
	return nil
}

func (kv *KVStore) GetAll() []int {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var keys []int

	for k := range kv.store {
		keys = append(keys, k)
	}

	return keys
}

func (kv *KVStore) SetAll(vals []int) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, val := range vals {
		kv.Set(val)
	}

	return nil
}
