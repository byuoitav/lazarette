package memstore

import (
	"strings"
	"sync"

	"github.com/byuoitav/lazarette/store"
)

type memstore struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// NewStore .
func NewStore() (store.Store, error) {
	return &memstore{
		m: make(map[string][]byte),
	}, nil
}

// Get .
func (s *memstore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var val []byte
	if data, ok := s.m[string(key)]; ok {
		val = make([]byte, len(data))
		copy(val, data)
	}

	return val, nil
}

// Set .
func (s *memstore) Set(key, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m[string(key)] = val
	return nil
}

// Clean .
func (s *memstore) Clean() error {
	s.mu.Lock()
	s.m = make(map[string][]byte)
	s.mu.Unlock()

	return nil
}

// Close .
func (s *memstore) Close() error {
	return nil
}

func (s *memstore) GetPrefix(prefix []byte) ([]store.KeyValue, error) {
	var kvs []store.KeyValue

	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, value := range s.m {
		if strings.HasPrefix(key, string(prefix)) {
			v := make([]byte, len(value))
			copy(v, value)

			kvs = append(kvs, store.KeyValue{
				Key:   []byte(key),
				Value: v,
			})
		}
	}

	return kvs, nil
}
