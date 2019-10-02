package syncmapstore

import (
	"sync"

	"github.com/byuoitav/lazarette/store"
)

type syncmapstore struct {
	*sync.Map
}

// NewStore .
func NewStore() (store.Store, error) {
	return &syncmapstore{
		Map: &sync.Map{},
	}, nil
}

// Get .
func (s *syncmapstore) Get(key []byte) ([]byte, error) {
	var val []byte

	data, ok := s.Load(string(key))
	if ok {
		if buf, ok := data.([]byte); ok {
			val = make([]byte, len(buf))
			copy(val, buf)
		}
	}

	return val, nil
}

// Put .
func (s *syncmapstore) Set(key, val []byte) error {
	s.Store(string(key), val)
	return nil
}

// Clean .
func (s *syncmapstore) Clean() error {
	s.Map = &sync.Map{}
	return nil
}

// Close .
func (s *syncmapstore) Close() error {
	return nil
}
