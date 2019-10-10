package syncmapstore

import (
	"strings"
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

func (s *syncmapstore) Close() error {
	s.Map = nil
	return nil
}

func (s *syncmapstore) GetPrefix(prefix []byte) ([]store.KeyValue, error) {
	var kvs []store.KeyValue

	s.Map.Range(func(key, value interface{}) bool {
		k := key.(string)

		if strings.HasPrefix(k, string(prefix)) {
			if buf, ok := value.([]byte); ok {
				v := make([]byte, len(buf))
				copy(v, buf)

				kvs = append(kvs, store.KeyValue{
					Key:   []byte(k),
					Value: v,
				})
			}
		}

		return true
	})

	return kvs, nil
}
