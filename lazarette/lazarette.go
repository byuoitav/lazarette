package lazarette

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/byuoitav/lazarette/cache"
	"github.com/byuoitav/lazarette/store"
	"github.com/byuoitav/lazarette/store/boltstore"
	bolt "go.etcd.io/bbolt"
)

type lazarette struct {
	store store.Store
}

// Open .
func Open(path string) (cache.Cache, error) {
	path = filepath.Clean(path)
	options := &bolt.Options{
		Timeout: 2 * time.Second,
	}

	db, err := bolt.Open(path+"/lazarette.bolt", 0666, options)
	if err != nil {
		return nil, fmt.Errorf("unable to open bolt: %v", err)
	}

	store, err := boltstore.NewStore(db)
	if err != nil {
		return nil, fmt.Errorf("unable to create boltstore: %v", err)
	}

	c := &lazarette{
		store: store,
	}

	return c, nil
}

// Get .
func (l *lazarette) Get(key string) ([]byte, error) {
	val, err := l.store.Get([]byte(key))
	switch {
	case err != nil:
		return nil, err
	case val == nil:
		return nil, cache.ErrKeyNotFound
	}

	return val, nil
}

// Put .
func (l *lazarette) Put(key string, val []byte) error {
	return l.store.Put([]byte(key), val)
}
