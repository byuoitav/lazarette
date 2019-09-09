package lazarette

import (
	"errors"
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

	subs map[string][]chan cache.Value
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
		subs:  make(map[string][]chan cache.Value),
	}

	return c, nil
}

// Get .
func (l *lazarette) Get(key string) (*cache.Value, error) {
	data, err := l.store.Get([]byte(key))
	switch {
	case err != nil:
		return nil, err
	case data == nil:
		return nil, cache.ErrKeyNotFound
	}

	val := &cache.Value{}
	err = val.UnmarshalBinary(data)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal val: %v", err)
	}

	return val, nil
}

// Set .
func (l *lazarette) Set(key string, val *cache.Value) error {
	if val == nil {
		return errors.New("value must not be nil")
	}

	// get the current val
	cur, err := l.Get(key)
	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		return fmt.Errorf("unable to get current value: %v", err)
	}

	if cur != nil && cur.Timestamp.After(val.Timestamp) {
		return cache.ErrOutOfDate
	}

	bytes, err := val.MarshalBinary()
	if err != nil {
		return fmt.Errorf("unable to marshal new value: %v", err)
	}

	return l.store.Put([]byte(key), bytes)
}
