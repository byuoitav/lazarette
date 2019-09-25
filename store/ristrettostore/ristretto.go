package ristrettostore

import (
	"fmt"
	"sync"

	"github.com/byuoitav/lazarette/store"
	"github.com/dgraph-io/ristretto"
)

type ristrettostore struct {
	*ristretto.Cache

	keys *sync.Map
}

func NewStore(maxCost int64) (store.Store, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 5 * maxCost,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to build cache: %v", err)
	}

	s := &ristrettostore{
		Cache: cache,
		keys:  &sync.Map{},
	}

	return s, nil
}

func (r *ristrettostore) Clean() error {
	r.keys.Range(func(key, value interface{}) bool {
		r.Cache.Del(key)
		return true
	})

	return nil
}

func (r *ristrettostore) Close() error {
	r.Cache.Close()
	return nil
}

func (r *ristrettostore) Get(key []byte) ([]byte, error) {
	var val []byte

	data, ok := r.Cache.Get(string(key))
	if ok {
		if buf, ok := data.([]byte); ok {
			val = make([]byte, len(buf))
			copy(val, buf)
		}
	}

	return val, nil
}

func (r *ristrettostore) Set(key, val []byte) error {
	r.keys.Store(string(key), struct{}{})

	// TODO compute cost?
	ok := r.Cache.Set(string(key), val, 1)
	if !ok {
		return fmt.Errorf("set was dropped and not saved to store")
	}

	return nil
}
