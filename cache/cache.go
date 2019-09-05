package cache

import "errors"

var (
	// ErrKeyNotFound .
	ErrKeyNotFound = errors.New("key not found in cache")
)

// Cache .
type Cache interface {
	Get(key string) ([]byte, error)
	Put(key string, val []byte) error

	// Subscribe(key string) (chan Value, error)
}

// Value .
type Value interface {
}
