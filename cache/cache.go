package cache

import (
	"errors"
)

var (
	// ErrKeyNotFound .
	ErrKeyNotFound = errors.New("key not found in cache")

	// ErrOutOfDate .
	ErrOutOfDate = errors.New("a newer value was found; not setting")
)

// Cache .
type Cache interface {
	Get(key string) (*Value, error)
	Set(key string, val *Value) error

	// Subscribe(prefix string) (chan Value, error)
}
