package store

// Store .
type Store interface {
	Get(key []byte) ([]byte, error)
	GetPrefix(prefix []byte) ([]KeyValue, error)
	Set(key, val []byte) error

	Close() error
	Clean() error
}

type KeyValue struct {
	Key   []byte
	Value []byte
}
