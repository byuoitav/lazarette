package store

// Store .
type Store interface {
	Get(key []byte) ([]byte, error)
	Set(key, val []byte) error

	Close() error
	Clean() error

	// DumpPrefix()
	// Dump() (map[[]byte][]byte, error)
}
