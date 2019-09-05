package store

// Store .
type Store interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, val []byte) error

	// DumpPrefix()
	// Dump() (map[[]byte][]byte, error)
}
