package boltstore

import (
	"bytes"
	"fmt"

	"github.com/byuoitav/lazarette/store"
	bolt "go.etcd.io/bbolt"
)

// DefaultBucket .
var DefaultBucket = []byte{0x00}

type boltstore struct {
	*bolt.DB
}

// NewStore .
func NewStore(db *bolt.DB) (store.Store, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(DefaultBucket)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create default bucket: %v", err)
	}

	s := &boltstore{
		DB: db,
	}

	return s, nil
}

func (s *boltstore) Clean() error {
	return s.Update(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			return tx.DeleteBucket(name)
		})
		if err != nil {
			return fmt.Errorf("failed to delete old buckets: %v", err)
		}

		_, err = tx.CreateBucketIfNotExists(DefaultBucket)
		if err != nil {
			return fmt.Errorf("failed to create default bucket: %v", err)
		}

		return nil
	})
}

// Get .
func (s *boltstore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(DefaultBucket)
		if bucket == nil {
			return fmt.Errorf("no %v bucket found", DefaultBucket)
		}

		tmp := bucket.Get(key)
		if tmp != nil {
			val = make([]byte, len(tmp))
			copy(val, tmp)
		}

		return nil
	})

	return val, err
}

// Set .
func (s *boltstore) Set(key, val []byte) error {
	err := s.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(DefaultBucket)
		if bucket == nil {
			return fmt.Errorf("no %v bucket found", DefaultBucket)
		}

		return bucket.Put(key, val)
	})

	return err
}

func (s *boltstore) GetPrefix(prefix []byte) ([]store.KeyValue, error) {
	var kvs []store.KeyValue

	err := s.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(DefaultBucket)
		if bucket == nil {
			return fmt.Errorf("no %v bucket found", DefaultBucket)
		}

		return bucket.ForEach(func(key, value []byte) error {
			if bytes.HasPrefix(key, prefix) {
				v := make([]byte, len(value))
				copy(v, value)

				kvs = append(kvs, store.KeyValue{
					Key:   key,
					Value: v,
				})
			}

			return nil
		})
	})

	return kvs, err
}

func (s *boltstore) Dump() ([]store.KeyValue, error) {
	var kvs []store.KeyValue

	err := s.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(DefaultBucket)
		if bucket == nil {
			return fmt.Errorf("no %v bucket found", DefaultBucket)
		}

		return bucket.ForEach(func(key, value []byte) error {
			v := make([]byte, len(value))
			copy(v, value)

			kvs = append(kvs, store.KeyValue{
				Key:   key,
				Value: v,
			})

			return nil
		})
	})

	return kvs, err
}
