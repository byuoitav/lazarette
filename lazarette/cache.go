package lazarette

import (
	"errors"
	fmt "fmt"
	"sync"
	"time"

	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store"
	"go.uber.org/zap"
)

//go:generate protoc -I ./ --go_out=plugins=grpc:./ ./lazarette.proto

// ErrNotNew is returned when a newer value is found and a value is not set
var ErrNotNew = errors.New("a newer value was found; not setting")

// Cache .
type Cache struct {
	store  store.Store
	pStore store.Store

	interval time.Duration

	subsMu sync.RWMutex
	subs   map[string][]chan *KeyValue

	replsMu sync.RWMutex
	repls   []*replication

	log *zap.Logger

	kill chan struct{}
}

// New returns a new cache from a store
func New(store store.Store, opts ...Option) (*Cache, error) {
	options := options{
		logger: log.P,
	}

	for _, o := range opts {
		o.apply(&options)
	}

	c := &Cache{
		store:    store,
		pStore:   options.pStore,
		interval: options.interval,

		subs: make(map[string][]chan *KeyValue),
		log:  options.logger,
	}

	var err error

	if c.interval > 0 {
		c.kill = make(chan struct{})
		err = c.restore()
		go c.persist()
	}

	return c, err
}

// Close closes the cache
func (c *Cache) Close() error {
	c.log.Info("Closing lazarette Cache")
	if c.interval > 0 {
		c.log.Info("Closing persistent store")
		close(c.kill)
		if err := c.pStore.Close(); err != nil {
			c.log.Warn("failed to close persistent store")
			return err
		}
	}
	return c.store.Close()
}

// Clean cleans the cache
func (c *Cache) Clean() error {
	c.log.Info("Cleaning lazarette Cache")
	if c.interval > 0 {
		c.log.Info("Cleaning persistent store")
		if err := c.pStore.Clean(); err != nil {
			c.log.Warn("failed to clean persistent store")
			return err
		}
	}

	return c.store.Clean()
}

func (c *Cache) persist() {
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.log.Info("dumping cache")
			kvs, err := c.store.Dump()
			if err != nil {
				c.log.Warn("failed to dump from memory store", zap.Error(err))
				continue
			}
			if err = c.pStore.Clean(); err != nil {
				c.log.Warn("failed to clear persistent store", zap.Error(err))
				continue
			}
			for _, kv := range kvs {
				if err = c.pStore.Set(kv.Key, kv.Value); err != nil {
					c.log.Warn(fmt.Sprintf("failed to set %s - %s:", kv.Key, kv.Value), zap.Error(err))
					continue
				}
			}
		case <-c.kill:
			return
		}

	}
}

func (c *Cache) restore() error {
	c.log.Info("restoring from persistent storage")

	kvs, err := c.pStore.Dump()
	if err != nil {
		c.log.Warn("failed to dump from persistent store", zap.Error(err))
		return err
	}
	if err = c.store.Clean(); err != nil {
		c.log.Warn("failed to clear memory store", zap.Error(err))
		return err
	}
	errCount := 0
	for _, kv := range kvs {
		if err = c.store.Set(kv.Key, kv.Value); err != nil {
			c.log.Warn(fmt.Sprintf("failed to set %s - %s:", kv.Key, kv.Value), zap.Error(err))
			errCount++
			continue
		}
	}
	if errCount > 0 {
		c.log.Warn(fmt.Sprintf("encountered %v errors setting key-value pairs", errCount))
		return fmt.Errorf("encountered error(s) restoring data from persistent storage")
	}
	return nil
}
