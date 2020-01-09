package lazarette

import (
	"errors"
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
	store store.Store

	pStore   store.Store
	interval time.Duration

	// map of [prefix][]*Subscription
	subs sync.Map

	log *zap.Logger

	// stop all goroutines created by this cache
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
		store: store,
		kill:  make(chan struct{}),

		pStore:   options.pStore,
		interval: options.interval,

		log: options.logger,
	}

	if c.interval > 0 {
		if err := c.restore(); err != nil {
			return nil, err
		}

		go c.persist()
	}

	return c, nil
}

// Close closes the cache
func (c *Cache) Close() error {
	c.log.Info("Closing lazarette Cache")

	close(c.kill)

	if c.interval > 0 {
		if err := c.pStore.Close(); err != nil {
			return err
		}
	}

	return c.store.Close()
}

// Clean cleans the cache
func (c *Cache) Clean() error {
	c.log.Info("Cleaning lazarette Cache")

	// TODO do we want to clean? i don't think so
	// bc it should just be a 5 minute backup, so it will get wiped in a little while
	if c.interval > 0 {
		if err := c.pStore.Clean(); err != nil {
			return err
		}
	}

	return c.store.Clean()
}
