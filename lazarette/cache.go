package lazarette

import (
	"errors"
	"sync"

	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store"
	"go.uber.org/zap"
)

//go:generate protoc -I ./ --go_out=plugins=grpc:./ ./lazarette.proto

var ErrNotNew = errors.New("a newer value was found; not setting")

// Cache .
type Cache struct {
	store store.Store

	subsMu sync.RWMutex
	subs   map[string][]chan *KeyValue

	log *zap.Logger
}

func New(store store.Store, opts ...Option) (*Cache, error) {
	options := options{
		logger: log.P,
	}

	for _, o := range opts {
		o.apply(&options)
	}

	c := &Cache{
		store: store,
		subs:  make(map[string][]chan *KeyValue),
		log:   options.logger,
	}

	return c, nil
}

func (c *Cache) Close() error {
	c.log.Info("Closing lazarette Cache")
	return c.store.Close()
}

func (c *Cache) Clean() error {
	c.log.Info("Cleaning lazarette Cache")
	return c.store.Clean()
}
