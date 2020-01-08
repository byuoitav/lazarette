package lazarette

import (
	"time"

	"github.com/byuoitav/lazarette/store"
	"go.uber.org/zap"
)

type options struct {
	logger   *zap.Logger
	pStore   store.Store
	interval time.Duration
}

// Option is an option to the cache struct
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

// WithLogger adds a logger to the cache
func WithLogger(l *zap.Logger) Option {
	return optionFunc(func(o *options) {
		o.logger = l
	})
}

// WithPersistent adds a persistent store to the cache
func WithPersistent(p store.Store, d time.Duration) Option {
	return optionFunc(func(o *options) {
		o.pStore = p
		o.interval = d
	})
}
