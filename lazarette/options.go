package lazarette

import "go.uber.org/zap"

type options struct {
	logger *zap.Logger
}

type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

func WithLogger(l *zap.Logger) Option {
	return optionFunc(func(o *options) {
		o.logger = l
	})
}
