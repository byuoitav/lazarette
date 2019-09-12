package log

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var plain *zap.Logger
var cfg zap.Config

func init() {
	var err error
	cfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding: "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "@",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.FullCallerEncoder,
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	plain, err = cfg.Build()
	if err != nil {
		log.Fatalf("failed to initialize zap logger: %v", err)
	}

	plain.Info("Zap logger started")

	defer plain.Sync()
}

// Debug .
func Debug(msg string, fields ...zap.Field) {
	plain.Debug(msg, fields...)
}

// Info .
func Info(msg string, fields ...zap.Field) {
	plain.Info(msg, fields...)
}

// Warn .
func Warn(msg string, fields ...zap.Field) {
	plain.Warn(msg, fields...)
}

// Error .
func Error(msg string, fields ...zap.Field) {
	plain.Error(msg, fields...)
}

// Fatal .
func Fatal(msg string, fields ...zap.Field) {
	plain.Fatal(msg, fields...)
}

// Named .
func Named(s string) *zap.Logger {
	return plain.Named(s)
}
