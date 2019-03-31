package main

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	defaultLogger = zap.NewNop()
)

func NewLogger(level zap.AtomicLevel) (*zap.Logger, error) {
	conf := zap.NewProductionConfig()
	conf.Level = level
	conf.Encoding = "console"
	conf.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	conf.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	conf.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	conf.Sampling = nil // Disable rate limiting -- this is a CLI tool, we don't care too much.
	return conf.Build()
}

func logRepoData(file string) zap.Field {
	return zap.String("repodata", file)
}

func logFile(file string) zap.Field {
	return zap.String("file", file)
}

func logDumpFile(file string) zap.Field {
	return zap.String("dumpfile", file)
}

func logPkgFile(file string) zap.Field {
	return zap.String("pkgfile", file)
}
