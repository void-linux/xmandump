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
	return conf.Build()
}

func logFile(file string) zap.Field {
	return zap.String("file", file)
}

func logPkgFile(file string) zap.Field {
	return zap.String("pkgfile", file)
}
