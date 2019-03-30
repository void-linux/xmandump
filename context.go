package main

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type contextKey int

const (
	ctxLogger contextKey = iota
)

func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxLogger, logger)
}

func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	return WithLogger(ctx, Logger(ctx).With(fields...))
}

func Logger(ctx context.Context) *zap.Logger {
	if logger, ok := ctx.Value(ctxLogger).(*zap.Logger); ok && logger != nil {
		return logger
	}
	return defaultLogger
}

func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

func Panic(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Panic(msg, fields...)
}

func DPanic(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).WithOptions(zap.AddCallerSkip(1)).DPanic(msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	Logger(ctx).WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

func Elapsed(key string) func() zap.Field {
	now := time.Now()
	return func() zap.Field {
		return zap.Duration(key, time.Since(now))
	}
}
