package db

import (
	"context"
	"log/slog"
)

type loggerCtxKey struct{}

func ContextWithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerCtxKey{}, l)
}

func ContextLogger(ctx context.Context, def *slog.Logger) *slog.Logger {
	if l, ok := ctx.Value(loggerCtxKey{}).(*slog.Logger); ok {
		return l
	}
	return def
}
