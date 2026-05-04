// Package log builds the *slog.Logger used by the dugdale daemon. Wraps
// stdlib slog.NewJSONHandler / NewTextHandler with cfg.LogConfig — JSON to
// stderr by default.
package log

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"letts/internal/config"
)

// noopCloser is a no-op io.Closer wrapper for stderr/stdout.
type noopCloser struct{ io.Writer }

func (noopCloser) Close() error { return nil }

// New returns a logger configured per cfg. The returned io.Closer must be
// Closed at process exit (closes the file when output is a path; no-op for
// std streams).
func New(cfg config.LogConfig) (*slog.Logger, io.Closer, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, nil, err
	}
	format := strings.ToLower(strings.TrimSpace(cfg.Format))
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "text" {
		return nil, nil, fmt.Errorf("invalid log format %q (want json|text)", cfg.Format)
	}

	out, closer, err := openOutput(cfg.Output)
	if err != nil {
		return nil, nil, err
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(out, opts)
	} else {
		handler = slog.NewTextHandler(out, opts)
	}
	return slog.New(handler), closer, nil
}

// parseLevel maps cfg.Level to slog.Level. Empty defaults to info.
func parseLevel(s string) (slog.Level, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("invalid log level %q (want debug|info|warn|error)", s)
}

// openOutput resolves cfg.Output to a writer and closer. "stderr"/"stdout"
// (and the empty default) return the std stream wrapped in a no-op closer.
// Any other value is treated as a file path opened for append and create.
func openOutput(s string) (io.Writer, io.Closer, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "stderr":
		return os.Stderr, noopCloser{os.Stderr}, nil
	case "stdout":
		return os.Stdout, noopCloser{os.Stdout}, nil
	}
	f, err := os.OpenFile(s, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file %s: %w", s, err)
	}
	return f, f, nil
}
