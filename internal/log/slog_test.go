package log_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"letts/internal/config"
	"letts/internal/log"
)

func TestNewDefaultsJSONInfoStderr(t *testing.T) {
	logger, closer, err := log.New(config.LogConfig{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = closer.Close() }()
	if logger == nil {
		t.Fatal("nil logger")
	}
}

func TestNewJSONFormatProducesParseableJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.log")
	logger, closer, err := log.New(config.LogConfig{Format: "json", Level: "info", Output: path})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("hello", "k", "v", "n", 42)
	_ = closer.Close()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	line := strings.TrimSpace(string(b))
	if line == "" {
		t.Fatal("no output")
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("not JSON: %q (%v)", line, err)
	}
	if rec["msg"] != "hello" {
		t.Errorf("msg=%v", rec["msg"])
	}
	if rec["k"] != "v" {
		t.Errorf("k=%v", rec["k"])
	}
	if rec["level"] != "INFO" {
		t.Errorf("level=%v", rec["level"])
	}
}

func TestNewTextFormatNotJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.log")
	logger, closer, err := log.New(config.LogConfig{Format: "text", Output: path})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("hi", "x", 1)
	_ = closer.Close()

	b, _ := os.ReadFile(path)
	s := strings.TrimSpace(string(b))
	if s == "" {
		t.Fatal("no output")
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(s), &rec); err == nil {
		t.Errorf("text-format output unexpectedly parses as JSON: %q", s)
	}
	if !strings.Contains(s, "msg=hi") {
		t.Errorf("missing msg key/val: %q", s)
	}
}

func TestNewLevelFiltering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.log")
	logger, closer, err := log.New(config.LogConfig{Format: "json", Level: "warn", Output: path})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("info-msg")
	logger.Warn("warn-msg")
	_ = closer.Close()

	b, _ := os.ReadFile(path)
	s := string(b)
	if strings.Contains(s, "info-msg") {
		t.Errorf("info leaked above warn level: %q", s)
	}
	if !strings.Contains(s, "warn-msg") {
		t.Errorf("warn missing: %q", s)
	}
}

func TestNewOutputStdout(t *testing.T) {
	logger, closer, err := log.New(config.LogConfig{Output: "stdout"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer.Close() }()
	if logger == nil {
		t.Fatal("nil logger")
	}
}

func TestNewOutputStderr(t *testing.T) {
	logger, closer, err := log.New(config.LogConfig{Output: "stderr"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closer.Close() }()
	if logger == nil {
		t.Fatal("nil logger")
	}
}

func TestNewInvalidLevel(t *testing.T) {
	_, _, err := log.New(config.LogConfig{Level: "loud"})
	if err == nil {
		t.Error("expected error for bad level")
	}
}

func TestNewInvalidFormat(t *testing.T) {
	_, _, err := log.New(config.LogConfig{Format: "yaml"})
	if err == nil {
		t.Error("expected error for bad format")
	}
}

func TestNewBadPathReturnsError(t *testing.T) {
	_, _, err := log.New(config.LogConfig{Output: "/nonexistent-dir-xyz/log.log"})
	if err == nil {
		t.Error("expected error for unwritable path")
	}
}

func TestLevelAliases(t *testing.T) {
	for _, lvl := range []string{"debug", "DEBUG", "info", "INFO", "warn", "warning", "WARN", "error", "ERROR"} {
		_, closer, err := log.New(config.LogConfig{Level: lvl})
		if err != nil {
			t.Errorf("level %q: %v", lvl, err)
			continue
		}
		_ = closer.Close()
	}
}
