// cmd/letts/format_test.go
package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestParseFormatValid(t *testing.T) {
	cases := []struct {
		in   string
		want Format
	}{
		{"text", FormatText},
		{"", FormatText}, // empty string defaults to text (no --output flag)
		{"json", FormatJSON},
		{"yaml", FormatYAML},
		{"ndjson", FormatNDJSON},
	}
	for _, tc := range cases {
		got, err := ParseFormat(tc.in)
		if err != nil {
			t.Fatalf("ParseFormat(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("ParseFormat(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParseFormatInvalid(t *testing.T) {
	if _, err := ParseFormat("xml"); err == nil {
		t.Fatal("expected error on unknown format")
	}
}

func TestPrintJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := PrintJSON(&buf, map[string]any{"k": 1}); err != nil {
		t.Fatalf("PrintJSON: %v", err)
	}
	var got map[string]int
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["k"] != 1 {
		t.Errorf("got %v, want {k:1}", got)
	}
}
