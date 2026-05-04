package main

import (
	"strings"
	"testing"
)

// TestParseExecKVValid exercises a multi-pair happy path: two distinct keys
// with absolute and relative paths both parse and round-trip verbatim.
func TestParseExecKVValid(t *testing.T) {
	got, err := parseExecKV([]string{"pdf=./in.pdf", "txt=/tmp/data.txt"}, "--in")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key != "pdf" || got[1].Path != "/tmp/data.txt" {
		t.Errorf("got %+v", got)
	}
}

// TestParseExecKVRejectsBadKeys covers every validation failure mode of
// parseExecKV: reserved __ prefix, illegal chars, empty fields, missing
// separator, leading digit, length > 64.
func TestParseExecKVRejectsBadKeys(t *testing.T) {
	cases := []struct{ Name, Pair string }{
		{"reserved __", "__stdin=/tmp/x"},
		{"slash", "pdf/x=./in"},
		{"empty key", "=./in"},
		{"empty path", "pdf="},
		{"no equals", "pdf"},
		{"hyphen", "pd-f=./in"},
		{"leading digit", "1pdf=./in"},
		{"too long", strings.Repeat("a", 65) + "=./in"},
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if _, err := parseExecKV([]string{c.Pair}, "--in"); err == nil {
				t.Errorf("expected error for %q", c.Pair)
			}
		})
	}
}

// TestParseExecKVRejectsDuplicateKey: two pairs with the same key (even
// different paths) must surface a BadUsageError.
func TestParseExecKVRejectsDuplicateKey(t *testing.T) {
	if _, err := parseExecKV([]string{"pdf=./a", "pdf=./b"}, "--in"); err == nil {
		t.Error("expected duplicate-key error")
	}
}
