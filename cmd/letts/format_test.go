// cmd/letts/format_test.go
package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

func TestPrintYAMLUsesJSONEncoding(t *testing.T) {
	type rec struct {
		MissionID   string          `json:"mission_id"`
		Name        string          `json:"mission_name"`
		TimeCreated int64           `json:"time_created"`
		Input       json.RawMessage `json:"input"`
		Pid         int             `json:"pid,omitempty"`
		Note        string          `json:"note"`
		Data        string          `json:"data"`
	}
	var buf bytes.Buffer
	err := PrintYAML(&buf, []rec{{
		MissionID: "m1", Name: "true", TimeCreated: 1760000000000,
		Input: json.RawMessage(`{"a":1,"b":[1,2]}`), Note: "a: b", Data: "line1\nline2\n",
	}})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"mission_id: m1", `mission_name: "true"`, "time_created: 1760000000000", "    a: 1", "note: 'a: b'"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "pid") || strings.Contains(out, "missionid") || strings.Contains(out, "- 123") {
		t.Errorf("Go-field or byte rendering leaked:\n%s", out)
	}
	var back []map[string]any
	if err := yaml.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if back[0]["data"] != "line1\nline2\n" || back[0]["mission_name"] != "true" {
		t.Errorf("round trip = %+v", back[0])
	}
}

func TestPrintYAMLEdgeCases(t *testing.T) {
	v := map[string]any{
		"raw":   json.RawMessage(`{"url":"http:\/\/x","bad":"\udcff","n":1e21,"big":12345678901234567890}`),
		"yes":   "yes",
		"off":   "off",
		"clock": "1:20",
		"num":   "123",
		"nul":   "null",
		"empty": map[string]any{},
		"list":  []any{},
		"nil":   nil,
	}
	var buf bytes.Buffer
	if err := PrintYAML(&buf, v); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"url: http://x", `"yes": "yes"`, `"off": "off"`, `clock: "1:20"`, `num: "123"`, `nul: "null"`, "empty: {}", "list: []", "nil: null", "big: 12345678901234567890", `"n": 1e21`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	var back map[string]any
	if err := yaml.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatalf("output is not YAML: %v\n%s", err, out)
	}
	if back["clock"] != "1:20" || back["yes"] != "yes" {
		t.Errorf("round trip = %+v", back)
	}
	buf.Reset()
	if err := PrintYAML(&buf, nil); err != nil || strings.TrimSpace(buf.String()) != "null" {
		t.Errorf("nil: %q err=%v", buf.String(), err)
	}
}
