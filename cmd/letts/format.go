// cmd/letts/format.go
package main

import (
	"encoding/json"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// Format is the CLI output mode.
type Format int

const (
	FormatText Format = iota
	FormatJSON
	FormatYAML
	FormatNDJSON
)

// ParseFormat decodes the --output flag string.
func ParseFormat(s string) (Format, error) {
	switch s {
	case "text", "":
		return FormatText, nil
	case "json":
		return FormatJSON, nil
	case "yaml":
		return FormatYAML, nil
	case "ndjson":
		return FormatNDJSON, nil
	default:
		return 0, fmt.Errorf("unknown format %q (want text|json|yaml|ndjson)", s)
	}
}

// PrintJSON writes a single JSON-encoded value followed by a newline.
func PrintJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// PrintYAML writes a single YAML-encoded value.
func PrintYAML(w io.Writer, v any) error {
	return yaml.NewEncoder(w).Encode(v)
}

// PrintNDJSON writes one JSON object per line, no indent.
func PrintNDJSON(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}
