// cmd/letts/format.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

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

// PrintYAML writes v as YAML built from its JSON encoding, so field names,
// omitempty and raw JSON fields come out as in PrintJSON.
func PrintYAML(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	doc, err := yamlNodeFromJSON(dec)
	if err != nil {
		return err
	}
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return err
	}
	return enc.Close()
}

// yamlNodeFromJSON converts the next JSON value read from dec into a block
// style YAML node.
func yamlNodeFromJSON(dec *json.Decoder) (*yaml.Node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		n := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		if t == '{' {
			n = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		}
		for dec.More() {
			if n.Kind == yaml.MappingNode {
				key, err := dec.Token()
				if err != nil {
					return nil, err
				}
				n.Content = append(n.Content, yamlString(key.(string)))
			}
			child, err := yamlNodeFromJSON(dec)
			if err != nil {
				return nil, err
			}
			n.Content = append(n.Content, child)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return n, nil
	case string:
		return yamlString(t), nil
	case json.Number:
		tag := "!!int"
		if strings.ContainsAny(t.String(), ".eE") {
			tag = "!!float"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: t.String()}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(t)}, nil
	default:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	}
}

// yamlString returns a string scalar. It is double-quoted whenever yaml.v3's
// own string encoding would double-quote it: the node encoder leaves YAML 1.1
// booleans (yes, off, …) and base-60 numbers (1:20) plain, which YAML 1.1
// parsers then misread.
func yamlString(s string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
	if out, err := yaml.Marshal(s); err == nil && len(out) > 0 && out[0] == '"' {
		n.Style = yaml.DoubleQuotedStyle
	}
	return n
}

// PrintNDJSON writes one JSON object per line, no indent.
func PrintNDJSON(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

// printRecord writes v in machine format f and reports true, or writes
// nothing and reports false for FormatText.
func printRecord(w io.Writer, f Format, v any) (bool, error) {
	switch f {
	case FormatJSON:
		return true, PrintJSON(w, v)
	case FormatYAML:
		return true, PrintYAML(w, v)
	case FormatNDJSON:
		return true, PrintNDJSON(w, v)
	}
	return false, nil
}

// printList is printRecord for a listing: json/yaml write whole, ndjson
// writes one line per item.
func printList[T any](w io.Writer, f Format, whole any, items []T) (bool, error) {
	if f != FormatNDJSON {
		return printRecord(w, f, whole)
	}
	for _, it := range items {
		if err := PrintNDJSON(w, it); err != nil {
			return true, err
		}
	}
	return true, nil
}
