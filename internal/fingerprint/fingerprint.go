// Package fingerprint computes RFC 8785 idempotency fingerprints for
// dispatch payloads.
package fingerprint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"letts/internal/jcs"
)

// FileRef is one entry in dispatch.files (sorted by Role for canonicalization).
type FileRef struct {
	Role      string
	StagingID string
	Sha256    string
	Size      int64
}

// MissionInput is the fingerprint input shape (kind=mission only).
type MissionInput struct {
	Lane           string
	Mission        string
	InputCanonical []byte // already JCS-canonicalized request.input bytes (or "null")
	TimeoutMs      *int64 // nil → JSON null
	Files          []FileRef
}

// Mission computes sha256(jcs(payload)) where payload follows the schema.
func Mission(in MissionInput) (string, error) {
	inputSha := sha256.Sum256(in.InputCanonical)
	payload := map[string]any{
		"kind":         "mission",
		"lane":         in.Lane,
		"mission":      in.Mission,
		"input_sha256": hex.EncodeToString(inputSha[:]),
	}
	if in.TimeoutMs != nil {
		payload["timeout_ms"] = *in.TimeoutMs
	} else {
		payload["timeout_ms"] = nil
	}

	files := make([]FileRef, len(in.Files))
	copy(files, in.Files)
	sort.Slice(files, func(i, j int) bool { return files[i].Role < files[j].Role })

	arr := make([]any, 0, len(files))
	for _, f := range files {
		arr = append(arr, map[string]any{
			"role":       f.Role,
			"staging_id": f.StagingID,
			"sha256":     f.Sha256,
			"size":       f.Size,
		})
	}
	payload["files"] = arr

	canonical, err := jcs.Canonicalize(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalizeInput JCS-canonicalizes a json.RawMessage from a dispatch body
// for storage in MissionInput.InputCanonical and in missions.input.
//
// Uses json.Decoder.UseNumber so integers beyond float64's 2^53 precision
// boundary survive the round-trip; jcs.encode then preserves int64/uint64
// values byte-exact.
func CanonicalizeInput(raw json.RawMessage) ([]byte, error) {
	// RFC 8785 section 3.1 (I-JSON): reject duplicate object keys, which
	// Go's decoder would otherwise silently collapse (last value wins) — making
	// two distinct request bodies hash to the same idempotency fingerprint.
	if err := rejectDuplicateKeys(raw); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return jcs.Canonicalize(v)
}

// rejectDuplicateKeys walks the JSON token stream and returns an error if any
// object has a repeated key at any nesting depth.
func rejectDuplicateKeys(raw json.RawMessage) error {
	return walkNoDupKeys(json.NewDecoder(bytes.NewReader(raw)))
}

func walkNoDupKeys(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil // scalar value
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := kt.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if seen[key] {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = true
			if err := walkNoDupKeys(dec); err != nil { // value
				return err
			}
		}
		_, err := dec.Token() // consume '}'
		return err
	case '[':
		for dec.More() {
			if err := walkNoDupKeys(dec); err != nil {
				return err
			}
		}
		_, err := dec.Token() // consume ']'
		return err
	}
	return nil
}
