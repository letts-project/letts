// Package fingerprint — exec dispatch fingerprint.
package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"letts/internal/jcs"
)

// ExecInput is the canonical input for fingerprint.Exec. Fields mirror the
// /v1/exec/dispatch body schema plus resolved staging metadata
// (sha256, size) so fingerprint binds to content, not just to the staging
// id (which could be reused by a stable-content lookup).
//
// GroupID and DisplayName are deliberately excluded — they're metadata the
// caller can mutate across retries without changing identity.
type ExecInput struct {
	Lane           string
	Command        []string
	Script         *ExecScriptRef
	In             []ExecFileRef
	Out            []ExecOutKey
	Stdin          string // "" treated as "none"
	StdinStagingID string
	TimeoutMs      *int64
	GroupID        string // excluded from fingerprint
	DisplayName    string // excluded from fingerprint
}

// ExecScriptRef is the resolved script reference (staging id and content sha).
type ExecScriptRef struct {
	StagingID string
	Sha256    string
}

// ExecFileRef is one in[] entry with resolved content metadata.
type ExecFileRef struct {
	Key       string
	StagingID string
	Sha256    string
	Size      int64
}

// ExecOutKey is one out[] entry (declaration only — no content at dispatch).
type ExecOutKey struct {
	Key string
}

// Exec returns hex sha256 of the JCS canonical payload.
// Stable across In[]/Out[] key order; insensitive to GroupID/DisplayName.
func Exec(in ExecInput) (string, error) {
	// Normalize stdin to its canonical form so "" and "none" hash the same.
	stdin := in.Stdin
	if stdin == "" {
		stdin = "none"
	}

	cmd := make([]any, len(in.Command))
	for i, s := range in.Command {
		cmd[i] = s
	}

	payload := map[string]any{
		"kind":    "exec",
		"lane":    in.Lane,
		"command": cmd,
		"stdin":   stdin,
	}
	if in.Script != nil {
		payload["script"] = map[string]any{
			"staging_id": in.Script.StagingID,
			"sha256":     in.Script.Sha256,
		}
	} else {
		payload["script"] = nil
	}
	if in.StdinStagingID != "" {
		payload["stdin_staging_id"] = in.StdinStagingID
	} else {
		payload["stdin_staging_id"] = nil
	}
	if in.TimeoutMs != nil {
		payload["timeout_ms"] = *in.TimeoutMs
	} else {
		payload["timeout_ms"] = nil
	}

	// Sort in[] by key for stable canonicalization. We don't mutate the
	// caller's slice — copy first.
	ins := make([]ExecFileRef, len(in.In))
	copy(ins, in.In)
	sort.Slice(ins, func(i, j int) bool { return ins[i].Key < ins[j].Key })
	inArr := make([]any, 0, len(ins))
	for _, e := range ins {
		inArr = append(inArr, map[string]any{
			"key":        e.Key,
			"staging_id": e.StagingID,
			"sha256":     e.Sha256,
			"size":       e.Size,
		})
	}
	payload["in"] = inArr

	outs := make([]ExecOutKey, len(in.Out))
	copy(outs, in.Out)
	sort.Slice(outs, func(i, j int) bool { return outs[i].Key < outs[j].Key })
	outArr := make([]any, 0, len(outs))
	for _, e := range outs {
		outArr = append(outArr, map[string]any{"key": e.Key})
	}
	payload["out"] = outArr

	canonical, err := jcs.Canonicalize(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}
