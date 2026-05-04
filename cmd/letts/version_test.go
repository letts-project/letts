package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionCmdPrintsVersion(t *testing.T) {
	root := newRootCmd()
	root.AddCommand(newVersionCmd())
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"version"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(out.String(), "letts ") {
		t.Errorf("got %q", out.String())
	}
}
