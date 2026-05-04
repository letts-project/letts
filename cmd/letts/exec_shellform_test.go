package main

import (
	"testing"

	"letts/internal/fingerprint"
)

func TestExecShellFormMatchesServerCases(t *testing.T) {
	for _, tc := range fingerprint.ShellFormCases {
		t.Run(tc.Name, func(t *testing.T) {
			got := isShellForm(tc.Argv)
			if got != tc.Shell {
				t.Errorf("CLI isShellForm(%v) = %v, want %v (server agrees)", tc.Argv, got, tc.Shell)
			}
		})
	}
}
