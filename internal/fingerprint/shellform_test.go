package fingerprint

import "testing"

func TestIsShellForm(t *testing.T) {
	for _, tc := range ShellFormCases {
		t.Run(tc.Name, func(t *testing.T) {
			got := IsShellForm(tc.Argv)
			if got != tc.Shell {
				t.Errorf("IsShellForm(%v) = %v, want %v", tc.Argv, got, tc.Shell)
			}
		})
	}
}
