package config

import "testing"

func TestValidators(t *testing.T) {
	tests := []struct {
		name string
		fn   func(string) error
		ok   []string
		bad  []string
	}{
		{"DugdaleID", ValidateDugdaleID, []string{"s1", "prod-1", "k_v_2"}, []string{"", "S1", "1foo", "a-b!"}},
		{"Lane", ValidateLaneName, []string{"normal", "high-priority", "p"}, []string{"", "Normal", "x" + string(make([]byte, 32))}},
		{"Label", ValidateLabel, []string{"prod", "kv", "us-east"}, []string{"", "Prod", "_x"}},
		{"Route", ValidateRouteName, []string{"normal", "rt-1"}, []string{"", "1x"}},
		{"Mission", ValidateMissionName, []string{"BookCalc", "X.y_z-1", "0name"}, []string{"", "../etc", "x" + string(make([]byte, 128))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, s := range tt.ok {
				if err := tt.fn(s); err != nil {
					t.Errorf("%s ok %q: %v", tt.name, s, err)
				}
			}
			for _, s := range tt.bad {
				if err := tt.fn(s); err == nil {
					t.Errorf("%s bad %q: expected error", tt.name, s)
				}
			}
		})
	}
}

func TestValidateRoleKey(t *testing.T) {
	okCases := []string{"foo", "Bar", "_x", "a1", "A_B_C"}
	for _, s := range okCases {
		if err := ValidateRoleKey(s); err != nil {
			t.Errorf("ValidateRoleKey ok %q: %v", s, err)
		}
	}
	badCases := []string{"", "__reserved", "1bad", "has-dash", "has.dot"}
	for _, s := range badCases {
		if err := ValidateRoleKey(s); err == nil {
			t.Errorf("ValidateRoleKey bad %q: expected error", s)
		}
	}
}
