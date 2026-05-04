package lettsconfig

import "testing"

func TestBaseURLFor(t *testing.T) {
	cfg := &Config{
		Defaults: Defaults{Port: 7190},
		Dugdales: []Dugdale{
			{ID: "s1", Host: "server1.internal"},
			{ID: "s2", Host: "server2.internal", Port: 7181},
			{ID: "s3", Host: "server3.internal:7999"}, // port baked into host wins
			{ID: "third", URL: "https://letts-third.example.com"},
		},
	}
	cases := []struct{ id, want string }{
		{"s1", "http://server1.internal:7190"},
		{"s2", "http://server2.internal:7181"},
		{"s3", "http://server3.internal:7999"},
		{"third", "https://letts-third.example.com"},
	}
	for _, c := range cases {
		got, err := BaseURLFor(cfg, c.id)
		if err != nil {
			t.Fatalf("%s: %v", c.id, err)
		}
		if got != c.want {
			t.Errorf("%s: got %q want %q", c.id, got, c.want)
		}
	}
	cfgNoDefault := &Config{Dugdales: []Dugdale{{ID: "x", Host: "h"}}}
	if got, _ := BaseURLFor(cfgNoDefault, "x"); got != "http://h:7180" {
		t.Errorf("default port: got %q want http://h:7180", got)
	}
	if _, err := BaseURLFor(cfg, "nope"); err == nil {
		t.Error("expected error for unknown dugdale id")
	}
}
