package ids

import (
	"path/filepath"
	"testing"
)

func TestShardPath(t *testing.T) {
	cases := []struct {
		id   string
		want string
	}{
		{"0192a8b3-d2c1-7abc-bad0-1234567890ab", filepath.Join("01", "92")},
		{"ffffffff-ffff-7fff-bfff-ffffffffffff", filepath.Join("ff", "ff")},
		{"00000000-0000-7000-8000-000000000000", filepath.Join("00", "00")},
	}
	for _, c := range cases {
		got, err := ShardPath(c.id)
		if err != nil {
			t.Fatalf("ShardPath(%s): %v", c.id, err)
		}
		if got != c.want {
			t.Errorf("ShardPath(%s) = %s, want %s", c.id, got, c.want)
		}
	}
}

func TestShardPathInvalid(t *testing.T) {
	for _, bad := range []string{"", "not-a-uuid", "0192a8b3"} {
		if _, err := ShardPath(bad); err == nil {
			t.Errorf("ShardPath(%q) = nil err, want error", bad)
		}
	}
}
