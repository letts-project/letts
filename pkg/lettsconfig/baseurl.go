package lettsconfig

import (
	"fmt"
	"net"
	"strconv"
)

// BaseURLFor returns the dial-able HTTP base URL for the dugdale with the
// given id, resolved from cfg. An explicit `url:` wins; otherwise host+port
// where the port precedence is: a port embedded in `host:` (e.g.
// "server1:7180") > Dugdale.Port > Defaults.Port > 7180. net.SplitHostPort /
// net.JoinHostPort handle bracketed IPv6 correctly.
//
// This mirrors the resolution cmd/letts uses (runtime.go), so external
// consumers (arby) reach the same endpoint as the CLI.
func BaseURLFor(cfg *Config, dugdaleID string) (string, error) {
	for i := range cfg.Dugdales {
		d := &cfg.Dugdales[i]
		if d.ID != dugdaleID {
			continue
		}
		if d.URL != "" {
			return d.URL, nil
		}
		port := d.Port
		if port == 0 {
			port = cfg.Defaults.Port
		}
		if port == 0 {
			port = 7180
		}
		host := d.Host
		if h, p, err := net.SplitHostPort(d.Host); err == nil {
			host = h
			if pi, e2 := strconv.Atoi(p); e2 == nil && pi > 0 {
				port = pi
			}
		}
		return "http://" + net.JoinHostPort(host, strconv.Itoa(port)), nil
	}
	return "", fmt.Errorf("dugdale %q not found in config", dugdaleID)
}
