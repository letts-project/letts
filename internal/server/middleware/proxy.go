package middleware

import (
	"net"
	"net/http"
	"strings"
)

// ClientIP determines the originating client IP address.
//
// If useXFF is true and r.RemoteAddr's IP is in one of the trustedCIDRs,
// the leftmost valid IP from the X-Forwarded-For header is returned.
// In all other cases the host portion of r.RemoteAddr is returned.
func ClientIP(r *http.Request, trustedCIDRs []*net.IPNet, useXFF bool) string {
	remoteHost, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// Malformed RemoteAddr (e.g., no port) — return it as-is.
		return r.RemoteAddr
	}

	if !useXFF {
		return remoteHost
	}

	remoteIP := net.ParseIP(remoteHost)
	if remoteIP == nil || !inCIDRList(remoteIP, trustedCIDRs) {
		return remoteHost
	}

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return remoteHost
	}

	// XFF is a comma-separated list; leftmost is the original client.
	parts := strings.SplitN(xff, ",", 2)
	candidate := strings.TrimSpace(parts[0])
	if net.ParseIP(candidate) == nil {
		// Malformed leftmost entry → fall back.
		return remoteHost
	}
	return candidate
}

func inCIDRList(ip net.IP, cidrs []*net.IPNet) bool {
	for _, cidr := range cidrs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}
