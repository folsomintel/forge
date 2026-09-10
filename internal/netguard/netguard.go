// Package netguard builds HTTP clients whose dials refuse private and
// reserved addresses - the SSRF guard shared by webhook delivery and
// import downloads (both fetch customer-supplied URLs).
package netguard

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// Client returns an egress client. allowPrivate disables the guard (dev).
// proxyURL optionally routes through a CONNECT proxy (Smokescreen).
func Client(timeout time.Duration, allowPrivate bool, proxyURL string) *http.Client {
	transport := &http.Transport{
		DialContext:       Dialer(allowPrivate).DialContext,
		ForceAttemptHTTP2: true,
	}
	if proxyURL != "" {
		if u, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(u)
		}
	}
	rt := http.RoundTripper(transport)
	if !allowPrivate {
		rt = hostScreen{next: transport}
	}
	return &http.Client{Timeout: timeout, Transport: rt}
}

type hostScreen struct{ next http.RoundTripper }

func (h hostScreen) RoundTrip(r *http.Request) (*http.Response, error) {
	if !HostAllowed(r.URL.Hostname()) {
		return nil, fmt.Errorf("target blocked: %s is an internal hostname", r.URL.Hostname())
	}
	return h.next.RoundTrip(r)
}

// blockedHostSuffixes are rejected by NAME before DNS even runs - Fly 6PN
// (.internal/.flycast resolve into fdaa::/16, which the IP check also
// catches since fdaa:: is inside fd00::/8 / IsPrivate) plus the usual
// loopback aliases. Belt and suspenders.
var blockedHostSuffixes = []string{
	"localhost", ".localhost", ".internal", ".flycast", ".local", ".consul", ".home.arpa",
}

// blockedHostExact: internal names that carry no dotted suffix. Fly's
// fly-local-6pn resolves to the machine's own 6PN address; the rest are
// common infra aliases. (The internal Machines API `_api.internal:4280`
// and metadata service on fdaa::3 are caught by .internal / the IP check.)
var blockedHostExact = map[string]bool{
	"fly-local-6pn": true, "metadata": true, "instance-data": true,
}

// HostAllowed rejects obviously-internal hostnames pre-DNS.
func HostAllowed(host string) bool {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if blockedHostExact[h] {
		return false
	}
	for _, suffix := range blockedHostSuffixes {
		if h == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(h, suffix) {
			return false
		}
	}
	return true
}

// Dialer builds a net.Dialer whose guard runs in Control - after DNS resolution, on the literal
// address, re-checked on every redirect hop. Hostnames are additionally
// screened pre-DNS.
func Dialer(allowPrivate bool) *net.Dialer {
	d := &net.Dialer{Timeout: 5 * time.Second}
	if allowPrivate {
		return d
	}
	d.Resolver = net.DefaultResolver
	d.Control = func(network, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		if !HostAllowed(host) {
			return fmt.Errorf("target blocked: %s is an internal hostname", host)
		}
		ip := net.ParseIP(host)
		if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
			ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
			return fmt.Errorf("target blocked: %s is a private or reserved address", host)
		}
		return nil
	}
	return d
}
