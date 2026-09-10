package netguard

import (
	"context"
	"strings"
	"testing"
)

func TestHostAllowed(t *testing.T) {
	blocked := []string{
		"localhost", "LOCALHOST", "foo.localhost",
		"_api.internal", "cp.example.internal", "top1.nearest.of.app.internal",
		"myapp.flycast", "fly-local-6pn", "FLY-LOCAL-6PN",
		"metadata", "instance-data", "db.local", "svc.consul", "router.home.arpa",
		"app.internal.", // trailing dot must not bypass
	}
	for _, h := range blocked {
		if HostAllowed(h) {
			t.Errorf("HostAllowed(%q) = true, want blocked", h)
		}
	}
	allowed := []string{
		"github.com", "objects.example.com", "myapp.fly.dev",
		"internal.example.com", // ".internal" is a suffix match, not a substring match
		"flycast.example.org",
	}
	for _, h := range allowed {
		if !HostAllowed(h) {
			t.Errorf("HostAllowed(%q) = false, want allowed", h)
		}
	}
}

func TestDialerBlocksInternalAddresses(t *testing.T) {
	d := Dialer(false)
	for _, addr := range []string{
		"127.0.0.1:80", "10.0.0.1:443", "169.254.169.254:80",
		"[::1]:80", "[fdaa:0:1:a7b:1:2:3:4]:4280", "[fdaa::3]:53", "[fe80::1]:80",
	} {
		_, err := d.DialContext(context.Background(), "tcp", addr)
		if err == nil || !strings.Contains(err.Error(), "target blocked") {
			t.Errorf("dial %s: err = %v, want target-blocked", addr, err)
		}
	}
}
