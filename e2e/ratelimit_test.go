package e2e

import (
	"net/http"
	"testing"

	"github.com/folsomintel/forge/internal/config"
)

func TestRateLimitAPI(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.RateAPI = 2 }) // burst 9
	e.createRepo("demo")

	limited := 0
	var lastHeaders http.Header
	for i := 0; i < 40; i++ {
		req, _ := http.NewRequest("GET", e.base+"/api/repos/demo", nil)
		req.Header.Set("Authorization", "Bearer "+e.token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			limited++
			lastHeaders = resp.Header
		} else if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status %d", resp.StatusCode)
		}
	}
	if limited == 0 {
		t.Fatal("no request was rate limited at 2 rps over 40 rapid requests")
	}
	if lastHeaders.Get("Retry-After") == "" {
		t.Fatal("429 without Retry-After header")
	}
}

func TestRateLimitGit(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.RateGit = 1 }) // burst 5
	e.createRepo("demo")

	limited := 0
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest("GET", e.base+"/demo.git/info/refs?service=git-upload-pack", nil)
		req.Header.Set("Authorization", "Bearer "+e.token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("git endpoint never rate limited at 1 rps over 20 rapid requests")
	}
}

func TestRateLimitDisabledByDefaultConfigZero(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.RateAPI = 0 })
	e.createRepo("demo")
	for i := 0; i < 50; i++ {
		status, _ := e.api("GET", "/api/repos/demo", nil)
		if status != http.StatusOK {
			t.Fatalf("request %d: status %d with limiting disabled", i, status)
		}
	}
}
