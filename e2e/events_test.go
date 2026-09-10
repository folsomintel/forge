package e2e

import (
	"bufio"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/folsomintel/forge/internal/config"
)

type sseEvent struct {
	Repo string `json:"repo"`
	Seq  int64  `json:"seq"`
	Ref  string `json:"ref"`
	Old  string `json:"old"`
	New  string `json:"new"`
}

// sseCollect opens the stream and forwards parsed push events (and reset
// markers as Ref="<reset>") until the connection closes.
func sseCollect(t *testing.T, e *env, path string) (<-chan sseEvent, func()) {
	t.Helper()
	req, _ := http.NewRequest("GET", e.base+path, nil)
	req.Header.Set("Authorization", "Bearer "+e.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sse connect: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("sse status %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("sse content-type %q", ct)
	}
	out := make(chan sseEvent, 64)
	go func() {
		defer close(out)
		sc := bufio.NewScanner(res.Body)
		event := ""
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				if event == "reset" {
					out <- sseEvent{Ref: "<reset>"}
				} else if event == "push" {
					var ev sseEvent
					if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) == nil {
						out <- ev
					}
				}
				event = ""
			}
		}
	}()
	return out, func() { res.Body.Close() }
}

func nextEvent(t *testing.T, ch <-chan sseEvent, what string) sseEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatalf("stream closed waiting for %s", what)
		}
		return ev
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
	return sseEvent{}
}

// Live push events reach a subscriber for every push path (fast-path and
// git-fallback both funnel through the WAL).
func TestRefEventsLive(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.GoReceive = true })
	e.createRepo("ev")

	ch, cancel := sseCollect(t, e, "/api/repos/ev/events")
	defer cancel()

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("ev", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")
	os.WriteFile(filepath.Join(work, "f.txt"), []byte("one\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "one")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	ev := nextEvent(t, ch, "push event")
	if ev.Ref != "refs/heads/main" || ev.New == "" || ev.Seq < 1 {
		t.Fatalf("unexpected event: %+v", ev)
	}

	// API-driven commit also lands on the stream.
	e.mustAPI("PUT", "/api/repos/ev/contents/api.txt", map[string]any{
		"message": "via api", "content": b64("hi\n"),
	}, http.StatusCreated)
	ev2 := nextEvent(t, ch, "api commit event")
	if ev2.Ref != "refs/heads/main" || ev2.Seq <= ev.Seq {
		t.Fatalf("api event: %+v (after %+v)", ev2, ev)
	}
}

// Replay: a subscriber reconnecting with ?after=<seq> receives everything
// it missed, in order, from the WAL.
func TestRefEventsReplay(t *testing.T) {
	t.Parallel()
	e := startServerWith(t, func(cfg *config.Config) { cfg.GoReceive = true })
	e.createRepo("rp")

	for i, name := range []string{"a.txt", "b.txt", "c.txt"} {
		_ = i
		e.mustAPI("PUT", "/api/repos/rp/contents/"+name, map[string]any{
			"message": "add " + name, "content": b64(name + "\n"),
		}, http.StatusCreated)
	}

	// Missed everything: replay from 0 yields all three, in seq order.
	ch, cancel := sseCollect(t, e, "/api/repos/rp/events?after=0")
	defer cancel()
	var last int64
	for i := 0; i < 3; i++ {
		ev := nextEvent(t, ch, "replayed event")
		if ev.Seq <= last {
			t.Fatalf("replay out of order: %+v after seq %d", ev, last)
		}
		last = ev.Seq
	}

	// And the stream stays live after replay.
	e.mustAPI("PUT", "/api/repos/rp/contents/d.txt", map[string]any{
		"message": "add d", "content": b64("d\n"),
	}, http.StatusCreated)
	ev := nextEvent(t, ch, "live event after replay")
	if ev.Seq <= last {
		t.Fatalf("live after replay out of order: %+v", ev)
	}
}
