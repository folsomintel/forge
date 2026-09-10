package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/events"
	"github.com/folsomintel/forge/internal/repodb"
)

// Ref-event streams (SSE). Agents stop polling info/refs on a timer and
// hold one connection instead: every durable ref transaction is pushed to
// them in commit order, with the WAL as the replay log. Reconnect with
// ?after=<last seen seq> (or Last-Event-ID) to recover missed events; if
// that tail was already pruned, the stream opens with "event: reset" and
// the client re-lists refs once.

// refEventSource is the WAL capability the stream needs (seq + replay).
type refEventSource interface {
	WALSeq(ctx context.Context, repoID string) (int64, error)
	ReplayRefEvents(ctx context.Context, repoID string, afterSeq int64) (updates []repodb.RefUpdate, seqs []int64, ts []int64, complete bool, err error)
}

const (
	sseHeartbeat = 25 * time.Second
	// sseWriteTimeout bounds a single frame write. A client that stops
	// reading (without closing the socket) would otherwise wedge this
	// goroutine on a full TCP send buffer forever; the deadline turns that
	// into a write error that tears the stream down.
	sseWriteTimeout = 10 * time.Second
)

func (s *Server) registerEvents(api huma.API) {
	huma.Register(api, op("streamRepoEvents", "GET", "/api/repos/{id}/events", auth.ScopeGitRead,
		"Stream the repository's ref events (SSE); reconnect with ?after=<seq> to replay missed events"),
		func(ctx context.Context, in *struct {
			ID          string `path:"id"`
			After       int64  `query:"after" default:"-1" doc:"Replay events with seq > after before going live (-1 = live only)"`
			LastEventID string `header:"Last-Event-ID"`
		}) (*huma.StreamResponse, error) {
			if s.Events == nil {
				return nil, huma.Error404NotFound("event streaming not enabled")
			}
			if _, err := s.DB.GetRepo(ctx, in.ID); err != nil {
				return nil, huma.Error404NotFound("repository not found")
			}
			after := in.After
			if in.LastEventID != "" {
				if v, err := strconv.ParseInt(in.LastEventID, 10, 64); err == nil {
					after = v
				}
			}
			return &huma.StreamResponse{Body: func(hc huma.Context) {
				s.streamEvents(hc, in.ID, after)
			}}, nil
		})

	huma.Register(api, op("streamEvents", "GET", "/api/events", auth.ScopeOrgRead,
		"Stream ref events for every repository (SSE, live only)"),
		func(ctx context.Context, _ *struct{}) (*huma.StreamResponse, error) {
			if s.Events == nil {
				return nil, huma.Error404NotFound("event streaming not enabled")
			}
			return &huma.StreamResponse{Body: func(hc huma.Context) {
				s.streamEvents(hc, "", -1)
			}}, nil
		})
}

func (s *Server) streamEvents(hc huma.Context, repoID string, after int64) {
	hc.SetHeader("Content-Type", "text/event-stream")
	hc.SetHeader("Cache-Control", "no-cache")
	hc.SetHeader("X-Accel-Buffering", "no")
	w := hc.BodyWriter()
	fl, _ := w.(http.Flusher)
	// A ResponseController lets us bound each write; not every adapter's
	// BodyWriter is an http.ResponseWriter, so guard it. When absent we lose
	// only the deadline, not correctness.
	var rc *http.ResponseController
	if rw, ok := w.(http.ResponseWriter); ok {
		rc = http.NewResponseController(rw)
	}
	// emit writes one raw frame under a write deadline, returning an error if
	// the client has stopped reading. Every write path funnels through it so
	// a wedged socket always tears the stream down.
	emit := func(format string, args ...any) error {
		if rc != nil {
			rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
		}
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}
	flush := func() {
		if fl != nil {
			fl.Flush()
		}
	}
	write := func(ev events.Event) error {
		data, _ := json.Marshal(ev)
		return emit("id: %d\nevent: push\ndata: %s\n\n", ev.Seq, data)
	}

	// Subscribe FIRST, replay second: no window where an event can slip
	// between the two. Live events replayed twice are deduped by seq.
	ch, cancel := s.Events.Subscribe(repoID)
	defer cancel()

	lastSent := int64(-1)
	if repoID != "" && after >= 0 {
		src, ok := s.DB.(refEventSource)
		if !ok {
			after = -1
		} else {
			updates, seqs, ts, complete, err := src.ReplayRefEvents(hc.Context(), repoID, after)
			if err != nil || !complete {
				// The missed tail is gone (pruned) or unreadable: tell the
				// client to re-list refs once, then continue live.
				if emit("event: reset\ndata: {}\n\n") != nil {
					return
				}
			} else {
				for i, u := range updates {
					if write(events.Event{Repo: repoID, Seq: seqs[i], Ref: u.Name, Old: u.Old, New: u.New, TS: ts[i]}) != nil {
						return
					}
					lastSent = seqs[i]
				}
			}
		}
	}
	if emit(": connected\n\n") != nil {
		return
	}
	flush()

	heartbeat := time.NewTicker(sseHeartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-hc.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return // hub closed (drain): end the stream cleanly
			}
			if repoID != "" && ev.Seq <= lastSent {
				continue // already covered by replay
			}
			if write(ev) != nil {
				return
			}
			flush()
		case <-heartbeat.C:
			if emit(": ping\n\n") != nil {
				return
			}
			flush()
		}
	}
}
