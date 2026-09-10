// Package events is the in-process ref-event bus: every durable ref
// transaction (fast-path push, git-fallback push, API commit - they all
// funnel through the WAL) is published here, and SSE subscribers stream it
// out. This replaces advertisement polling for agents: instead of hitting
// info/refs on a timer, they hold one connection and react in
// milliseconds. The WAL doubles as the replay log, so a reconnecting
// subscriber recovers missed events (or is told to reset when the tail
// was pruned).
package events

import "sync"

// Event is one ref move, in WAL commit order. Seq is the repo's WAL
// sequence: contiguous per repo, so a subscriber can detect gaps and
// reconnect with ?after=<last seen>.
type Event struct {
	Repo string `json:"repo"`
	Seq  int64  `json:"seq"`
	Ref  string `json:"ref"`
	Old  string `json:"old"`
	New  string `json:"new"`
	TS   int64  `json:"ts"`
}

// subBuffer is each subscriber's channel depth. A subscriber that falls
// further behind than this loses events (non-blocking publish - a slow
// reader must never stall the push path); it detects the per-repo seq gap
// and reconnects with replay.
const subBuffer = 256

type sub struct {
	ch   chan Event
	repo string // "" = firehose (all repos)
}

type Hub struct {
	mu     sync.Mutex
	subs   map[*sub]struct{}
	closed bool
}

func NewHub() *Hub { return &Hub{subs: map[*sub]struct{}{}} }

// Subscribe returns a channel of events for one repo ("" = all repos) and
// a cancel func. The channel is closed on cancel or hub Close.
func (h *Hub) Subscribe(repo string) (<-chan Event, func()) {
	s := &sub{ch: make(chan Event, subBuffer), repo: repo}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		close(s.ch)
		return s.ch, func() {}
	}
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			if _, ok := h.subs[s]; ok {
				delete(h.subs, s)
				close(s.ch)
			}
			h.mu.Unlock()
		})
	}
	return s.ch, cancel
}

// Publish fans events out to matching subscribers. Non-blocking: a full
// subscriber drops the event (it recovers via seq-gap detection + replay).
func (h *Hub) Publish(evs ...Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	for _, ev := range evs {
		for s := range h.subs {
			if s.repo != "" && s.repo != ev.Repo {
				continue
			}
			select {
			case s.ch <- ev:
			default: // slow subscriber: drop; the seq gap tells it to replay
			}
		}
	}
}

// Close terminates every subscription (graceful drain: SSE handlers exit
// so shutdown never waits on idle event streams). Idempotent.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for s := range h.subs {
		delete(h.subs, s)
		close(s.ch)
	}
}
