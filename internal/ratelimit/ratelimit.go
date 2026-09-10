// Package ratelimit provides per-key token buckets. In-process by design:
// nodes are the scaling unit (one tenant per node now, per-repo routing
// later), so per-node limits are the correct scope — no Redis.
package ratelimit

import (
	"sync"

	"golang.org/x/time/rate"
)

type Limiter struct {
	rps   rate.Limit
	burst int

	mu      sync.Mutex
	buckets map[string]*rate.Limiter
}

// New returns a limiter allowing rps requests/second with the given burst
// per key. rps <= 0 disables limiting (Allow always true, nil receiver ok).
func New(rps float64, burst int) *Limiter {
	if rps <= 0 {
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	return &Limiter{rps: rate.Limit(rps), burst: burst, buckets: map[string]*rate.Limiter{}}
}

func (l *Limiter) Allow(key string) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	b, ok := l.buckets[key]
	if !ok {
		// Bound memory: a full map is cleared rather than LRU-tracked;
		// refilling costs one burst per active key, which is harmless.
		if len(l.buckets) >= 100_000 {
			l.buckets = map[string]*rate.Limiter{}
		}
		b = rate.NewLimiter(l.rps, l.burst)
		l.buckets[key] = b
	}
	l.mu.Unlock()
	return b.Allow()
}
