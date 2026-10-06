package maintain

import (
	"sync"
	"time"

	"github.com/folsomintel/forge/internal/repodb"
)

// Pacing. Under a continuous push load every push adds a pack, so a pure
// pack-count trigger would keep a repo in maintenance back to back - each
// run a full repack + bitmap + derive - and on a burstable shared CPU that
// drains the burst balance until the host is throttled. Threshold-gated
// runs (nudges, the worker) therefore wait at least MinInterval after the
// previous run finished, or paceDutyFactor times that run's duration if
// longer, bounding maintenance to a fraction of wall time per repo.
// Forced runs (imports, the ops endpoint) are never paced, but they do
// start a new window.

const paceDutyFactor = 4

type repoPace struct {
	mu        sync.Mutex
	next      time.Time // threshold-gated runs wait until then
	lastStart time.Time // start of the last run that completed without error
	// est is a cheap upper-bound guess at the pack count - the last
	// observed count plus one per nudge since (a push adds at most one
	// pack) - so NudgeIfNeeded lists packs only when the threshold may
	// actually have been crossed. -1 = unknown.
	est int
	// pending: a nudge arrived while a run was in flight.
	pending bool
}

// takePending reports and clears a nudge dropped during a run.
func (p *Pipeline) takePending(repoID string) bool {
	v, ok := p.pace.Load(repoID)
	if !ok {
		return false
	}
	rp := v.(*repoPace)
	rp.mu.Lock()
	defer rp.mu.Unlock()
	was := rp.pending
	rp.pending = false
	return was
}

func (p *Pipeline) paceFor(repoID string) *repoPace {
	v, _ := p.pace.LoadOrStore(repoID, &repoPace{est: -1})
	return v.(*repoPace)
}

// paced reports whether a threshold-gated run for repoID must wait.
func (p *Pipeline) paced(repoID string) bool {
	if p.MinInterval <= 0 {
		return false
	}
	v, ok := p.pace.Load(repoID)
	if !ok {
		return false
	}
	rp := v.(*repoPace)
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return time.Now().Before(rp.next)
}

// ran records a finished run: opens the next pacing window and forgets the
// pack estimate (the run changed the count; the next nudge re-reads it).
func (p *Pipeline) ran(repoID string, start time.Time, err error) {
	rp := p.paceFor(repoID)
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if p.MinInterval > 0 {
		rp.next = time.Now().Add(max(p.MinInterval, paceDutyFactor*time.Since(start)))
	}
	if err == nil {
		rp.lastStart = start
	}
	rp.est = -1
}

// mayNeed is NudgeIfNeeded's cheap pre-check, counting this nudge's pack:
// false while a run is in flight (which then re-checks when it ends), while
// paced, or while the estimate says the threshold can't have been reached.
func (p *Pipeline) mayNeed(repoID string) bool {
	rp := p.paceFor(repoID)
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if _, busy := p.inflight.Load(repoID); busy {
		rp.pending = true
		return false
	}
	if rp.est >= 0 {
		rp.est++
	}
	if p.MinInterval > 0 && time.Now().Before(rp.next) {
		return false
	}
	return rp.est < 0 || rp.est >= p.MinPacks
}

// observed records an exact pack count read by NudgeIfNeeded.
func (p *Pipeline) observed(repoID string, packs int) {
	rp := p.paceFor(repoID)
	rp.mu.Lock()
	rp.est = packs
	rp.mu.Unlock()
}

// quiet reports whether a below-threshold repo has gone QuietAfter without
// a new pack and has not been maintained since its newest pack landed.
func (p *Pipeline) quiet(repoID string, packs []repodb.Pack) bool {
	var newest time.Time
	for _, pk := range packs {
		if pk.CreatedAt.After(newest) {
			newest = pk.CreatedAt
		}
	}
	if time.Since(newest) < p.QuietAfter {
		return false
	}
	v, ok := p.pace.Load(repoID)
	if !ok {
		return true
	}
	rp := v.(*repoPace)
	rp.mu.Lock()
	defer rp.mu.Unlock()
	// created_at has one-second resolution; a run that started within the
	// newest pack's second may not have seen it.
	return rp.lastStart.IsZero() || !rp.lastStart.After(newest.Add(time.Second))
}
