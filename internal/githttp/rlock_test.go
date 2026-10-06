package githttp

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A reader behind a held write lock gives up when its request ends instead
// of parking forever, and still gets in once the writer leaves.
func TestRLockCtxGivesUpWithRequest(t *testing.T) {
	var l sync.RWMutex
	l.Lock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if rlockCtx(ctx, &l) {
		t.Fatal("got the read lock while a writer held it")
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("waited %v after the request ended", waited)
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		l.Unlock()
	}()
	if !rlockCtx(context.Background(), &l) {
		t.Fatal("read lock not acquired after the writer released")
	}
	l.RUnlock()
}
