package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/config"
)

// Oversized pushes fail LOUDLY (receive.maxInputSize) instead of OOMing
// the machine into a silent hang - the 256MB-tier lesson. Offline import
// remains the path for monster migrations.
func TestOversizedPushFailsLoudly(t *testing.T) {
	e := startServerWith(t, func(c *config.Config) {
		c.MaxPushBytes = 64 << 10 // 64KB cap for the test
	})
	work := e.seedRepo("demo") // small seed fits under the cap

	// Random bytes: incompressible, so the pack genuinely exceeds the cap.
	raw := make([]byte, 512<<10)
	rand.Read(raw)
	writeFile(t, work, "big.bin", hex.EncodeToString(raw))
	e.git(work, "add", "-A")
	e.git(work, "commit", "-qm", "too big")
	out, err := e.gitErr(work, "push", "origin", "main")
	if err == nil {
		t.Fatal("oversized push was accepted")
	}
	if !strings.Contains(out, "max") && !strings.Contains(out, "exceeds") {
		t.Fatalf("expected a loud size error, got:\n%s", out)
	}

	// The repo still serves and the ref did not move.
	e.assertRepoIntegrity("demo")
}
