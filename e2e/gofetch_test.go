package e2e

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/config"
	"github.com/folsomintel/forge/internal/ingest"
)

func goFetchEnv(t *testing.T) *env {
	t.Helper()
	return startServerWith(t, func(cfg *config.Config) {
		cfg.GoReceive = true
		cfg.GoFetch = true
	})
}

// The incremental fast path: a follower fetching pushes that arrived via
// goreceive gets the stored receive packs re-emitted - no git fork.
func TestGoFetchIncrementalChain(t *testing.T) {
	t.Parallel()
	e := goFetchEnv(t)
	e.createRepo("inc")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("inc", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")
	os.WriteFile(filepath.Join(work, "f.txt"), []byte("base\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	// The follower clones at base.
	follower := filepath.Join(e.dir, "follower")
	e.git(e.dir, "clone", e.remote("inc", ""), follower)

	// Two more pushes land via goreceive (a 2-pack chain for the follower).
	for i := 0; i < 2; i++ {
		os.WriteFile(filepath.Join(work, "f.txt"), []byte(fmt.Sprintf("base\nrev %d\n", i)), 0o644)
		e.git(work, "add", ".")
		e.git(work, "commit", "-q", "-m", fmt.Sprintf("rev %d", i))
		e.git(work, "push", "-q", "origin", "HEAD:main")
	}

	before := e.srv.GitHTTP.GoFetchStatsSnapshot()
	e.git(follower, "fetch", "-q", "origin")
	e.git(follower, "merge", "-q", "--ff-only", "origin/main")
	after := e.srv.GitHTTP.GoFetchStatsSnapshot()

	if after.Eligible <= before.Eligible {
		t.Fatalf("incremental fetch did not use the fast path: before=%+v after=%+v", before, after)
	}
	got, err := os.ReadFile(filepath.Join(follower, "f.txt"))
	if err != nil || string(got) != "base\nrev 1\n" {
		t.Fatalf("follower content = %q, %v", got, err)
	}
	if out := e.git(follower, "fsck", "--strict"); out != "" {
		t.Logf("fsck: %s", out)
	}
}

// Clone passthrough: a consolidated repo clones straight from the stored
// gc pack, without materialization or a git fork.
func TestGoFetchClonePassthrough(t *testing.T) {
	t.Parallel()
	e := goFetchEnv(t)
	e.createRepo("cold")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("cold", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")
	for i := 0; i < 3; i++ {
		os.WriteFile(filepath.Join(work, "f.txt"), []byte(fmt.Sprintf("v%d\n", i)), 0o644)
		e.git(work, "add", ".")
		e.git(work, "commit", "-q", "-m", fmt.Sprintf("c%d", i))
		e.git(work, "push", "-q", "origin", "HEAD:main")
	}
	// Consolidate to a single gc pack.
	e.mustAPI("POST", "/api/repos/cold/maintenance", map[string]any{}, http.StatusOK)

	before := e.srv.GitHTTP.GoFetchStatsSnapshot()
	clone := filepath.Join(e.dir, "clone")
	e.git(e.dir, "clone", e.remote("cold", ""), clone)
	after := e.srv.GitHTTP.GoFetchStatsSnapshot()

	if after.CloneStream <= before.CloneStream {
		t.Fatalf("clone did not stream from the store: before=%+v after=%+v", before, after)
	}
	got, err := os.ReadFile(filepath.Join(clone, "f.txt"))
	if err != nil || string(got) != "v2\n" {
		t.Fatalf("clone content = %q, %v", got, err)
	}
}

// Cross-branch blob reuse via real git: whatever git chooses to put in
// the push pack, the follower's fetch must end up byte-correct (served
// fast when the pack is self-contained, via git otherwise).
func TestGoFetchCrossBranchBlobReuse(t *testing.T) {
	t.Parallel()
	e := goFetchEnv(t)
	e.createRepo("lin")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("lin", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")

	os.WriteFile(filepath.Join(work, "readme.txt"), []byte("base\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	follower := filepath.Join(e.dir, "follower")
	e.git(e.dir, "clone", e.remote("lin", ""), follower)

	e.git(work, "checkout", "-q", "-b", "side")
	os.WriteFile(filepath.Join(work, "unique.txt"), []byte("SHARED-CONTENT-9d2f\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "side adds X")
	e.git(work, "push", "-q", "origin", "side")

	e.git(work, "checkout", "-q", "main")
	os.WriteFile(filepath.Join(work, "copy.txt"), []byte("SHARED-CONTENT-9d2f\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "main reuses X")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	e.git(follower, "fetch", "-q", "origin", "main")
	e.git(follower, "merge", "-q", "--ff-only", "origin/main")
	got, err := os.ReadFile(filepath.Join(follower, "copy.txt"))
	if err != nil || string(got) != "SHARED-CONTENT-9d2f\n" {
		t.Fatalf("follower missing reused blob: %q, %v", got, err)
	}
}

// The soundness case that MUST fall back, constructed deterministically: a
// hand-crafted push whose pack references a server-known blob WITHOUT
// including it. The chain pack alone would hand a follower a broken
// closure; the lineage proof must reject it and git must serve the fetch.
func TestGoFetchLineageFallback(t *testing.T) {
	t.Parallel()
	e := goFetchEnv(t)
	e.createRepo("lin2")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("lin2", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")

	// base on main (distinct content), then branch keep introduces blob X.
	os.WriteFile(filepath.Join(work, "readme.txt"), []byte("base\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "push", "-q", "origin", "HEAD:main")
	baseTip := trim(e.git(work, "rev-parse", "HEAD"))

	e.git(work, "checkout", "-q", "-b", "keep")
	os.WriteFile(filepath.Join(work, "x.txt"), []byte("LINEAGE-X\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "keep adds X")
	e.git(work, "push", "-q", "origin", "keep")
	blobX := trim(e.git(work, "rev-parse", "HEAD:x.txt"))

	// Follower knows ONLY main@base (single branch: no keep, no X).
	follower := filepath.Join(e.dir, "follower")
	e.git(e.dir, "clone", "--single-branch", "--branch", "main", e.remote("lin2", ""), follower)

	// Hand-crafted push to main: commit whose tree references X, pack
	// deliberately WITHOUT the blob (the server has it via keep, so
	// connectivity passes and it lands as an external).
	newTip := e.rawPushReferencing(t, "lin2", "refs/heads/main", baseTip, blobX)

	before := e.srv.GitHTTP.GoFetchStatsSnapshot()
	e.git(follower, "fetch", "-q", "origin", "main")
	e.git(follower, "merge", "-q", "--ff-only", newTip)
	after := e.srv.GitHTTP.GoFetchStatsSnapshot()

	// Content correct (git served it after the lineage rejection)...
	got, err := os.ReadFile(filepath.Join(follower, "copy.txt"))
	if err != nil || string(got) != "LINEAGE-X\n" {
		t.Fatalf("follower missing externally-referenced blob: %q, %v", got, err)
	}
	// ...and the fast path must NOT have served it.
	if after.Eligible > before.Eligible {
		t.Fatalf("lineage-unsafe fetch served by fast path: before=%+v after=%+v", before, after)
	}
	if after.FellBackBy["lineage"] <= before.FellBackBy["lineage"] {
		t.Fatalf("expected a lineage rejection: before=%+v after=%+v", before, after)
	}
}

func trim(s string) string { return strings.TrimSpace(s) }

// rawPushReferencing hand-crafts a push: a commit whose tree references
// blobX WITHOUT shipping the blob in the pack - the shape a git client
// produces when it knows the server already has the object. Returns the
// new tip oid.
func (e *env) rawPushReferencing(t *testing.T, repo, ref, parent, blobX string) string {
	t.Helper()
	rawX, err := hex.DecodeString(blobX)
	if err != nil || len(rawX) != 20 {
		t.Fatalf("bad blob oid %q", blobX)
	}
	tree := append([]byte("100644 copy.txt\x00"), rawX...)
	treeOID := gitOID("tree", tree)
	commit := fmt.Sprintf(
		"tree %s\nparent %s\nauthor t <t@t> 1700000000 +0000\ncommitter t <t@t> 1700000000 +0000\n\nreuse X\n",
		treeOID, parent)
	commitOID := gitOID("commit", []byte(commit))

	pack := ingest.WritePack([]ingest.PackObject{
		{Type: "commit", Data: []byte(commit)},
		{Type: "tree", Data: tree},
	})

	var body bytes.Buffer
	cmd := fmt.Sprintf("%s %s %s\x00report-status agent=e2e-raw/1", parent, commitOID, ref)
	fmt.Fprintf(&body, "%04x%s", len(cmd)+4, cmd)
	body.WriteString("0000")
	body.Write(pack)

	req, _ := http.NewRequest("POST", e.base+"/"+repo+".git/git-receive-pack", &body)
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("raw push: %v", err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !bytes.Contains(out, []byte("unpack ok")) || bytes.Contains(out, []byte("ng ")) {
		t.Fatalf("raw push rejected: %d %q", res.StatusCode, out)
	}
	return commitOID
}

func gitOID(typ string, data []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", typ, len(data))
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Security: an ephemeral (refs/namespaces/*) tip must not be fetchable
// through the normal view by oid, and a normal clone must not receive
// ephemeral objects via clone passthrough.
func TestGoFetchNamespaceIsolation(t *testing.T) {
	t.Parallel()
	e := goFetchEnv(t)
	e.createRepo("nsiso")

	work := filepath.Join(e.dir, "work")
	e.git(e.dir, "clone", e.remote("nsiso", ""), work)
	e.git(work, "config", "user.email", "t@example.com")
	e.git(work, "config", "user.name", "t")
	os.WriteFile(filepath.Join(work, "a.txt"), []byte("base\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "base")
	e.git(work, "push", "-q", "origin", "HEAD:main")

	// Push a secret to an ephemeral namespace.
	os.WriteFile(filepath.Join(work, "secret.txt"), []byte("TOP-SECRET\n"), 0o644)
	e.git(work, "add", ".")
	e.git(work, "commit", "-q", "-m", "secret")
	ephTip := trim(e.git(work, "rev-parse", "HEAD"))
	e.git(work, "push", "-q", e.remote("nsiso", "+ephemeral"), "HEAD:refs/heads/pr1")

	// The fetch fast path must NOT serve the ephemeral tip by oid: it is not
	// a visible ref tip, so the visibility gate declines it (whatever the
	// fork path then chooses to do under git's own namespace rules).
	before := e.srv.GitHTTP.GoFetchStatsSnapshot()
	victim := filepath.Join(e.dir, "victim")
	e.git(e.dir, "clone", "-q", e.remote("nsiso", ""), victim)
	e.git(victim, "config", "user.email", "v@e")
	e.git(victim, "config", "user.name", "v")
	e.gitErr(victim, "fetch", "-q", "origin", ephTip) // outcome is git's call
	after := e.srv.GitHTTP.GoFetchStatsSnapshot()
	if after.Eligible > before.Eligible {
		t.Fatalf("fast path served an ephemeral oid: before=%+v after=%+v", before, after)
	}

	// Consolidate, then a fresh normal clone must not contain the secret
	// (clone passthrough must bail because a hidden ref exists).
	e.mustAPI("POST", "/api/repos/nsiso/maintenance", map[string]any{}, http.StatusOK)
	fresh := filepath.Join(e.dir, "fresh")
	e.git(e.dir, "clone", "-q", e.remote("nsiso", ""), fresh)
	if out := e.git(fresh, "cat-file", "--batch-all-objects", "--batch-check"); strings.Contains(out, ephTip) {
		t.Fatal("clone passthrough leaked the ephemeral commit into a normal clone")
	}
	if _, err := os.Stat(filepath.Join(fresh, "secret.txt")); err == nil {
		t.Fatal("secret file present in normal clone")
	}
}
