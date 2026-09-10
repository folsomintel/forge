package packstore

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/folsomintel/forge/internal/blobstore"
)

// TestObjectReaderMatchesGit builds a real packfile WITH deltas via git, then
// reads every object back through the block-backed reader and compares type +
// bytes to git cat-file. This exercises OFS/REF delta resolution over the
// remote pack (nothing but the .idx is "local").
func TestObjectReaderMatchesGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	git("init", "-q")
	git("config", "user.email", "t@example.com")
	git("config", "user.name", "t")

	// Successive near-identical large files -> git deltas them on repack, so
	// the pack contains OFS/REF deltas to resolve (not just base objects).
	base := strings.Repeat("the quick brown fox jumps over the lazy dog\n", 4000)
	for i := 0; i < 6; i++ {
		content := base + "revision " + strconv.Itoa(i) + "\n" + strings.Repeat("x", i*137)
		if err := os.WriteFile(filepath.Join(dir, "big.txt"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		os.WriteFile(filepath.Join(dir, "small-"+strconv.Itoa(i)+".txt"), []byte("hello "+strconv.Itoa(i)), 0o644)
		git("add", "-A")
		git("commit", "-qm", "rev "+strconv.Itoa(i))
	}
	// One pack, delta-compressed, loose objects removed.
	git("repack", "-adq", "--window=50", "--depth=50")

	packDir := filepath.Join(dir, ".git", "objects", "pack")
	entries, _ := os.ReadDir(packDir)
	var packName string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".pack") {
			packName = strings.TrimSuffix(e.Name(), ".pack")
		}
	}
	if packName == "" {
		t.Fatal("no pack produced")
	}
	packBytes, _ := os.ReadFile(filepath.Join(packDir, packName+".pack"))
	idxBytes, _ := os.ReadFile(filepath.Join(packDir, packName+".idx"))

	// Serve the pack from a blob store; keep only the idx "local".
	store, err := blobstore.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	store.Put(ctx, "repo", packName+".pack", bytes.NewReader(packBytes))

	idx, err := ParseIndex(idxBytes)
	if err != nil {
		t.Fatal(err)
	}
	reader := Open(ctx, store, NewCache(4<<20), "repo", packName+".pack", int64(len(packBytes)))
	or := NewObjectReader(reader, reader.Size(), idx)

	// Enumerate every object with its type; verify count matches the idx.
	list := strings.Fields(git("cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)"))
	if len(list)%2 != 0 {
		t.Fatalf("unexpected batch-check output")
	}
	checked := 0
	for i := 0; i < len(list); i += 2 {
		oid, wantType := list[i], list[i+1]
		gotType, gotData, err := or.Object(oid)
		if err != nil {
			t.Fatalf("Object(%s): %v", oid, err)
		}
		if gotType != wantType {
			t.Fatalf("%s type = %q, want %q", oid, gotType, wantType)
		}
		// Compare bytes to `git cat-file <type> <oid>`.
		want := gitBytes(t, dir, wantType, oid)
		if !bytes.Equal(gotData, want) {
			t.Fatalf("%s (%s) content mismatch: got %d bytes, want %d", oid, wantType, len(gotData), len(want))
		}
		checked++
	}
	if checked != idx.Count() {
		t.Fatalf("checked %d objects, idx says %d", checked, idx.Count())
	}
	if checked < 10 {
		t.Fatalf("suspiciously few objects (%d); test not exercising deltas", checked)
	}
	// Sanity: a missing oid errors, not panics.
	if _, _, err := or.Object("0000000000000000000000000000000000000000"); err == nil {
		t.Fatal("expected error for absent oid")
	}
}

func gitBytes(t *testing.T, dir, typ, oid string) []byte {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "cat-file", typ, oid)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git cat-file %s %s: %v", typ, oid, err)
	}
	return out
}
