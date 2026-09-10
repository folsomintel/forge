package gitcmd

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestLogRejectsOptionInjection proves the --end-of-options guard neutralizes
// git argument injection through a caller-supplied revision. Without it,
// `git log --output=<file>` (the rev "--output=pwned" parsed as an option)
// writes a file in the repo dir. With it, git treats "--output=pwned" as a
// revision -> "unknown revision" error, and no file is written.
func TestLogRejectsOptionInjection(t *testing.T) {
	dir := seedGitRepo(t)
	x := &Exec{Ctx: context.Background(), Dir: dir}

	// The classic primitive: arbitrary file write in the process CWD.
	if _, err := x.Log("--output=pwned", "", 1); err == nil {
		t.Error("Log accepted an option-shaped revision without error")
	}
	if _, err := os.Stat(filepath.Join(dir, "pwned")); err == nil {
		t.Fatal("option injection: git wrote --output=pwned as a real git option")
	}

	// LsTree and ObjectType take the same untrusted revision.
	if _, err := x.LsTree("--output=pwned2", "", false); err == nil {
		t.Error("LsTree accepted an option-shaped treeish without error")
	}
	if _, err := os.Stat(filepath.Join(dir, "pwned2")); err == nil {
		t.Fatal("option injection via LsTree wrote a file")
	}
	if _, err := x.ObjectType("--batch-all-objects"); err == nil {
		t.Error("ObjectType accepted an option-shaped object without error")
	}

	// A legitimate revision still resolves after the guard.
	if _, err := x.Log("HEAD", "", 1); err != nil {
		t.Fatalf("Log(HEAD) should still work: %v", err)
	}
}

func seedGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-q")
	run("config", "user.email", "t@example.com")
	run("config", "user.name", "t")
	run("commit", "-q", "--allow-empty", "-m", "seed")
	return dir
}
