package blobstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidRepoIDAndName(t *testing.T) {
	badRepo := []string{"", "..", ".", "../evil", "a/b", "a\\b", ".hidden", "-lead", "/abs", strings.Repeat("x", 101)}
	for _, id := range badRepo {
		if validRepoID(id) {
			t.Errorf("validRepoID(%q) = true, want false", id)
		}
	}
	for _, id := range []string{"repo", "My-Repo_1", "a", "a.b.c", strings.Repeat("x", 100)} {
		if !validRepoID(id) {
			t.Errorf("validRepoID(%q) = false, want true", id)
		}
	}

	badName := []string{"", "/abs", "a/../b", "..", "a/..", "./x", "a//b", "a/./b", "a\\b", "x\x00y", strings.Repeat("x", 513)}
	for _, n := range badName {
		if validName(n) {
			t.Errorf("validName(%q) = true, want false", n)
		}
	}
	for _, n := range []string{"pack-abc.pack", "refs/wal/5.json", "lfs/deadbeef", "HEAD", "a.b/c.d"} {
		if !validName(n) {
			t.Errorf("validName(%q) = false, want true", n)
		}
	}
}

// TestLocalStoreRejectsTraversal proves the store itself is the last line of
// defense: a traversal name never escapes the repo prefix on disk, even if a
// caller passed it unvalidated.
func TestLocalStoreRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	l, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// A traversal that WOULD land outside root if joined naively.
	if err := l.Put(ctx, "repo", "../../escape", strings.NewReader("x")); !errors.Is(err, ErrBadKey) {
		t.Fatalf("Put traversal name = %v, want ErrBadKey", err)
	}
	if err := l.Put(ctx, "../repo", "f", strings.NewReader("x")); !errors.Is(err, ErrBadKey) {
		t.Fatalf("Put traversal repo = %v, want ErrBadKey", err)
	}
	// Nothing was written outside the repo prefix.
	if _, err := os.Stat(filepath.Join(root, "escape")); err == nil {
		t.Fatal("traversal wrote a file outside the repo prefix")
	}

	// A legitimate subpath still works.
	if err := l.Put(ctx, "repo", "refs/wal/1.json", strings.NewReader("ok")); err != nil {
		t.Fatalf("legit subpath Put failed: %v", err)
	}
	rc, err := l.Get(ctx, "repo", "refs/wal/1.json")
	if err != nil {
		t.Fatalf("legit Get failed: %v", err)
	}
	rc.Close()
}
