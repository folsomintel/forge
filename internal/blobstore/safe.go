package blobstore

import (
	"errors"
	"fmt"
	"strings"
)

// ErrBadKey is returned when a repo id or blob name would escape its
// partition. The store is the final sink for every blob path, so it
// validates here rather than trusting each caller - a malicious name that
// slips past an upstream check (a crafted pack name, a signed bundle URL for
// a traversal repo) still cannot escape the repo prefix on disk or in S3.
var ErrBadKey = errors.New("invalid blob key")

// instancePrefix is the reserved store partition for instance-global truth
// (mirrors repodb.instancePrefix - blobstore can't import repodb). It starts
// with "_" precisely so it can never collide with a user repo id; the
// validator allows it explicitly since it's an internal constant, never user
// input, and carries no traversal.
const instancePrefix = "_forge"

// validRepoID mirrors repodb.ValidRepoID's rule. blobstore cannot import
// repodb (repodb imports blobstore), so the rule is duplicated here; keep the
// two in sync. First char alnum; rest alnum/._-; 1..100 bytes. That excludes
// "..", "/", and empty, so a repo id can never be a path-traversal segment.
func validRepoID(id string) bool {
	if id == instancePrefix {
		return true
	}
	if id == "" || len(id) > 100 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if i == 0 {
			if !alnum {
				return false
			}
			continue
		}
		if !(alnum || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// validName accepts a blob name, which legitimately carries subpaths
// ("refs/wal/5.json", "lfs/<oid>", "pack-<hex>.pack"), but rejects anything
// that could climb out of the repo prefix: absolute paths, backslashes, NUL
// or control bytes, and any "." / ".." / empty path segment.
func validName(name string) bool {
	if name == "" || len(name) > 512 {
		return false
	}
	if strings.HasPrefix(name, "/") || strings.Contains(name, `\`) {
		return false
	}
	for i := 0; i < len(name); i++ {
		if name[i] < 0x20 || name[i] == 0x7f {
			return false
		}
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// checkKey validates a (repoID, name) pair for the write/read/delete/copy
// methods.
func checkKey(repoID, name string) error {
	if !validRepoID(repoID) {
		return fmt.Errorf("%w: repo id %q", ErrBadKey, repoID)
	}
	if !validName(name) {
		return fmt.Errorf("%w: name %q", ErrBadKey, name)
	}
	return nil
}

// checkPrefix validates a List call: the repo id must be sound, and the
// (optional) prefix must not traverse. Empty prefix means "whole repo".
func checkPrefix(repoID, prefix string) error {
	if !validRepoID(repoID) {
		return fmt.Errorf("%w: repo id %q", ErrBadKey, repoID)
	}
	if prefix == "" {
		return nil
	}
	if strings.HasPrefix(prefix, "/") || strings.Contains(prefix, `\`) {
		return fmt.Errorf("%w: prefix %q", ErrBadKey, prefix)
	}
	// A prefix may end mid-segment ("refs/") so a trailing empty segment is
	// fine; only "." / ".." segments are traversal.
	for _, seg := range strings.Split(prefix, "/") {
		if seg == "." || seg == ".." {
			return fmt.Errorf("%w: prefix %q", ErrBadKey, prefix)
		}
	}
	return nil
}
