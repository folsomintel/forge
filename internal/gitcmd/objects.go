package gitcmd

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Fork-free object parsing: raw commit and tree bytes (as returned by the
// cat-file pool) parsed in Go, so the hot REST reads never spawn a git
// process. Formats are git's canonical on-disk encodings - stable since
// 2005.

// ParseTreeRaw decodes tree object bytes: repeated
// "<mode> <name>\x00<20-byte oid>". Modes are normalized to the 6-digit
// form ls-tree prints ("40000" -> "040000") so API output is unchanged.
func ParseTreeRaw(data []byte) ([]TreeEntry, error) {
	var out []TreeEntry
	for len(data) > 0 {
		sp := bytes.IndexByte(data, ' ')
		if sp <= 0 {
			return nil, fmt.Errorf("malformed tree entry (no mode)")
		}
		mode := string(data[:sp])
		rest := data[sp+1:]
		nul := bytes.IndexByte(rest, 0)
		if nul < 0 || nul+21 > len(rest) {
			return nil, fmt.Errorf("malformed tree entry (no oid)")
		}
		name := string(rest[:nul])
		oid := hex.EncodeToString(rest[nul+1 : nul+21])
		data = rest[nul+21:]

		if len(mode) == 5 {
			mode = "0" + mode
		}
		typ := "blob"
		switch mode {
		case "040000":
			typ = "tree"
		case "160000":
			typ = "commit" // submodule
		}
		out = append(out, TreeEntry{Mode: mode, Type: typ, SHA: oid, Path: name})
	}
	return out, nil
}

// CommitMeta is a fully parsed commit object, including the committer
// timestamp git's log traversal orders by (the API exposes the author
// timestamp, matching `git log --format=%at`).
type CommitMeta struct {
	Commit
	CommitterTS int64
}

// ParseCommitRaw decodes commit object bytes: header lines (tree, parent*,
// author, committer, ...) then a blank line and the message.
func ParseCommitRaw(sha string, data []byte) (CommitMeta, error) {
	cm := CommitMeta{Commit: Commit{SHA: sha}}
	rest := string(data)
	for {
		line, tail, ok := strings.Cut(rest, "\n")
		if !ok {
			return cm, fmt.Errorf("malformed commit: no header end")
		}
		rest = tail
		if line == "" {
			break // headers done; rest is the message
		}
		key, val, _ := strings.Cut(line, " ")
		switch key {
		case "tree":
			cm.Tree = val
		case "parent":
			cm.Parents = append(cm.Parents, val)
		case "author":
			name, email, ts := parseIdent(val)
			cm.Author, cm.Email, cm.Timestamp = name, email, ts
		case "committer":
			_, _, cm.CommitterTS = parseIdent(val)
		}
	}
	cm.Message = strings.TrimSpace(rest)
	if cm.Tree == "" {
		return cm, fmt.Errorf("malformed commit: no tree")
	}
	return cm, nil
}

// parseIdent splits "Name <email> ts tz".
func parseIdent(s string) (name, email string, ts int64) {
	lt := strings.IndexByte(s, '<')
	gt := strings.IndexByte(s, '>')
	if lt < 0 || gt < lt {
		return strings.TrimSpace(s), "", 0
	}
	name = strings.TrimSpace(s[:lt])
	email = s[lt+1 : gt]
	fields := strings.Fields(s[gt+1:])
	if len(fields) > 0 {
		ts, _ = strconv.ParseInt(fields[0], 10, 64)
	}
	return name, email, ts
}
