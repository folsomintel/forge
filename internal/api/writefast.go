package api

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
	"github.com/folsomintel/forge/internal/webhook"
)

// Fork-free API writes: build the new blobs, trees, and commit in pure Go
// (reading parent trees through the cat-file pool), pack them, and land
// refs + data in ONE batched conditional PUT via the inline-pack WAL.
// The fork path costs ~6 git forks + pack-objects + 2 store PUTs + CAS,
// all under the repo write lock; this path is ~1ms of hashing + a CAS the
// group commit amortizes, with NO repo lock - concurrent API writers
// batch instead of serializing. Any anomaly (odd modes, submodules in the
// path, oversize content, missing machinery) falls back to the fork path,
// which is always correct.

// fastChange is one path edit for the Go-native commit builder.
type fastChange struct {
	path    string // clean, non-empty
	content []byte // nil when delete
	mode    string // "100644" | "100755" | "120000"
	delete  bool
}

// fastWriteMax bounds the direct-put pack; larger content forks git.
const fastWriteMax = 8 << 20

// writeCommitGo builds and lands one commit without forking. handled=false
// means the caller must run the fork path. On success the returned commit
// matches the fork path's output shape.
func (s *Server) writeCommitGo(ctx context.Context, repoID, branch string, ephemeral bool, tip, message string, author Author, pusher string, changes []fastChange) (*gitcmd.Commit, error, bool) {
	total := 0
	for _, c := range changes {
		switch c.mode {
		case "", "100644", "100755", "120000":
		default:
			return nil, nil, false
		}
		total += len(c.content)
	}
	if total > fastWriteMax {
		return nil, nil, false
	}

	// Parent tree entries come through the pool; a fresh repo (tip == "")
	// starts from an empty tree and needs no local materialization at all.
	var dir string
	rootTree := ""
	if tip != "" {
		d, ok := s.readDir(ctx, repoID)
		if !ok {
			return nil, nil, false
		}
		dir = d
		rt, ok := s.commitTree(repoID, dir, tip)
		if !ok {
			return nil, nil, false
		}
		rootTree = rt
	}

	b := &treeBuilder{s: s, repoID: repoID, dir: dir}
	newRoot, ok := b.apply(rootTree, groupChanges(changes))
	if !ok {
		return nil, nil, false
	}
	if newRoot == rootTree {
		return nil, ingest.ErrNothingToCommit, true
	}

	// Strip ident-breaking bytes: name/email interpolate into the commit
	// header, so a newline would inject headers (fake parent/tree). git's
	// commit-tree does the same; the Go builder must not be weaker.
	sig := sanitizeIdent(author.Name)
	email := sanitizeIdent(author.Email)
	if sig == "" {
		sig = "forge"
	}
	if email == "" {
		email = "forge@localhost"
	}
	now := time.Now().Unix()
	var cb strings.Builder
	fmt.Fprintf(&cb, "tree %s\n", newRoot)
	if tip != "" {
		fmt.Fprintf(&cb, "parent %s\n", tip)
	}
	fmt.Fprintf(&cb, "author %s <%s> %d +0000\n", sig, email, now)
	// Drop NUL from the message: git -m (the fork path) truncates at it,
	// and a NUL in the object body is fsck's nulInCommit.
	message = strings.ReplaceAll(message, "\x00", "")
	fmt.Fprintf(&cb, "committer %s <%s> %d +0000\n\n%s\n", sig, email, now, message)
	commitBytes := []byte(cb.String())
	commitOID := gitOIDFor("commit", commitBytes)
	b.objects = append(b.objects, ingest.PackObject{Type: "commit", OID: commitOID, Data: commitBytes})

	pack := ingest.WritePack(dedupeObjects(b.objects))
	parsed, _, err := ingest.ReadPackThin(pack, nil)
	if err != nil {
		// We just emitted this pack; a parse failure means our own writer
		// produced something unreadable. Never commit an idx over it (an
		// empty idx indexes zero objects -> a silently corrupt pack) - fall
		// back to forking real git, which rebuilds pack+idx from scratch.
		return nil, nil, false
	}
	idx, err := ingest.WriteIdxV2(parsed)
	if err != nil {
		return nil, nil, false
	}
	name := "pack-" + packTrailerHex(pack)

	old := tip
	if old == "" {
		old = repodb.ZeroOID
	}
	ref := repodb.BranchRef(branch, ephemeral)
	update := repodb.RefUpdate{Name: ref, Old: old, New: commitOID}
	events := []repodb.Event{webhook.PushEventFor(repoID, update, pusher)}

	type inlineCommitter interface {
		UpdateRefsWithPack(ctx context.Context, repoID string, updates []repodb.RefUpdate, events []repodb.Event, pack *repodb.InlinePack) error
	}
	if ic, ok := s.DB.(inlineCommitter); ok && len(pack) <= repodb.InlinePackMax {
		err = ic.UpdateRefsWithPack(ctx, repoID, []repodb.RefUpdate{update}, events, &repodb.InlinePack{Name: name, Data: pack})
	} else {
		// Bigger content: pack + idx to the store first (parallel), then CAS.
		errs := make(chan error, 2)
		go func() { errs <- s.Ingest.Blobs.Put(ctx, repoID, name+".pack", strings.NewReader(string(pack))) }()
		go func() { errs <- s.Ingest.Blobs.Put(ctx, repoID, name+".idx", strings.NewReader(string(idx))) }()
		for i := 0; i < 2; i++ {
			if perr := <-errs; perr != nil {
				return nil, nil, false
			}
		}
		if aerr := s.DB.AddPacks(ctx, repoID, []repodb.Pack{{Name: name, SizeBytes: int64(len(pack)), Source: "api"}}); aerr != nil {
			return nil, nil, false
		}
		err = s.DB.UpdateRefs(ctx, repoID, []repodb.RefUpdate{update}, events)
	}
	if err != nil {
		if isCAS(err) {
			return nil, err, true
		}
		return nil, nil, false // transactional: nothing applied; fork path retries
	}
	// Best-effort local install so same-node reads skip the store - even
	// into a not-yet-materialized dir (a cold repo's next materialize would
	// otherwise race the async flush and pay a ~200ms negative GET).
	installLocalPack(s.Cache.RepoDir(repoID), name, pack, idx)
	s.Cache.Invalidate(repoID) // local refs lag the DB until next materialize
	return &gitcmd.Commit{
		SHA: commitOID, Tree: newRoot, Parents: parentsOf(tip),
		Author: sig, Email: email, Timestamp: now, Message: strings.TrimSpace(message),
	}, nil, true
}

func parentsOf(tip string) []string {
	if tip == "" {
		return nil
	}
	return []string{tip}
}

const emptyTreeOID = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

// --- tree construction ---

// changeNode groups changes by their next path component.
type changeNode struct {
	leaf     *fastChange            // a change AT this exact path
	children map[string]*changeNode // deeper changes
}

func groupChanges(changes []fastChange) *changeNode {
	root := &changeNode{children: map[string]*changeNode{}}
	for i := range changes {
		c := &changes[i]
		node := root
		comps := strings.Split(c.path, "/")
		for j, comp := range comps {
			if node.children == nil {
				node.children = map[string]*changeNode{}
			}
			child, ok := node.children[comp]
			if !ok {
				child = &changeNode{children: map[string]*changeNode{}}
				node.children[comp] = child
			}
			node = child
			if j == len(comps)-1 {
				node.leaf = c
			}
		}
	}
	return root
}

type treeBuilder struct {
	s       *Server
	repoID  string
	dir     string
	objects []ingest.PackObject
}

// apply merges the change node into the tree at treeOID ("" = empty) and
// returns the new tree's oid ("" propagated failures via ok=false; an
// empty result prunes the subtree entirely, returning emptyTreeOID at the
// root and "" markers handled by the caller).
func (b *treeBuilder) apply(treeOID string, node *changeNode) (string, bool) {
	var entries []gitcmd.TreeEntry
	if treeOID != "" && treeOID != emptyTreeOID {
		data, err := b.s.Cache.BlobContents(b.repoID, b.dir, treeOID)
		if err != nil {
			return "", false
		}
		parsed, err := gitcmd.ParseTreeRaw(data)
		if err != nil {
			return "", false
		}
		entries = parsed
	}
	byName := map[string]gitcmd.TreeEntry{}
	for _, e := range entries {
		byName[e.Path] = e
	}

	for name, child := range node.children {
		// Defense in depth: apply must never write a name the boundary
		// validator would reject, even if a future caller forgets to check.
		if !validTreeComponent(name) {
			return "", false
		}
		existing, exists := byName[name]
		switch {
		case child.leaf != nil && len(child.children) == 0:
			// A change lands exactly here.
			c := child.leaf
			if c.delete {
				if !exists || existing.Type != "blob" {
					return "", false // path missing / not a file: fork path decides the error
				}
				delete(byName, name)
				continue
			}
			if exists && existing.Type == "tree" {
				return "", false // writing a file over a directory: git's problem
			}
			mode := c.mode
			if mode == "" {
				mode = "100644"
			}
			oid := gitOIDFor("blob", c.content)
			b.objects = append(b.objects, ingest.PackObject{Type: "blob", OID: oid, Data: c.content})
			byName[name] = gitcmd.TreeEntry{Mode: mode, Type: "blob", SHA: oid, Path: name}
		case len(child.children) > 0 && child.leaf == nil:
			// Descend.
			sub := ""
			if exists {
				if existing.Type != "tree" {
					return "", false // descending through a file
				}
				sub = existing.SHA
			}
			newSub, ok := b.apply(sub, child)
			if !ok {
				return "", false
			}
			if newSub == "" || newSub == emptyTreeOID {
				delete(byName, name) // subtree emptied: prune
				continue
			}
			byName[name] = gitcmd.TreeEntry{Mode: "040000", Type: "tree", SHA: newSub, Path: name}
		default:
			return "", false // change at a path AND below it: nonsensical
		}
	}

	if len(byName) == 0 {
		return emptyTreeOID, true
	}
	out := make([]gitcmd.TreeEntry, 0, len(byName))
	for _, e := range byName {
		out = append(out, e)
	}
	// git's tree order: byte-wise on the name, with directories comparing
	// as if suffixed by "/". Getting this wrong makes fsck reject the tree.
	sort.Slice(out, func(i, j int) bool {
		return treeSortKey(out[i]) < treeSortKey(out[j])
	})
	raw := serializeTree(out)
	oid := gitOIDFor("tree", raw)
	b.objects = append(b.objects, ingest.PackObject{Type: "tree", OID: oid, Data: raw})
	return oid, true
}

func treeSortKey(e gitcmd.TreeEntry) string {
	if e.Type == "tree" {
		return e.Path + "/"
	}
	return e.Path
}

// serializeTree emits git's raw tree format. Raw trees carry "40000" for
// directories (NOT the zero-padded display form fsck rejects).
func serializeTree(entries []gitcmd.TreeEntry) []byte {
	var out []byte
	for _, e := range entries {
		mode := e.Mode
		if e.Type == "tree" {
			mode = "40000"
		}
		out = append(out, mode...)
		out = append(out, ' ')
		out = append(out, e.Path...)
		out = append(out, 0)
		raw, _ := hex.DecodeString(e.SHA)
		out = append(out, raw...)
	}
	return out
}

func gitOIDFor(typ string, data []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", typ, len(data))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// dedupeObjects drops duplicate OIDs (identical blobs written twice).
func dedupeObjects(objs []ingest.PackObject) []ingest.PackObject {
	seen := map[string]bool{}
	out := objs[:0]
	for _, o := range objs {
		if seen[o.OID] {
			continue
		}
		seen[o.OID] = true
		out = append(out, o)
	}
	return out
}

func packTrailerHex(pack []byte) string {
	return hex.EncodeToString(pack[len(pack)-20:])
}

// installLocalPack drops pack+idx into the cache repo (pack first: a pack
// is only discoverable via its idx). Best-effort.
func installLocalPack(dir, name string, pack, idx []byte) {
	packDir := dir + "/objects/pack"
	if err := os.MkdirAll(packDir, 0o755); err != nil {
		return
	}
	// Atomic tmp+rename, .pack before .idx (git discovers a pack via its
	// idx, so the idx must never point at a half-written pack).
	if repocache.WriteFileAtomic(packDir+"/"+name+".pack", pack) != nil {
		return
	}
	repocache.WriteFileAtomic(packDir+"/"+name+".idx", idx)
}
