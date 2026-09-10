package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/gitcmd"
	"github.com/folsomintel/forge/internal/ingest"
	"github.com/folsomintel/forge/internal/repodb"
)

// Contents is a file (content set) or a directory (entries set).
type Contents struct {
	Type     string `json:"type" enum:"file,dir"`
	Path     string `json:"path"`
	SHA      string `json:"sha,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Encoding string `json:"encoding,omitempty"`
	Content  string `json:"content,omitempty" doc:"Base64 file content (files only)"`
	// TooLarge is set (with Content empty) when the file exceeds the inline
	// size cap; fetch it via /raw/{path} or clone instead of loading it into
	// a JSON body.
	TooLarge bool            `json:"too_large,omitempty"`
	Entries  []ContentsEntry `json:"entries,omitempty" doc:"Directory listing (dirs only)"`
}

// maxInlineContentBytes caps how large a blob getContents will base64 into a
// JSON response; larger files return metadata + TooLarge so a client streams
// them via /raw instead of OOMing the server on a big blob.
const maxInlineContentBytes = 10 << 20 // 10 MiB

type ContentsEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type" enum:"file,dir,commit"`
	SHA  string `json:"sha"`
	Size int64  `json:"size"`
	Mode string `json:"mode"`
}

type contentsGetIn struct {
	ID        string `path:"id"`
	Path      string `path:"path"`
	Ref       string `query:"ref" doc:"Branch, tag, full ref, or commit sha; default branch if empty"`
	Ephemeral bool   `query:"ephemeral"`
}

var (
	errStaleFile   = errors.New("stale file sha")
	errPathMissing = errors.New("path missing")
)

func (s *Server) registerContents(api huma.API) {
	huma.Register(api, op("getRootContents", "GET", "/api/repos/{id}/contents", auth.ScopeGitRead,
		"List the repository root"),
		func(ctx context.Context, in *struct {
			ID        string `path:"id"`
			Ref       string `query:"ref"`
			Ephemeral bool   `query:"ephemeral"`
		}) (*struct{ Body Contents }, error) {
			return s.getContents(ctx, in.ID, "", in.Ref, in.Ephemeral)
		})

	huma.Register(api, op("getContents", "GET", "/api/repos/{id}/contents/{path...}", auth.ScopeGitRead,
		"Get a file (base64) or directory listing"),
		func(ctx context.Context, in *contentsGetIn) (*struct{ Body Contents }, error) {
			return s.getContents(ctx, in.ID, cleanPath(in.Path), in.Ref, in.Ephemeral)
		})

	// Raw bytes live on their own path so the operation stays typed.
	huma.Register(api, op("getRawFile", "GET", "/api/repos/{id}/raw/{path...}", auth.ScopeGitRead,
		"Stream a file's raw bytes"),
		func(ctx context.Context, in *contentsGetIn) (*huma.StreamResponse, error) {
			filePath := cleanPath(in.Path)
			oid, _, err := s.resolveRev(ctx, in.ID, in.Ref, in.Ephemeral)
			if err != nil {
				return nil, huma.Error404NotFound("ref not found")
			}
			// Fork-free fast path for files under the inline cap: resolve +
			// read through the pool. Bigger files (or any anomaly) stream
			// through git below.
			if dir, ok := s.readDir(ctx, in.ID); ok {
				if root, ok := s.commitTree(in.ID, dir, oid); ok {
					entry, found, ok := s.resolveTreePath(in.ID, dir, root, filePath)
					if ok && (!found || entry.Type != "blob") {
						return nil, huma.Error404NotFound("file not found")
					}
					if ok && found {
						if _, size, err := s.Cache.ObjectInfo(in.ID, dir, entry.SHA); err == nil && size <= maxInlineContentBytes {
							if content, err := s.Cache.BlobContents(in.ID, dir, entry.SHA); err == nil {
								sha := entry.SHA
								pub := s.repoPublic(ctx, in.ID)
								return &huma.StreamResponse{Body: func(hc huma.Context) {
									if serveImmutable(hc, sha, pub) {
										return
									}
									hc.SetHeader("Content-Type", "application/octet-stream")
									hc.BodyWriter().Write(content)
								}}, nil
							}
						}
					}
				}
			}
			x, release, err := s.exec(ctx, in.ID)
			if err != nil {
				return nil, err
			}
			entries, err := x.LsTree(oid, filePath, false)
			if err != nil || len(entries) == 0 || entries[0].Path != filePath || entries[0].Type != "blob" {
				release()
				return nil, huma.Error404NotFound("file not found")
			}
			sha := entries[0].SHA
			pub := s.repoPublic(ctx, in.ID)
			return &huma.StreamResponse{Body: func(hc huma.Context) {
				defer release()
				// ETag on the resolved blob OID: if the file content changed
				// the OID changes, so the client always revalidates correctly.
				if serveImmutable(hc, sha, pub) {
					return
				}
				hc.SetHeader("Content-Type", "application/octet-stream")
				x.RunStream(hc.BodyWriter(), "cat-file", "blob", "--end-of-options", sha)
			}}, nil
		})

	putOp := op("putContents", "PUT", "/api/repos/{id}/contents/{path...}", auth.ScopeGitWrite,
		"Create or update one file as a commit")
	putOp.DefaultStatus = http.StatusCreated
	huma.Register(api, putOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Path string `path:"path"`
		Body ContentsWrite
	}) (*struct{ Body ContentsWriteResult }, error) {
		return s.contentsWrite(ctx, in.ID, cleanPath(in.Path), in.Body, false)
	})

	delOp := op("deleteContents", "DELETE", "/api/repos/{id}/contents/{path...}", auth.ScopeGitWrite,
		"Delete one file as a commit")
	delOp.DefaultStatus = http.StatusCreated
	huma.Register(api, delOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Path string `path:"path"`
		Body ContentsWrite
	}) (*struct{ Body ContentsWriteResult }, error) {
		return s.contentsWrite(ctx, in.ID, cleanPath(in.Path), in.Body, true)
	})

	commitOp := op("commitFiles", "POST", "/api/repos/{id}/commits", auth.ScopeGitWrite,
		"Create one commit that adds, updates, and/or deletes multiple files atomically")
	commitOp.DefaultStatus = http.StatusCreated
	huma.Register(api, commitOp, func(ctx context.Context, in *struct {
		ID   string `path:"id"`
		Body MultiCommitWrite
	}) (*struct{ Body ContentsWriteResult }, error) {
		return s.commitFiles(ctx, in.ID, in.Body)
	})
}

func (s *Server) getContents(ctx context.Context, repoID, filePath, ref string, ephemeral bool) (*struct{ Body Contents }, error) {
	oid, _, err := s.resolveRev(ctx, repoID, ref, ephemeral)
	if err != nil {
		// An existing-but-empty repo (no commits yet) lists its root as empty
		// rather than 404 - so a freshly created repo is browsable and clients
		// don't have to special-case "not found" vs "no content".
		if filePath == "" {
			if _, gerr := s.DB.GetRepo(ctx, repoID); gerr == nil {
				return &struct{ Body Contents }{dirContents("", nil)}, nil
			}
		}
		return nil, huma.Error404NotFound("ref not found")
	}

	// Fork-free fast path: pooled cat-file + Go parsers; any anomaly falls
	// through to the fork path below.
	if out, herr, handled := s.getContentsGo(ctx, repoID, filePath, oid); handled {
		return out, herr
	}

	x, release, err := s.exec(ctx, repoID)
	if err != nil {
		return nil, err
	}
	defer release()

	if filePath == "" {
		entries, err := x.LsTree(oid, "", false)
		if err != nil {
			return nil, huma.Error404NotFound("tree not found")
		}
		return &struct{ Body Contents }{dirContents("", entries)}, nil
	}
	entries, err := x.LsTree(oid, filePath, false)
	if err != nil || len(entries) == 0 {
		return nil, huma.Error404NotFound("path not found")
	}
	if entries[0].Path == filePath && entries[0].Type == "blob" {
		e := entries[0]
		if e.Size > maxInlineContentBytes {
			// Don't load a huge blob into a JSON body; point the client at /raw.
			return &struct{ Body Contents }{Contents{
				Type: "file", Path: e.Path, SHA: e.SHA, Size: e.Size, TooLarge: true,
			}}, nil
		}
		content, err := s.Cache.BlobContents(repoID, x.Dir, e.SHA)
		if err != nil {
			return nil, internalErr("cat-file", err)
		}
		return &struct{ Body Contents }{Contents{
			Type: "file", Path: e.Path, SHA: e.SHA, Size: e.Size,
			Encoding: "base64", Content: base64.StdEncoding.EncodeToString(content),
		}}, nil
	}
	children, err := x.LsTree(oid, filePath+"/", false)
	if err != nil || len(children) == 0 {
		return nil, huma.Error404NotFound("path not found")
	}
	return &struct{ Body Contents }{dirContents(filePath, children)}, nil
}

// getContentsGo serves contents from the cat-file pool without forking.
// handled=false means machinery failure - the caller runs the fork path.
func (s *Server) getContentsGo(ctx context.Context, repoID, filePath, oid string) (*struct{ Body Contents }, error, bool) {
	dir, ok := s.readDir(ctx, repoID)
	if !ok {
		return nil, nil, false
	}
	root, ok := s.commitTree(repoID, dir, oid)
	if !ok {
		return nil, nil, false
	}
	entry, found, ok := s.resolveTreePath(repoID, dir, root, filePath)
	if !ok {
		return nil, nil, false
	}
	if !found {
		return nil, huma.Error404NotFound("path not found"), true
	}
	switch entry.Type {
	case "tree":
		entries, ok := s.listTree(repoID, dir, entry.SHA, filePath)
		if !ok {
			return nil, nil, false
		}
		return &struct{ Body Contents }{dirContents(filePath, entries)}, nil, true
	case "blob":
		_, size, err := s.Cache.ObjectInfo(repoID, dir, entry.SHA)
		if err != nil {
			return nil, nil, false
		}
		if size > maxInlineContentBytes {
			return &struct{ Body Contents }{Contents{
				Type: "file", Path: filePath, SHA: entry.SHA, Size: size, TooLarge: true,
			}}, nil, true
		}
		content, err := s.Cache.BlobContents(repoID, dir, entry.SHA)
		if err != nil {
			return nil, nil, false
		}
		return &struct{ Body Contents }{Contents{
			Type: "file", Path: filePath, SHA: entry.SHA, Size: size,
			Encoding: "base64", Content: base64.StdEncoding.EncodeToString(content),
		}}, nil, true
	default:
		// Submodule (gitlink): matches the fork path's not-found answer.
		return nil, huma.Error404NotFound("path not found"), true
	}
}

func dirContents(dir string, entries []gitcmd.TreeEntry) Contents {
	out := Contents{Type: "dir", Path: dir, Entries: []ContentsEntry{}}
	for _, e := range entries {
		t := e.Type
		if t == "blob" {
			t = "file"
		} else if t == "tree" {
			t = "dir"
		}
		out.Entries = append(out.Entries, ContentsEntry{
			Name: path.Base(e.Path), Path: e.Path, Type: t, SHA: e.SHA, Size: e.Size, Mode: e.Mode,
		})
	}
	return out
}

type ContentsWrite struct {
	Message   string `json:"message,omitempty"`
	Content   string `json:"content,omitempty" doc:"Base64 file content (PUT only)"`
	Branch    string `json:"branch,omitempty"`
	Ephemeral bool   `json:"ephemeral,omitempty"`
	SHA       string `json:"sha,omitempty" doc:"Expected current blob sha (file-level compare-and-swap)"`
	// ExpectedHead is a branch-level precondition: the write is rejected with
	// 409 unless the branch tip currently equals it. Optimistic concurrency
	// for automation that read the branch, then writes back.
	ExpectedHead string `json:"expected_head,omitempty" doc:"Expected current branch tip (branch-level compare-and-swap)"`
	Mode         string `json:"mode,omitempty" enum:",100644,100755,120000"`
	Author       Author `json:"author,omitzero"`
}

type Author struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
}

type ContentsWriteResult struct {
	Commit gitcmd.Commit `json:"commit"`
	Path   string        `json:"path"`
	Branch string        `json:"branch"`
}

func (s *Server) contentsWrite(ctx context.Context, repoID, filePath string, req ContentsWrite, isDelete bool) (*struct{ Body ContentsWriteResult }, error) {
	if filePath == "" {
		return nil, huma.Error400BadRequest("path required")
	}
	if !validTreePath(filePath) {
		return nil, huma.Error400BadRequest("invalid path: components cannot be empty, '.', '..', or '.git'")
	}
	if req.Message == "" {
		verb := "Update"
		if isDelete {
			verb = "Delete"
		}
		req.Message = fmt.Sprintf("%s %s", verb, filePath)
	}
	var content []byte
	if !isDelete {
		var err error
		content, err = base64.StdEncoding.DecodeString(req.Content)
		if err != nil {
			return nil, huma.Error400BadRequest("content must be base64")
		}
	}
	mode := req.Mode
	if mode == "" {
		mode = "100644"
	}
	repo, err := s.DB.GetRepo(ctx, repoID)
	if err != nil {
		return nil, huma.Error404NotFound("repository not found")
	}
	branch := req.Branch
	if branch == "" {
		branch = repo.DefaultBranch
	}
	tip, _, _ := s.resolveRev(ctx, repoID, branch, req.Ephemeral)
	if isDelete && tip == "" {
		return nil, huma.Error404NotFound("branch not found")
	}
	if req.ExpectedHead != "" && req.ExpectedHead != tip {
		return nil, huma.Error409Conflict("branch head moved - expected " + req.ExpectedHead + ", got " + tip)
	}

	spec := ingest.CommitSpec{
		RepoID:  repoID,
		Ref:     repodb.BranchRef(branch, req.Ephemeral),
		OldOID:  tip,
		Message: req.Message,
		Author:  ingest.Sig{Name: req.Author.Name, Email: req.Author.Email},
		Build: func(x *gitcmd.Exec) (string, error) {
			if tip != "" {
				if _, err := x.Run("read-tree", tip); err != nil {
					return "", err
				}
				cur, _ := x.LsTree(tip, filePath, false)
				curSHA := ""
				if len(cur) > 0 && cur[0].Path == filePath {
					curSHA = cur[0].SHA
				}
				if req.SHA != "" && req.SHA != curSHA {
					return "", errStaleFile
				}
				if isDelete && curSHA == "" {
					return "", errPathMissing
				}
			} else if _, err := x.Run("read-tree", "--empty"); err != nil {
				return "", err
			}
			if isDelete {
				rm := fmt.Sprintf("0 %s\t%s\n", repodb.ZeroOID, filePath)
				if _, err := x.RunIn(strings.NewReader(rm), "update-index", "--index-info"); err != nil {
					return "", err
				}
			} else {
				blob, err := x.RunIn(strings.NewReader(string(content)), "hash-object", "-w", "--stdin")
				if err != nil {
					return "", err
				}
				info := fmt.Sprintf("%s,%s,%s", mode, strings.TrimSpace(blob), filePath)
				if _, err := x.Run("update-index", "--add", "--cacheinfo", info); err != nil {
					return "", err
				}
			}
			return x.Run("write-tree")
		},
	}
	if tip != "" {
		spec.Parents = []string{tip}
	}

	// Fork-free fast path: the per-file CAS/missing checks run through the
	// pool, then the commit lands via the Go builder + inline-pack WAL. Any
	// anomaly falls through to the fork path (spec) below.
	commit, err, handled := s.contentsWriteGo(ctx, repoID, filePath, req, isDelete, tip, branch, mode, content)
	if !handled {
		commit, err = s.writeCommit(ctx, spec)
	}
	switch {
	case errors.Is(err, errStaleFile):
		return nil, huma.Error409Conflict("file sha does not match - fetch the latest content and retry")
	case errors.Is(err, errPathMissing):
		return nil, huma.Error404NotFound("path not found")
	case errors.Is(err, ingest.ErrNothingToCommit):
		// Idempotent: the file already holds exactly this content, so the
		// desired state is met. Return the current tip commit as success
		// rather than erroring, so a retried PUT is a no-op, not a 409.
		if x, release, xerr := s.exec(ctx, repoID); xerr == nil {
			defer release()
			if c, cerr := x.GetCommit(tip); cerr == nil {
				return &struct{ Body ContentsWriteResult }{ContentsWriteResult{Commit: *c, Path: filePath, Branch: branch}}, nil
			}
		}
		return &struct{ Body ContentsWriteResult }{ContentsWriteResult{Commit: gitcmd.Commit{SHA: tip}, Path: filePath, Branch: branch}}, nil
	case isCAS(err):
		return nil, huma.Error409Conflict("branch moved concurrently - retry")
	case err != nil:
		return nil, internalErr("contents write", err)
	}
	return &struct{ Body ContentsWriteResult }{ContentsWriteResult{Commit: *commit, Path: filePath, Branch: branch}}, nil
}

// FileChange is one path's edit within a multi-file commit: an upsert
// (base64 content) or a delete.
type FileChange struct {
	Path    string `json:"path"`
	Content string `json:"content,omitempty" doc:"Base64 file content (omit when delete=true)"`
	Mode    string `json:"mode,omitempty" enum:",100644,100755,120000"`
	Delete  bool   `json:"delete,omitempty"`
}

type MultiCommitWrite struct {
	Message      string       `json:"message,omitempty"`
	Branch       string       `json:"branch,omitempty"`
	Ephemeral    bool         `json:"ephemeral,omitempty"`
	ExpectedHead string       `json:"expected_head,omitempty" doc:"Expected current branch tip (branch-level compare-and-swap)"`
	Author       Author       `json:"author,omitzero"`
	Changes      []FileChange `json:"changes"`
}

// commitFiles applies several file changes as one atomic commit - the write
// analogue of a real git commit, so callers stop stacking N single-file PUTs
// (N commits, N CAS rounds) to land one logical change.
func (s *Server) commitFiles(ctx context.Context, repoID string, req MultiCommitWrite) (*struct{ Body ContentsWriteResult }, error) {
	if len(req.Changes) == 0 {
		return nil, huma.Error400BadRequest("at least one change required")
	}
	type staged struct {
		path, mode string
		content    []byte
		del        bool
	}
	items := make([]staged, 0, len(req.Changes))
	for _, ch := range req.Changes {
		p := cleanPath(ch.Path)
		if p == "" || !validTreePath(p) {
			return nil, huma.Error400BadRequest("invalid change path: components cannot be empty, '.', '..', or '.git'")
		}
		it := staged{path: p, del: ch.Delete, mode: ch.Mode}
		if it.mode == "" {
			it.mode = "100644"
		}
		if !ch.Delete {
			c, err := base64.StdEncoding.DecodeString(ch.Content)
			if err != nil {
				return nil, huma.Error400BadRequest("content must be base64: " + p)
			}
			it.content = c
		}
		items = append(items, it)
	}
	repo, err := s.DB.GetRepo(ctx, repoID)
	if err != nil {
		return nil, huma.Error404NotFound("repository not found")
	}
	branch := req.Branch
	if branch == "" {
		branch = repo.DefaultBranch
	}
	if req.Message == "" {
		req.Message = fmt.Sprintf("Update %d file(s)", len(items))
	}
	tip, _, _ := s.resolveRev(ctx, repoID, branch, req.Ephemeral)
	if req.ExpectedHead != "" && req.ExpectedHead != tip {
		return nil, huma.Error409Conflict("branch head moved - expected " + req.ExpectedHead + ", got " + tip)
	}
	spec := ingest.CommitSpec{
		RepoID:  repoID,
		Ref:     repodb.BranchRef(branch, req.Ephemeral),
		OldOID:  tip,
		Message: req.Message,
		Author:  ingest.Sig{Name: req.Author.Name, Email: req.Author.Email},
		Build: func(x *gitcmd.Exec) (string, error) {
			if tip != "" {
				if _, err := x.Run("read-tree", tip); err != nil {
					return "", err
				}
			} else if _, err := x.Run("read-tree", "--empty"); err != nil {
				return "", err
			}
			for _, it := range items {
				if it.del {
					rm := fmt.Sprintf("0 %s\t%s\n", repodb.ZeroOID, it.path)
					if _, err := x.RunIn(strings.NewReader(rm), "update-index", "--index-info"); err != nil {
						return "", err
					}
					continue
				}
				blob, err := x.RunIn(strings.NewReader(string(it.content)), "hash-object", "-w", "--stdin")
				if err != nil {
					return "", err
				}
				info := fmt.Sprintf("%s,%s,%s", it.mode, strings.TrimSpace(blob), it.path)
				if _, err := x.Run("update-index", "--add", "--cacheinfo", info); err != nil {
					return "", err
				}
			}
			return x.Run("write-tree")
		},
	}
	if tip != "" {
		spec.Parents = []string{tip}
	}
	// Fork-free fast path for the whole multi-file commit.
	fcs := make([]fastChange, len(items))
	for i, it := range items {
		fcs[i] = fastChange{path: it.path, mode: it.mode, content: it.content, delete: it.del}
	}
	commit, err, handled := s.writeCommitGo(ctx, repoID, branch, req.Ephemeral, tip, req.Message, req.Author, subject(ctx), fcs)
	if !handled {
		commit, err = s.writeCommit(ctx, spec)
	}
	switch {
	case errors.Is(err, ingest.ErrNothingToCommit):
		return nil, huma.Error409Conflict("nothing to commit: changes leave the tree unchanged")
	case isCAS(err):
		return nil, huma.Error409Conflict("branch moved concurrently - retry")
	case err != nil:
		return nil, internalErr("multi-file commit", err)
	}
	return &struct{ Body ContentsWriteResult }{ContentsWriteResult{Commit: *commit, Path: "", Branch: branch}}, nil
}

// contentsWriteGo runs the single-file write fork-free: per-file CAS and
// delete-missing checks through the pool, then the Go commit builder.
// handled=false -> the caller runs the fork path (which re-checks
// everything itself, so a bail-out here never skips validation).
func (s *Server) contentsWriteGo(ctx context.Context, repoID, filePath string, req ContentsWrite, isDelete bool, tip, branch, mode string, content []byte) (*gitcmd.Commit, error, bool) {
	if tip != "" && (req.SHA != "" || isDelete) {
		dir, ok := s.readDir(ctx, repoID)
		if !ok {
			return nil, nil, false
		}
		root, ok := s.commitTree(repoID, dir, tip)
		if !ok {
			return nil, nil, false
		}
		entry, found, ok := s.resolveTreePath(repoID, dir, root, filePath)
		if !ok {
			return nil, nil, false
		}
		curSHA := ""
		if found && entry.Type == "blob" {
			curSHA = entry.SHA
		}
		if req.SHA != "" && req.SHA != curSHA {
			return nil, errStaleFile, true
		}
		if isDelete && curSHA == "" {
			return nil, errPathMissing, true
		}
	}
	ch := fastChange{path: filePath, mode: mode, delete: isDelete}
	if !isDelete {
		ch.content = content
	}
	return s.writeCommitGo(ctx, repoID, branch, req.Ephemeral, tip, req.Message, req.Author, subject(ctx), []fastChange{ch})
}

// cleanPath normalizes and rejects escapes; "" means repo root.
func cleanPath(p string) string {
	p = strings.Trim(path.Clean("/"+p), "/")
	if p == "." || strings.HasPrefix(p, "..") {
		return ""
	}
	return p
}

// validTreePath enforces the constraints git's own verify_path applies at
// index time - the fork write path inherits them from update-index, but
// the Go tree builder writes objects directly and must enforce them
// itself, or a crafted request could plant a ".git/hooks/..." entry or a
// path component git would refuse. Rejects empty components, "." / "..",
// and any ".git" component (case- and HFS/NTFS-variant-insensitive, the
// set git blocks). p is assumed already cleanPath'd (non-empty, no escapes).
func validTreePath(p string) bool {
	if p == "" {
		return false
	}
	for _, comp := range strings.Split(p, "/") {
		if !validTreeComponent(comp) {
			return false
		}
	}
	return true
}

// validTreeComponent rejects a single path component git's index would
// refuse. Control bytes (incl. NUL) would corrupt the tree object - a NUL
// truncates the name in git's "<mode> <name>\x00<oid>" encoding, so any
// parser reads garbage after it; JSON permits \x00, update-index (the fork
// path) rejects it as a C string.
func validTreeComponent(comp string) bool {
	if comp == "" || comp == "." || comp == ".." {
		return false
	}
	for i := 0; i < len(comp); i++ {
		if comp[i] < 0x20 || comp[i] == 0x7f {
			return false
		}
	}
	return !isDotGit(comp)
}

// isDotGit matches the ".git" spellings git's verify_path / checkout guard
// rejects (protect_hfs + protect_ntfs, both default-on): exact (any case),
// NTFS trailing-dot/space and short-name and ADS variants, and HFS
// zero-width-ignorable spellings. Conservative superset - false positives
// only cost a fork fallback, false negatives plant a .git entry.
func isDotGit(c string) bool {
	lower := strings.ToLower(c)
	// NTFS strips trailing dots and spaces, and treats "name::$..." (ADS)
	// and "name:..." as the base name.
	lower = strings.TrimRight(lower, ". ")
	if i := strings.IndexByte(lower, ':'); i >= 0 {
		lower = lower[:i]
	}
	if lower == ".git" || lower == "git~1" || lower == "git~2" {
		return true
	}
	// HFS+ ignores these code points when comparing; strip them and recheck.
	stripped := strings.Map(func(r rune) rune {
		switch r {
		case 0x200c, 0x200d, 0x200e, 0x200f, 0x202a, 0x202b, 0x202c, 0x202d,
			0x202e, 0x206a, 0x206b, 0x206c, 0x206d, 0x206e, 0x206f, 0x2060,
			0x00ad, 0x034f, 0x115f, 0x1160, 0x17b4, 0x17b5, 0x3164, 0xffa0,
			0xfeff:
			return -1
		}
		return r
	}, lower)
	return stripped == ".git"
}

// sanitizeIdent makes name/email safe inside a commit's ident line
// ("author NAME <EMAIL> ts tz"), matching git's strbuf_addstr_without_crud:
// drop NUL / newline / '<' / '>' anywhere (they break the header or the
// object), then trim leading and trailing "crud" (control chars, space,
// and .,:;'"). git's commit-tree does this; the Go builder must not be
// weaker or the same request yields a different commit on the two paths.
func sanitizeIdent(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == 0 || r == '\n' || r == '\r' || r == '<' || r == '>' {
			return -1
		}
		return r
	}, s)
	isCrud := func(b byte) bool {
		switch b {
		case '.', ',', ':', ';', '\'', '"':
			return true
		}
		return b <= ' '
	}
	for len(s) > 0 && isCrud(s[0]) {
		s = s[1:]
	}
	for len(s) > 0 && isCrud(s[len(s)-1]) {
		s = s[:len(s)-1]
	}
	return s
}
