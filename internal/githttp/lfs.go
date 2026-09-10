package githttp

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/folsomintel/forge/internal/auth"
	"github.com/folsomintel/forge/internal/repodb"
)

// Git LFS: the batch API plus "basic" transfer endpoints. Objects live in
// the pack store under lfs/<oid>. With S3 this can later hand out presigned
// URLs; routing bytes through the server keeps one code path for both
// stores today.

const lfsMediaType = "application/vnd.git-lfs+json"

var oidRe = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (h *Handler) RegisterLFS(mux *http.ServeMux) {
	mux.HandleFunc("POST /{repo}/info/lfs/objects/batch", h.lfsBatch)
	mux.HandleFunc("GET /{repo}/lfs/objects/{oid}", h.lfsDownload)
	mux.HandleFunc("PUT /{repo}/lfs/objects/{oid}", h.lfsUpload)
}

type lfsObject struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

type lfsAction struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header,omitempty"`
}

type lfsBatchObject struct {
	lfsObject
	Authenticated bool                  `json:"authenticated"`
	Actions       map[string]*lfsAction `json:"actions,omitempty"`
	Error         *lfsError             `json:"error,omitempty"`
}

type lfsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	}
	return scheme + "://" + r.Host
}

func (h *Handler) lfsBatch(w http.ResponseWriter, r *http.Request) {
	repo, _ := parseTarget(r)
	var req struct {
		Operation string      `json:"operation"`
		Objects   []lfsObject `json:"objects"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid batch request", http.StatusBadRequest)
		return
	}
	scope := auth.ScopeGitRead
	if req.Operation == "upload" {
		scope = auth.ScopeGitWrite
	}
	claims, err := h.Auth.FromRequest(r)
	if err != nil || !claims.Allow(scope, repo) {
		w.Header().Set("WWW-Authenticate", `Basic realm="forge"`)
		http.Error(w, `{"message":"authentication required"}`, http.StatusUnauthorized)
		return
	}
	if !repodb.ValidRepoID(repo) {
		http.Error(w, `{"message":"repository not found"}`, http.StatusNotFound)
		return
	}
	if _, err := h.DB.GetRepo(r.Context(), repo); err != nil {
		http.Error(w, `{"message":"repository not found"}`, http.StatusNotFound)
		return
	}

	// The client re-presents its Authorization header on transfer requests
	// when we echo it in action headers.
	authHeader := r.Header.Get("Authorization")
	out := make([]lfsBatchObject, 0, len(req.Objects))
	for _, obj := range req.Objects {
		b := lfsBatchObject{lfsObject: obj, Authenticated: true}
		if !oidRe.MatchString(obj.OID) {
			b.Error = &lfsError{Code: 422, Message: "invalid oid"}
			out = append(out, b)
			continue
		}
		href := fmt.Sprintf("%s/%s.git/lfs/objects/%s", baseURL(r), repo, obj.OID)
		existing, _ := h.DB.GetLFSObject(r.Context(), repo, obj.OID)
		switch req.Operation {
		case "download":
			if existing == nil {
				b.Error = &lfsError{Code: 404, Message: "object not found"}
			} else {
				b.Actions = map[string]*lfsAction{"download": {Href: href, Header: map[string]string{"Authorization": authHeader}}}
			}
		case "upload":
			if existing == nil {
				b.Actions = map[string]*lfsAction{"upload": {Href: href, Header: map[string]string{"Authorization": authHeader}}}
			} // exists → no actions → client skips upload
		default:
			b.Error = &lfsError{Code: 422, Message: "unknown operation"}
		}
		out = append(out, b)
	}
	w.Header().Set("Content-Type", lfsMediaType)
	json.NewEncoder(w).Encode(map[string]any{"transfer": "basic", "objects": out})
}

// lfsMaxUploadBytes is a safety backstop against an unbounded upload
// filling the volume; normal LFS objects are far under it.
const lfsMaxUploadBytes = 5 << 30 // 5 GiB

func (h *Handler) lfsAuthTransfer(w http.ResponseWriter, r *http.Request, scope string) (repo, oid string, ok bool) {
	repo, _ = parseTarget(r)
	oid = r.PathValue("oid")
	// Validate the repo id (rejects "..", slashes, "api") BEFORE it is used
	// as a store prefix - a path-traversal guard - and require it to be a
	// real repo.
	if !repodb.ValidRepoID(repo) {
		http.Error(w, "repository not found", http.StatusNotFound)
		return "", "", false
	}
	claims, err := h.Auth.FromRequest(r)
	if err != nil || !claims.Allow(scope, repo) {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return "", "", false
	}
	if !oidRe.MatchString(oid) {
		http.Error(w, "invalid oid", http.StatusBadRequest)
		return "", "", false
	}
	if _, err := h.DB.GetRepo(r.Context(), repo); err != nil {
		http.Error(w, "repository not found", http.StatusNotFound)
		return "", "", false
	}
	return repo, oid, true
}

func (h *Handler) lfsDownload(w http.ResponseWriter, r *http.Request) {
	repo, oid, ok := h.lfsAuthTransfer(w, r, auth.ScopeGitRead)
	if !ok {
		return
	}
	obj, err := h.DB.GetLFSObject(r.Context(), repo, oid)
	if err != nil {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}
	rc, err := h.Blobs.Get(r.Context(), repo, "lfs/"+oid)
	if err != nil {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	_, _ = io.Copy(w, rc)
}

// lfsUpload streams the (size-bounded) body into a private temp key while
// hashing, then promotes it to lfs/<oid> only on a digest match. Writing to
// a temp key first means a bad or duplicate upload can never delete a
// concurrent valid lfs/<oid>.
func (h *Handler) lfsUpload(w http.ResponseWriter, r *http.Request) {
	repo, oid, ok := h.lfsAuthTransfer(w, r, auth.ScopeGitWrite)
	if !ok {
		return
	}
	var rnd [8]byte
	rand.Read(rnd[:])
	tmpKey := "staged/lfs-" + oid + "-" + hex.EncodeToString(rnd[:]) // swept as orphan on crash

	hasher := sha256.New()
	body := http.MaxBytesReader(w, r.Body, lfsMaxUploadBytes)
	counter := &countReader{r: io.TeeReader(body, hasher)}
	if err := h.Blobs.Put(r.Context(), repo, tmpKey, counter); err != nil {
		_ = h.Blobs.Delete(r.Context(), repo, tmpKey)
		slog.Error("lfs upload", "repo", repo, "oid", oid, "err", err)
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	if hex.EncodeToString(hasher.Sum(nil)) != oid {
		_ = h.Blobs.Delete(r.Context(), repo, tmpKey)
		http.Error(w, "content does not match oid", http.StatusBadRequest)
		return
	}
	if err := h.Blobs.Copy(r.Context(), repo, tmpKey, "lfs/"+oid); err != nil {
		_ = h.Blobs.Delete(r.Context(), repo, tmpKey)
		slog.Error("lfs promote", "repo", repo, "oid", oid, "err", err)
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	h.Blobs.Delete(r.Context(), repo, tmpKey) // best-effort temp cleanup
	if err := h.DB.AddLFSObject(r.Context(), repo, repodb.LFSObject{OID: oid, Size: counter.n}); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
