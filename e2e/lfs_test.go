package e2e

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Exercises the LFS batch + basic transfer protocol over raw HTTP, so the
// suite doesn't depend on a git-lfs client being installed.
func TestLFSBatchUploadDownload(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("demo")

	content := []byte("big binary blob pretend\n")
	sum := sha256.Sum256(content)
	oid := hex.EncodeToString(sum[:])

	// 1. Batch upload: server hands back an upload action.
	batch := e.lfsBatch(t, "upload", oid, len(content))
	obj := batch["objects"].([]any)[0].(map[string]any)
	actions := obj["actions"].(map[string]any)
	upload := actions["upload"].(map[string]any)

	// 2. PUT the bytes to the action href with the echoed auth header.
	e.lfsTransfer(t, "PUT", upload, bytes.NewReader(content), http.StatusOK)

	// Corrupt uploads are rejected and not stored.
	badBatch := e.lfsBatch(t, "upload", strings.Repeat("b", 64), 3)
	badObj := badBatch["objects"].([]any)[0].(map[string]any)
	badUpload := badObj["actions"].(map[string]any)["upload"].(map[string]any)
	e.lfsTransfer(t, "PUT", badUpload, strings.NewReader("xyz"), http.StatusBadRequest)

	// 3. Re-batching the uploaded object returns no actions (already there).
	again := e.lfsBatch(t, "upload", oid, len(content))
	if _, has := again["objects"].([]any)[0].(map[string]any)["actions"]; has {
		t.Fatalf("re-upload offered for existing object: %v", again)
	}

	// 4. Batch download → GET → bytes round-trip.
	down := e.lfsBatch(t, "download", oid, len(content))
	dl := down["objects"].([]any)[0].(map[string]any)["actions"].(map[string]any)["download"].(map[string]any)
	body := e.lfsTransferRead(t, dl)
	if !bytes.Equal(body, content) {
		t.Fatalf("lfs roundtrip: %q", body)
	}

	// Unknown object download → per-object 404 error.
	missing := e.lfsBatch(t, "download", strings.Repeat("c", 64), 1)
	mObj := missing["objects"].([]any)[0].(map[string]any)
	if mObj["error"] == nil {
		t.Fatalf("expected per-object error: %v", missing)
	}
}

func (e *env) lfsBatch(t *testing.T, op, oid string, size int) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"operation": op,
		"objects":   []map[string]any{{"oid": oid, "size": size}},
	})
	req, _ := http.NewRequest("POST", e.base+"/demo.git/info/lfs/objects/batch", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("batch %s: status %d", op, resp.StatusCode)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func (e *env) lfsTransfer(t *testing.T, method string, action map[string]any, body io.Reader, wantStatus int) {
	t.Helper()
	req, _ := http.NewRequest(method, action["href"].(string), body)
	for k, v := range action["header"].(map[string]any) {
		req.Header.Set(k, v.(string))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s %s: status %d (want %d): %s", method, action["href"], resp.StatusCode, wantStatus, raw)
	}
}

func (e *env) lfsTransferRead(t *testing.T, action map[string]any) []byte {
	t.Helper()
	req, _ := http.NewRequest("GET", action["href"].(string), nil)
	for k, v := range action["header"].(map[string]any) {
		req.Header.Set(k, v.(string))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	return body
}

func TestLFSRequiresAuth(t *testing.T) {
	t.Parallel()
	e := startServer(t)
	e.createRepo("demo")
	body := `{"operation":"download","objects":[{"oid":"` + strings.Repeat("a", 64) + `","size":1}]}`
	resp, err := http.Post(fmt.Sprintf("%s/demo.git/info/lfs/objects/batch", e.base), "application/vnd.git-lfs+json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated batch: %d", resp.StatusCode)
	}
}
