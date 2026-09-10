package githttp

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/folsomintel/forge/internal/blobstore"
	"github.com/folsomintel/forge/internal/repocache"
	"github.com/folsomintel/forge/internal/repodb"
)

// The Go advertisement path parses client-supplied bytes (v2 command
// bodies) and serves from the ref index. These fuzzers assert no panics
// and sane bounds on both the pkt-line splitter and the full v2 command
// dispatcher backed by a real index.

func FuzzParsePktLines(f *testing.F) {
	f.Add([]byte("0014command=ls-refs0001000bpeel000eref-prefix r0000"))
	f.Add([]byte("0000"))
	f.Add([]byte("0001"))
	f.Add([]byte(""))
	f.Add([]byte("ffff"))
	f.Fuzz(func(t *testing.T, data []byte) {
		lines, err := parsePktLines(data)
		if err != nil {
			return
		}
		for _, l := range lines {
			if len(l) > len(data) {
				t.Fatalf("line longer than input: %d > %d", len(l), len(data))
			}
		}
	})
}

func FuzzGoUploadPackCommand(f *testing.F) {
	sq, err := repodb.OpenSQLite(filepath.Join(f.TempDir(), "fuzz.db"))
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { sq.Close() })
	db := repodb.NewWAL(sq, newAdvMemStore())
	ctx := context.Background()
	db.CreateRepo(ctx, "fz", "main")
	db.ApplyWAL(ctx, "fz", []repodb.RefUpdate{
		{Name: "refs/heads/main", Old: repodb.ZeroOID, New: "1111111111111111111111111111111111111111"},
		{Name: "refs/tags/v1", Old: repodb.ZeroOID, New: "2222222222222222222222222222222222222222"},
	}, nil, nil, 1)
	h := &Handler{DB: db, Cache: repocache.New(f.TempDir(), db, nil), GoFetch: true}

	// Seeds: a real ls-refs body, bundle-uri, and junk.
	var lsRefs []byte
	for _, s := range []string{"command=ls-refs\n", "agent=git/2.44\n"} {
		lsRefs = append(lsRefs, []byte(pkt(s))...)
	}
	lsRefs = append(lsRefs, []byte("0001")...)
	for _, s := range []string{"peel\n", "symrefs\n", "unborn\n", "ref-prefix refs/heads/\n"} {
		lsRefs = append(lsRefs, []byte(pkt(s))...)
	}
	lsRefs = append(lsRefs, []byte("0000")...)
	f.Add(lsRefs)
	f.Add([]byte(pkt("command=bundle-uri\n") + "0000"))
	// fetch command shapes: clone, incremental, junk args.
	var fetch []byte
	for _, s := range []string{"command=fetch\n", "agent=git/2.44\n"} {
		fetch = append(fetch, []byte(pkt(s))...)
	}
	fetch = append(fetch, []byte("0001")...)
	for _, s := range []string{"thin-pack\n", "ofs-delta\n",
		"want 1111111111111111111111111111111111111111\n",
		"have 2222222222222222222222222222222222222222\n", "done\n"} {
		fetch = append(fetch, []byte(pkt(s))...)
	}
	fetch = append(fetch, []byte("0000")...)
	f.Add(fetch)
	f.Add([]byte(pkt("command=fetch\n") + "0001" + pkt("want 1111111111111111111111111111111111111111\n") + pkt("done\n") + "0000"))
	f.Add([]byte(pkt("command=fetch\n") + "0000"))
	f.Add([]byte("garbage"))

	f.Fuzz(func(t *testing.T, body []byte) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/fz.git/git-upload-pack", nil)
		handled := h.goUploadPackCommand(rec, req, "fz", "", body)
		if handled && rec.Body.Len() == 0 {
			t.Fatal("handled a command but wrote nothing")
		}
	})
}

func pkt(s string) string {
	return string([]byte{hexDigit(len(s) + 4>>12), hexDigit(len(s) + 4>>8), hexDigit(len(s) + 4>>4), hexDigit(len(s) + 4)}) + s
}

func hexDigit(n int) byte { return "0123456789abcdef"[n&0xf] }

// newAdvMemStore is a minimal in-memory blobstore for the fuzz Handler.
func newAdvMemStore() blobstore.Store { return must(blobstore.NewLocal(fuzzStoreDir())) }

var fuzzStore struct {
	once sync.Once
	dir  string
}

func fuzzStoreDir() string {
	fuzzStore.once.Do(func() { fuzzStore.dir, _ = os.MkdirTemp("", "goadv-fuzz-store-*") })
	return fuzzStore.dir
}

func must(s blobstore.Store, err error) blobstore.Store {
	if err != nil {
		panic(err)
	}
	return s
}
