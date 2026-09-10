package repodb

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// DeleteRepo must purge the whole bucket prefix (packs included) and every
// repo-scoped table - except pack blobs a live fork still references.
func TestDeleteRepoPurgesBlobsAndTables(t *testing.T) {
	w, store := newTestWAL(t, 0)
	ctx := context.Background()

	// A pack blob + row, a WAL ref, and a webhook - one of each kind of state.
	if err := store.Put(ctx, "r", "pack-aaaa.pack", bytes.NewReader([]byte("packdata"))); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "r", "pack-aaaa.idx", bytes.NewReader([]byte("idxdata"))); err != nil {
		t.Fatal(err)
	}
	if err := w.AddPacks(ctx, "r", []Pack{{Name: "pack-aaaa", SizeBytes: 8, Source: "test"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateRefs(ctx, "r", []RefUpdate{{Name: "refs/heads/main", Old: ZeroOID, New: testOID}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.CreateWebhook(ctx, &Webhook{ID: "wh1", RepoID: "r", URL: "https://x", Events: []string{"push"}, Active: true}); err != nil {
		t.Fatal(err)
	}

	if err := w.DeleteRepo(ctx, "r"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Bucket prefix fully cleared.
	blobs, err := store.List(ctx, "r", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 0 {
		t.Fatalf("blobs left after delete: %v", blobs)
	}
	// Tables cleared.
	if refs, _ := w.ListRefs(ctx, "r"); len(refs) != 0 {
		t.Fatalf("refs left: %v", refs)
	}
	if packs, _ := w.ListPacks(ctx, "r"); len(packs) != 0 {
		t.Fatalf("packs left: %v", packs)
	}
	if hooks, _ := w.ListWebhooks(ctx, "r"); len(hooks) != 0 {
		t.Fatalf("webhooks left: %v", hooks)
	}
}

// A parent's pack blob is kept when a zero-copy fork still references it.
func TestDeleteRepoKeepsForkReferencedPacks(t *testing.T) {
	w, store := newTestWAL(t, 0)
	ctx := context.Background()

	store.Put(ctx, "r", "pack-bbbb.pack", bytes.NewReader([]byte("shared")))
	store.Put(ctx, "r", "pack-bbbb.idx", bytes.NewReader([]byte("sharedidx")))
	if err := w.AddPacks(ctx, "r", []Pack{{Name: "pack-bbbb", SizeBytes: 6, Source: "test"}}); err != nil {
		t.Fatal(err)
	}
	// Fork "r" -> "f": f's pack row points blob_repo back at r.
	if err := w.ForkRepo(ctx, "r", "f"); err != nil {
		t.Fatal(err)
	}

	if err := w.DeleteRepo(ctx, "r"); err != nil {
		t.Fatalf("delete parent: %v", err)
	}

	// The shared pack blob must survive under r's prefix for the fork to read.
	blobs, err := store.List(ctx, "r", "")
	if err != nil {
		t.Fatal(err)
	}
	kept := false
	for _, b := range blobs {
		if strings.HasPrefix(b.Name, "pack-bbbb") {
			kept = true
		}
	}
	if !kept {
		t.Fatalf("fork-referenced pack was deleted with the parent: %v", blobs)
	}
}
