package maintain

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/folsomintel/forge/internal/gitcmd"
)

// History pack (walgit Phase 3): a blobless pack of just commits + trees,
// published so a remote-placement replica can keep repo history LOCAL while
// blob bytes stream from the bucket on demand. refs/log/tree/web-UI reads then
// never touch the network; only opening a file's contents does.
//
// Built on the primary during maintenance (guarded by BuildHistory), stored as
// meta/history.{pack,idx}. The replica fetches it in syncPacks and serves
// commits/trees from it before falling through to the remote gc pack.

const (
	historyPackBlob = "meta/history.pack"
	historyIdxBlob  = "meta/history.idx"
)

type historyPack struct{}

func (historyPack) Kind() string { return "history" }

func (historyPack) Derive(ctx context.Context, p *Pipeline, st *RepoState) error {
	if !p.BuildHistory || st.GCPack == "" {
		return nil
	}
	x := &gitcmd.Exec{Ctx: ctx, Dir: st.Dir}
	// Positive revs = every ref tip; --filter=blob:none omits blobs, so the
	// pack is commits + trees only.
	var revs strings.Builder
	for _, r := range st.Refs {
		fmt.Fprintln(&revs, r.Target)
	}
	packBytes, err := x.RunIn(strings.NewReader(revs.String()),
		"pack-objects", "--revs", "--filter=blob:none", "--stdout")
	if err != nil {
		return fmt.Errorf("pack-objects (blobless): %w", err)
	}
	// Write the pack, then index it to produce the matching .idx.
	tmp := filepath.Join(st.Dir, "history-derive.pack")
	if err := os.WriteFile(tmp, []byte(packBytes), 0o644); err != nil {
		return err
	}
	defer os.Remove(tmp)
	defer os.Remove(strings.TrimSuffix(tmp, ".pack") + ".idx")
	if _, err := x.Run("index-pack", tmp); err != nil {
		return fmt.Errorf("index-pack (history): %w", err)
	}
	idxPath := strings.TrimSuffix(tmp, ".pack") + ".idx"

	pf, err := os.Open(tmp)
	if err != nil {
		return err
	}
	err = p.Blobs.Put(ctx, st.RepoID, historyPackBlob, pf)
	pf.Close()
	if err != nil {
		return err
	}
	xf, err := os.Open(idxPath)
	if err != nil {
		return err
	}
	err = p.Blobs.Put(ctx, st.RepoID, historyIdxBlob, xf)
	xf.Close()
	return err
}

func (historyPack) Hydrate(ctx context.Context, p *Pipeline, repoID, dir string) {
	// Fetched by the replica's syncPacks (remote mode) into meta-history/, not
	// here - a full-pack instance doesn't need it.
}
