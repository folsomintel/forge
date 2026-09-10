package maintain

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/folsomintel/forge/internal/repocache"
)

// urlReachable fetches a single byte through the URL - the BYOB private-
// endpoint safety net (a presigned GET can't be HEADed: the verb is signed,
// but Range is not).
func urlReachable(ctx context.Context, url string) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusPartialContent || resp.StatusCode == http.StatusOK
}

// Bundle-uri clone offload (the GitLab/Google pattern): maintenance
// publishes a full-clone bundle to the store; the repo advertises a signed
// URL for it (uploadpack.advertiseBundleURIs). Opted-in clients
// (transfer.bundleURI=true) bootstrap the clone from the bundle and only
// negotiate the delta against the server — cold big-repo clones stop
// costing compute. Everything degrades gracefully: no bundle, no
// advertisement; stale bundle, origin tops up.

const (
	bundleTTL = 48 * time.Hour
	// localBundleManifest is the cache-local copy of meta/bundles/list.json:
	// existence proof (advertise only when it's present) and the source for
	// the advertised URI list, so advertise never round-trips the store on
	// the hot materialize path once it's placed.
	localBundleManifest = ".bundles.json"
)

// BundleSig signs a bundle-download capability URL. name is the bundle file
// name ("<token>.bundle") so a signature is scoped to one blob, not the repo.
func (p *Pipeline) BundleSig(repoID, name string, exp int64) string {
	mac := hmac.New(sha256.New, p.BundleSecret)
	fmt.Fprintf(mac, "%s:%s:%d", repoID, name, exp)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyBundleSig validates a capability URL's signature and expiry.
func (p *Pipeline) VerifyBundleSig(repoID, name string, exp int64, sig string) bool {
	if len(p.BundleSecret) == 0 || exp < time.Now().Unix() {
		return false
	}
	want := p.BundleSig(repoID, name, exp)
	return hmac.Equal([]byte(want), []byte(sig))
}

// advertise writes/refreshes the repo's bundle-chain advertisement config
// (included from the cache repo's config file). Emits every chain member in
// creationToken order with mode=all, so git bootstraps from the full and
// only pulls newer incrementals on later operations.
func (p *Pipeline) advertise(ctx context.Context, repoID, dir string) {
	conf := filepath.Join(dir, "bundles.conf")
	if p.PublicURL == "" || len(p.BundleSecret) == 0 {
		os.Remove(conf)
		return
	}
	// Only advertise when the manifest exists; fetch it once per cache
	// lifetime, then trust the local copy. Absence is negative-cached too: a
	// missing-object GET costs ~200ms on Tigris (global negative lookup) and
	// this runs on every materialize.
	manPath := filepath.Join(dir, localBundleManifest)
	if _, err := os.Stat(manPath); err != nil {
		neg := filepath.Join(dir, ".bundle-checked")
		if info, nerr := os.Stat(neg); nerr == nil && time.Since(info.ModTime()) < 10*time.Minute {
			return // recently confirmed absent
		}
		if err := p.Cache.FetchBlob(ctx, repoID, bundleManifest, manPath); err != nil {
			os.WriteFile(neg, nil, 0o644)
			return // no bundles published yet
		}
	}
	// Refresh when younger than half the signature TTL remains.
	if info, err := os.Stat(conf); err == nil && time.Since(info.ModTime()) < bundleTTL/2 {
		return
	}
	data, err := os.ReadFile(manPath)
	if err != nil {
		return
	}
	var list bundleList
	if json.Unmarshal(data, &list); len(list.Entries) == 0 {
		os.Remove(conf)
		return
	}

	// Prefer presigned bucket URLs (clone bytes served globally, zero machine
	// bandwidth). BYOB caveat: the bucket may not be publicly reachable, so
	// probe one byte through the newest entry's URL; if it fails, serve every
	// member through our signed streaming endpoint instead.
	exp := time.Now().Add(bundleTTL).Unix()
	usePresigned := false
	if newest := list.last(); newest != nil {
		if u, perr := p.Blobs.Presign(ctx, repoID, bundlePrefix+newest.Name, bundleTTL); perr == nil && urlReachable(ctx, u) {
			usePresigned = true
		}
	}

	var b strings.Builder
	b.WriteString("[uploadpack]\n\tadvertiseBundleURIs = true\n")
	b.WriteString("[bundle]\n\tversion = 1\n\tmode = all\n")
	for _, e := range list.Entries {
		uri := fmt.Sprintf("%s/bundles/%s?name=%s&exp=%d&sig=%s",
			p.PublicURL, repoID, e.Name, exp, p.BundleSig(repoID, e.Name, exp))
		if usePresigned {
			if u, perr := p.Blobs.Presign(ctx, repoID, bundlePrefix+e.Name, bundleTTL); perr == nil {
				uri = u
			}
		}
		fmt.Fprintf(&b, "[bundle \"b%d\"]\n\turi = %s\n\tcreationToken = %d\n", e.Token, uri, e.Token)
	}
	if err := repocache.WriteFileAtomic(conf, []byte(b.String())); err != nil {
		slog.Warn("write bundles.conf", "repo", repoID, "err", err)
	}
}
