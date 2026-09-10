package main

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// storm is the push-throughput loadgen: N concurrent writers each land
// tiny real pushes (one commit on a fresh branch) over the smart-HTTP
// wire protocol, built in-process - no git forks on the client, so a
// single machine can drive hundreds of writers. Run it co-located with
// the target (same region) to measure the server ceiling instead of
// your RTT. Samples the server's WAL group-commit telemetry before and
// after so the report shows batch fatness and PUT cost, not just rates.
//
//	forged storm --url https://host --token JWT --writers 64 --pushes 20 --repos 1
func storm(args []string) error {
	fs := flag.NewFlagSet("storm", flag.ExitOnError)
	baseURL := fs.String("url", "", "target instance base URL")
	tok := fs.String("token", "", "bearer token (git:write repo:write org:read)")
	writers := fs.Int("writers", 64, "concurrent writers")
	pushes := fs.Int("pushes", 20, "pushes per writer")
	repos := fs.Int("repos", 1, "spread writers across this many repos")
	direct := fs.Bool("direct", false, "skip the info/refs advertisement round trip")
	keep := fs.Bool("keep", false, "keep storm-* repos afterwards")
	apiReads := fs.Bool("api-reads", false, "REST read storm (contents+commits) instead of pushes")
	fs.Parse(args)
	if *baseURL == "" || *tok == "" {
		return fmt.Errorf("--url and --token are required")
	}
	base := strings.TrimRight(*baseURL, "/")

	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns: *writers * 2, MaxIdleConnsPerHost: *writers * 2,
		IdleConnTimeout: 90 * time.Second,
	}}

	apiJSON := func(method, path string, body string) (map[string]any, error) {
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, base+path, rd)
		req.Header.Set("Authorization", "Bearer "+*tok)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		data, _ := io.ReadAll(res.Body)
		if res.StatusCode >= 300 && res.StatusCode != 409 {
			return nil, fmt.Errorf("%s %s: %d %s", method, path, res.StatusCode, string(data))
		}
		out := map[string]any{}
		json.Unmarshal(data, &out)
		return out, nil
	}

	for i := 0; i < *repos; i++ {
		if _, err := apiJSON("POST", "/api/repos", fmt.Sprintf(`{"id":"storm-%d"}`, i)); err != nil {
			return fmt.Errorf("create storm-%d: %w", i, err)
		}
	}
	if *apiReads {
		return apiReadStorm(client, base, *tok, *writers, *pushes, *repos, *keep, apiJSON)
	}
	walBefore, _ := apiJSON("GET", "/api/usage", "")

	runID := time.Now().UnixNano() % 1_000_000
	total := *writers * *pushes
	lats := make([]time.Duration, total)
	errs := make([]error, *writers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			repo := fmt.Sprintf("storm-%d", w%*repos)
			for i := 0; i < *pushes; i++ {
				branch := fmt.Sprintf("storm-%d-w%d-%d", runID, w, i)
				t0 := time.Now()
				if err := pushOnce(client, base, *tok, repo, branch, *direct); err != nil {
					errs[w] = fmt.Errorf("writer %d push %d: %w", w, i, err)
					return
				}
				lats[w**pushes+i] = time.Since(t0)
			}
		}(w)
	}
	wg.Wait()
	wall := time.Since(start)
	walAfter, _ := apiJSON("GET", "/api/usage", "")

	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
			fmt.Println("ERR", err)
		}
	}
	done := 0
	var ok []time.Duration
	for _, l := range lats {
		if l > 0 {
			ok = append(ok, l)
			done++
		}
	}
	sort.Slice(ok, func(i, j int) bool { return ok[i] < ok[j] })
	pct := func(p float64) time.Duration {
		if len(ok) == 0 {
			return 0
		}
		return ok[min(len(ok)-1, int(float64(len(ok))*p))]
	}
	fmt.Printf("== storm: %d writers x %d pushes across %d repo(s), direct=%v\n",
		*writers, *pushes, *repos, *direct)
	fmt.Printf("pushes      %d ok, %d writers failed\n", done, failed)
	fmt.Printf("wall        %v\n", wall.Round(time.Millisecond))
	fmt.Printf("throughput  %.1f pushes/s\n", float64(done)/wall.Seconds())
	fmt.Printf("latency     p50 %v  p95 %v  p99 %v  max %v\n",
		pct(0.50).Round(time.Millisecond), pct(0.95).Round(time.Millisecond),
		pct(0.99).Round(time.Millisecond), pct(1.0).Round(time.Millisecond))
	printWALDelta(walBefore, walAfter)

	if !*keep {
		for i := 0; i < *repos; i++ {
			apiJSON("DELETE", fmt.Sprintf("/api/repos/storm-%d", i), "")
		}
	}
	return nil
}

// apiReadStorm hammers the REST read surface: each worker seeds one file
// then loops GET contents + GET commits. "pushes" doubles as reads/worker.
func apiReadStorm(client *http.Client, base, tok string, writers, reads, repos int, keep bool, apiJSON func(string, string, string) (map[string]any, error)) error {
	// Seed each repo with one commit so reads have content.
	for i := 0; i < repos; i++ {
		body := `{"message":"seed","content":"c2VlZAo=","branch":"main"}`
		if _, err := apiJSON("PUT", fmt.Sprintf("/api/repos/storm-%d/contents/seed.txt", i), body); err != nil {
			return fmt.Errorf("seed storm-%d: %w", i, err)
		}
	}
	get := func(path string) (int, error) {
		req, _ := http.NewRequest("GET", base+path, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		res, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		return res.StatusCode, nil
	}
	total := writers * reads * 2
	lats := make([]time.Duration, total)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			repo := fmt.Sprintf("storm-%d", w%repos)
			for i := 0; i < reads; i++ {
				for j, path := range []string{
					"/api/repos/" + repo + "/contents",
					"/api/repos/" + repo + "/commits?limit=10",
				} {
					t0 := time.Now()
					code, err := get(path)
					if err != nil || code != 200 {
						errs[w] = fmt.Errorf("worker %d %s: code %d err %v", w, path, code, err)
						return
					}
					lats[(w*reads+i)*2+j] = time.Since(t0)
				}
			}
		}(w)
	}
	wg.Wait()
	wall := time.Since(start)
	failed, done := 0, 0
	var ok []time.Duration
	for _, err := range errs {
		if err != nil {
			failed++
			fmt.Println("ERR", err)
		}
	}
	for _, l := range lats {
		if l > 0 {
			ok = append(ok, l)
			done++
		}
	}
	sort.Slice(ok, func(i, j int) bool { return ok[i] < ok[j] })
	pct := func(p float64) time.Duration {
		if len(ok) == 0 {
			return 0
		}
		return ok[min(len(ok)-1, int(float64(len(ok))*p))]
	}
	fmt.Printf("== api-read storm: %d workers x %d loops x 2 reads across %d repo(s)\n", writers, reads, repos)
	fmt.Printf("pushes      %d ok, %d writers failed\n", done, failed)
	fmt.Printf("wall        %v\n", wall.Round(time.Millisecond))
	fmt.Printf("throughput  %.1f pushes/s\n", float64(done)/wall.Seconds())
	fmt.Printf("latency     p50 %v  p95 %v  p99 %v  max %v\n",
		pct(0.50).Round(time.Millisecond), pct(0.95).Round(time.Millisecond),
		pct(0.99).Round(time.Millisecond), pct(1.0).Round(time.Millisecond))
	fmt.Printf("wal         (api reads)\n")
	if !keep {
		for i := 0; i < repos; i++ {
			apiJSON("DELETE", fmt.Sprintf("/api/repos/storm-%d", i), "")
		}
	}
	return nil
}

func printWALDelta(before, after map[string]any) {
	getw := func(m map[string]any, k string) float64 {
		w, _ := m["wal"].(map[string]any)
		v, _ := w[k].(float64)
		return v
	}
	puts := getw(after, "puts") - getw(before, "puts")
	txs := getw(after, "txs") - getw(before, "txs")
	ms := getw(after, "put_ms_total") - getw(before, "put_ms_total")
	if puts <= 0 {
		fmt.Println("wal         no telemetry delta (old server image?)")
		return
	}
	fmt.Printf("wal         %.0f txs in %.0f PUTs (avg batch %.1f, max %.0f) avg PUT %.0fms max %.0fms cas-retries %.0f\n",
		txs, puts, txs/puts, getw(after, "max_batch"), ms/puts,
		getw(after, "put_ms_max"), getw(after, "cas_retries")-getw(before, "cas_retries"))
}

// pushOnce lands one commit on a new branch via smart HTTP: optional
// info/refs advertisement (like real git), then git-receive-pack with a
// two-object pack (empty tree + commit). report-status without
// side-band keeps the response trivially parseable.
func pushOnce(client *http.Client, base, tok, repo, branch string, direct bool) error {
	if !direct {
		req, _ := http.NewRequest("GET", base+"/"+repo+".git/info/refs?service=git-receive-pack", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("info/refs: %d", res.StatusCode)
		}
	}

	commit := fmt.Sprintf(
		"tree %s\nauthor storm <s@forge> %d +0000\ncommitter storm <s@forge> %d +0000\n\n%s\n",
		emptyTreeOID, time.Now().Unix(), time.Now().Unix(), branch)
	oid := objectOID("commit", []byte(commit))
	pack := buildPack([]packObj{{objTree, nil}, {objCommit, []byte(commit)}})

	var body bytes.Buffer
	cmd := fmt.Sprintf("%s %s refs/heads/%s\x00report-status agent=forge-storm/1",
		strings.Repeat("0", 40), oid, branch)
	fmt.Fprintf(&body, "%04x%s", len(cmd)+4, cmd)
	body.WriteString("0000")
	body.Write(pack)

	req, _ := http.NewRequest("POST", base+"/"+repo+".git/git-receive-pack", bytes.NewReader(body.Bytes()))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		return fmt.Errorf("receive-pack: %d %s", res.StatusCode, string(out))
	}
	if !bytes.Contains(out, []byte("unpack ok")) || bytes.Contains(out, []byte("ng ")) {
		return fmt.Errorf("push rejected: %q", string(out))
	}
	return nil
}

const emptyTreeOID = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

const (
	objCommit = 1
	objTree   = 2
)

type packObj struct {
	typ     int
	payload []byte
}

func objectOID(typ string, payload []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", typ, len(payload))
	h.Write(payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// buildPack encodes a v2 packfile: header, per-object varint type/size
// header + zlib-deflated payload, SHA-1 trailer.
func buildPack(objs []packObj) []byte {
	var b bytes.Buffer
	b.WriteString("PACK")
	binary.Write(&b, binary.BigEndian, uint32(2))
	binary.Write(&b, binary.BigEndian, uint32(len(objs)))
	for _, o := range objs {
		size := len(o.payload)
		hdr := []byte{byte(o.typ<<4) | byte(size&0x0F)}
		size >>= 4
		for size > 0 {
			hdr[len(hdr)-1] |= 0x80
			hdr = append(hdr, byte(size&0x7F))
			size >>= 7
		}
		b.Write(hdr)
		zw := zlib.NewWriter(&b)
		zw.Write(o.payload)
		zw.Close()
	}
	sum := sha1.Sum(b.Bytes())
	b.Write(sum[:])
	return b.Bytes()
}
