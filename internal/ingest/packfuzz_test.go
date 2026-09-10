package ingest

import (
	"bytes"
	"testing"
)

// The pack parser is the fast path's attack surface: any git:write token
// holder feeds it arbitrary bytes over HTTP. These fuzzers assert it never
// panics, never over-allocates past its caps, and that everything it
// accepts satisfies the invariants the rest of the path relies on.

func FuzzReadPackThin(f *testing.F) {
	// Seed with structurally valid packs so coverage-guided mutation
	// starts from real shapes, not noise.
	f.Add(buildTestPack(f, []testObj{{pkBlob, []byte("hello\n")}}))
	f.Add(buildTestPack(f, []testObj{
		{pkBlob, []byte("hello\n")},
		{pkCommit, []byte("tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n\nempty\n")},
	}))
	base := []byte("hello\n")
	f.Add(buildDeltaPack(f, []deltaObj{
		{typ: pkBlob, data: base},
		{typ: pkOfsDelta, data: deltaFor(base, []byte("hello world\n")), ofsBase: 0},
	}))
	f.Add(buildDeltaPack(f, []deltaObj{
		{typ: pkRefDelta, data: deltaFor(base, []byte("hello thin\n")), refBase: oidFor("blob", base)},
	}))
	f.Add([]byte("PACK"))
	f.Add([]byte{})

	fakeBase := func(oid string) (string, []byte, error) {
		return "blob", []byte("external base content\n"), nil
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		// Without bases: must not panic; on success, objects are coherent.
		if p, stored, err := ReadPackThin(data, nil); err == nil {
			if !bytes.Equal(stored, data) {
				t.Fatal("no-external parse must return the wire bytes unchanged")
			}
			checkPackInvariants(t, p)
		}
		// With an always-answering BaseFunc: on success the returned bytes
		// must be SELF-CONTAINED - they re-parse without any base source
		// to the same trailer and object count.
		p, stored, err := ReadPackThin(data, fakeBase)
		if err != nil {
			return
		}
		checkPackInvariants(t, p)
		p2, stored2, err := ReadPackThin(stored, nil)
		if err != nil {
			t.Fatalf("accepted pack is not self-contained: %v", err)
		}
		if p2.Trailer != p.Trailer {
			t.Fatal("trailer changed on re-parse")
		}
		if len(p2.Objects) != len(p.Objects) {
			t.Fatalf("object count changed on re-parse: %d != %d", len(p2.Objects), len(p.Objects))
		}
		if !bytes.Equal(stored2, stored) {
			t.Fatal("re-parse rebuilt an already-fixed pack")
		}
		// Anything the parser accepts must also be idx-representable or
		// rejected cleanly - never a panic downstream.
		_, _ = WriteIdxV2(p)
	})
}

func checkPackInvariants(t *testing.T, p *Pack) {
	t.Helper()
	total := 0
	for _, o := range p.Objects {
		if o.Type == "" || len(o.OID) != 40 {
			t.Fatalf("accepted object with bad type/oid: %q %q", o.Type, o.OID)
		}
		if len(o.Data) > maxObjectBytes {
			t.Fatalf("object exceeds per-object cap: %d", len(o.Data))
		}
		if got := oidFor(o.Type, o.Data); got != o.OID {
			t.Fatalf("OID does not match content: %s != %s", o.OID, got)
		}
		total += len(o.Data)
	}
	if total > maxResolvedBytes+maxObjectBytes {
		t.Fatalf("aggregate expansion above cap: %d", total)
	}
}

func FuzzApplyDelta(f *testing.F) {
	base := []byte("the quick brown fox jumps over the lazy dog\n")
	f.Add(base, deltaFor(base, []byte("the quick brown fox naps\n")))
	f.Add([]byte("hello\n"), deltaFor([]byte("hello\n"), []byte("hello world\n")))
	f.Add([]byte{}, []byte{})
	f.Fuzz(func(t *testing.T, baseData, delta []byte) {
		if len(baseData) > 1<<20 {
			baseData = baseData[:1<<20]
		}
		out, err := applyDelta(baseData, delta)
		if err != nil {
			return
		}
		if len(out) > maxObjectBytes {
			t.Fatalf("delta output exceeds cap: %d", len(out))
		}
	})
}
