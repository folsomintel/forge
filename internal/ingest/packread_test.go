package ingest

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type testObj struct {
	typ  int
	data []byte
}

// buildTestPack encodes a minimal v2 packfile of undeltified objects, the
// shape a client sends for a fresh agent commit. Mirrors the pack writer
// in cmd/forged/storm.go.
func buildTestPack(t testing.TB, objs []testObj) []byte {
	t.Helper()
	var b bytes.Buffer
	b.WriteString("PACK")
	binary.Write(&b, binary.BigEndian, uint32(2))
	binary.Write(&b, binary.BigEndian, uint32(len(objs)))
	for _, o := range objs {
		size := len(o.data)
		hdr := []byte{byte(o.typ<<4) | byte(size&0x0f)}
		size >>= 4
		for size > 0 {
			hdr[len(hdr)-1] |= 0x80
			hdr = append(hdr, byte(size&0x7f))
			size >>= 7
		}
		b.Write(hdr)
		zw := zlib.NewWriter(&b)
		zw.Write(o.data)
		zw.Close()
	}
	sum := sha1.Sum(b.Bytes())
	b.Write(sum[:])
	return b.Bytes()
}

func TestReadPackVerifiesOIDsAgainstGit(t *testing.T) {
	// "hello\n" as a git blob hashes to this exact OID under git's
	// canonical "blob <len>\x00<data>" rule.
	const helloBlobOID = "ce013625030ba8dba906f756967f9e9ca394464a"

	pack := buildTestPack(t, []testObj{
		{pkBlob, []byte("hello\n")},
		{pkBlob, []byte("a second blob\n")},
	})
	p, err := ReadPack(bytes.NewReader(pack))
	if err != nil {
		t.Fatalf("ReadPack: %v", err)
	}
	if len(p.Objects) != 2 {
		t.Fatalf("got %d objects, want 2", len(p.Objects))
	}
	if p.Objects[0].OID != helloBlobOID {
		t.Fatalf("blob OID = %s, want git's %s", p.Objects[0].OID, helloBlobOID)
	}
	if p.Objects[0].Type != "blob" || string(p.Objects[0].Data) != "hello\n" {
		t.Fatalf("object 0 = %q %q", p.Objects[0].Type, p.Objects[0].Data)
	}
}

func TestReadPackRejectsCorruptTrailer(t *testing.T) {
	pack := buildTestPack(t, []testObj{{pkBlob, []byte("hello\n")}})
	pack[len(pack)-1] ^= 0xff
	if _, err := ReadPack(bytes.NewReader(pack)); err == nil {
		t.Fatal("expected checksum mismatch, got nil")
	}
}

// deltaFor builds a git delta stream that turns base into result: one copy
// of the shared prefix (when any) plus one literal insert of the rest.
func deltaFor(base, result []byte) []byte {
	writeSize := func(b *bytes.Buffer, n int) {
		for {
			c := byte(n & 0x7f)
			n >>= 7
			if n > 0 {
				c |= 0x80
			}
			b.WriteByte(c)
			if n == 0 {
				return
			}
		}
	}
	prefix := 0
	for prefix < len(base) && prefix < len(result) && base[prefix] == result[prefix] {
		prefix++
	}
	var d bytes.Buffer
	writeSize(&d, len(base))
	writeSize(&d, len(result))
	if prefix > 0 {
		d.WriteByte(0x80 | 0x10) // copy: offset 0, one size byte
		d.WriteByte(byte(prefix))
	}
	rest := result[prefix:]
	for len(rest) > 0 {
		n := len(rest)
		if n > 127 {
			n = 127
		}
		d.WriteByte(byte(n))
		d.Write(rest[:n])
		rest = rest[n:]
	}
	return d.Bytes()
}

// buildDeltaPack writes a v2 pack of full objects and REF/OFS deltas,
// returning the wire bytes. A refBase makes the entry REF_DELTA; ofsBase
// (an index into objs) makes it OFS_DELTA against that earlier entry.
type deltaObj struct {
	typ     int
	data    []byte // content, or delta stream for delta entries
	refBase string // REF_DELTA base OID (hex)
	ofsBase int    // OFS_DELTA: index of the base entry (typ must be pkOfsDelta)
}

func buildDeltaPack(t testing.TB, objs []deltaObj) []byte {
	t.Helper()
	var b bytes.Buffer
	b.WriteString("PACK")
	binary.Write(&b, binary.BigEndian, uint32(2))
	binary.Write(&b, binary.BigEndian, uint32(len(objs)))
	offsets := make([]int64, len(objs))
	for i, o := range objs {
		offsets[i] = int64(b.Len())
		size := len(o.data)
		hdr := []byte{byte(o.typ<<4) | byte(size&0x0f)}
		size >>= 4
		for size > 0 {
			hdr[len(hdr)-1] |= 0x80
			hdr = append(hdr, byte(size&0x7f))
			size >>= 7
		}
		b.Write(hdr)
		switch o.typ {
		case pkRefDelta:
			raw, err := hex.DecodeString(o.refBase)
			if err != nil || len(raw) != 20 {
				t.Fatalf("bad refBase %q", o.refBase)
			}
			b.Write(raw)
		case pkOfsDelta:
			rel := offsets[i] - offsets[o.ofsBase]
			// git offset encoding, most-significant group first.
			var enc []byte
			enc = append(enc, byte(rel&0x7f))
			rel >>= 7
			for rel > 0 {
				rel--
				enc = append(enc, byte(rel&0x7f)|0x80)
				rel >>= 7
			}
			for j := len(enc) - 1; j >= 0; j-- {
				b.WriteByte(enc[j])
			}
		}
		zw := zlib.NewWriter(&b)
		zw.Write(o.data)
		zw.Close()
	}
	sum := sha1.Sum(b.Bytes())
	b.Write(sum[:])
	return b.Bytes()
}

func TestReadPackResolvesOfsDelta(t *testing.T) {
	base := []byte("hello\n")
	result := []byte("hello world\n")
	pack := buildDeltaPack(t, []deltaObj{
		{typ: pkBlob, data: base},
		{typ: pkOfsDelta, data: deltaFor(base, result), ofsBase: 0},
	})
	p, err := ReadPack(bytes.NewReader(pack))
	if err != nil {
		t.Fatalf("ReadPack: %v", err)
	}
	if len(p.Objects) != 2 {
		t.Fatalf("got %d objects, want 2", len(p.Objects))
	}
	if p.Objects[1].Type != "blob" || string(p.Objects[1].Data) != string(result) {
		t.Fatalf("delta resolved to %q %q", p.Objects[1].Type, p.Objects[1].Data)
	}
	if want := oidFor("blob", result); p.Objects[1].OID != want {
		t.Fatalf("delta OID = %s, want %s", p.Objects[1].OID, want)
	}
}

func TestThinPackFallsBackWithoutBases(t *testing.T) {
	base := []byte("hello\n")
	pack := buildDeltaPack(t, []deltaObj{
		{typ: pkRefDelta, data: deltaFor(base, []byte("hello world\n")), refBase: oidFor("blob", base)},
	})
	if _, err := ReadPack(bytes.NewReader(pack)); !errors.Is(err, ErrNeedsGit) {
		t.Fatalf("thin pack without bases: got %v, want ErrNeedsGit", err)
	}
}

// TestThinPackFixedAndGitValidated is the acid test for --fix-thin parity:
// resolve a REF_DELTA against an external base, verify the rebuilt pack is
// self-contained and (with git available) that git index-pack accepts it.
func TestThinPackFixedAndGitValidated(t *testing.T) {
	base := []byte("hello\n")
	result := []byte("hello thin world\n")
	baseOID := oidFor("blob", base)
	wire := buildDeltaPack(t, []deltaObj{
		{typ: pkBlob, data: []byte("unrelated\n")},
		{typ: pkRefDelta, data: deltaFor(base, result), refBase: baseOID},
	})
	fetched := 0
	p, fixed, err := ReadPackThin(wire, func(oid string) (string, []byte, error) {
		if oid != baseOID {
			t.Fatalf("asked for unexpected base %s", oid)
		}
		fetched++
		return "blob", base, nil
	})
	if err != nil {
		t.Fatalf("ReadPackThin: %v", err)
	}
	if fetched != 1 {
		t.Fatalf("base fetched %d times, want 1", fetched)
	}
	if len(p.Objects) != 3 { // unrelated + resolved delta + appended base
		t.Fatalf("got %d objects, want 3", len(p.Objects))
	}
	if got := p.Objects[1]; got.OID != oidFor("blob", result) || string(got.Data) != string(result) {
		t.Fatalf("resolved delta wrong: %s %q", got.OID, got.Data)
	}
	if got := p.Objects[2]; got.OID != baseOID {
		t.Fatalf("appended base OID = %s, want %s", got.OID, baseOID)
	}
	if bytes.Equal(fixed, wire) {
		t.Fatal("thin pack was not rebuilt")
	}
	// The fixed pack must re-parse standalone: its REF_DELTA base is now
	// in-pack, so no BaseFunc is needed and the trailer must verify.
	p2, again, err := ReadPackThin(fixed, nil)
	if err != nil {
		t.Fatalf("fixed pack does not re-parse: %v", err)
	}
	if !bytes.Equal(again, fixed) {
		t.Fatal("re-parse rebuilt an already-fixed pack")
	}
	if p2.Trailer != p.Trailer {
		t.Fatal("trailer mismatch between fix and re-parse")
	}

	// Acid test: real git must accept the fixed pack + our idx.
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	idx, err := WriteIdxV2(p)
	if err != nil {
		t.Fatalf("WriteIdxV2: %v", err)
	}
	dir := t.TempDir()
	name := fmt.Sprintf("pack-%x", p.Trailer)
	if err := os.WriteFile(filepath.Join(dir, name+".pack"), fixed, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".idx"), idx, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "verify-pack", "-v", filepath.Join(dir, name+".idx")).CombinedOutput()
	if err != nil {
		t.Fatalf("git verify-pack rejected fixed thin pack: %v\n%s", err, out)
	}
}

// TestWriteIdxV2ValidatedByGit is the acid test: our Go-generated idx must
// be accepted by real git. We build a pack, generate its idx, write both,
// and run `git verify-pack` - which parses the idx, walks the pack via our
// offsets, and checks every CRC.
func TestWriteIdxV2ValidatedByGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	pack := buildTestPack(t, []testObj{
		{pkBlob, []byte("hello\n")},
		{pkCommit, []byte("tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n\nempty\n")},
		{pkBlob, []byte("third object to exercise fanout\n")},
	})
	p, err := ReadPack(bytes.NewReader(pack))
	if err != nil {
		t.Fatalf("ReadPack: %v", err)
	}
	idx, err := WriteIdxV2(p)
	if err != nil {
		t.Fatalf("WriteIdxV2: %v", err)
	}

	dir := t.TempDir()
	name := "pack-" + hex.EncodeToString(p.Trailer[:])
	if err := os.WriteFile(filepath.Join(dir, name+".pack"), pack, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".idx"), idx, 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "verify-pack", "-v", filepath.Join(dir, name+".idx")).CombinedOutput()
	if err != nil {
		t.Fatalf("git verify-pack rejected our idx: %v\n%s", err, out)
	}
}
