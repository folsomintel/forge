package ingest

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"

	"github.com/folsomintel/forge/internal/packfmt"
)

// packread is the fork-free receive path's front half: parse and
// cryptographically verify an incoming git packfile in Go, so a small
// push can be ingested without spawning receive-pack + index-pack.
// Deltas are resolved natively: OFS_DELTA against in-pack bases, and
// REF_DELTA against in-pack bases or - for thin packs, which is what
// `git push` sends for every incremental push - against store objects
// supplied by a BaseFunc. Thin packs are then FIXED (missing bases
// appended, object count and trailer recomputed), exactly like
// `git index-pack --fix-thin`, so the stored pack is self-contained.
// Anything unresolvable returns ErrNeedsGit so the caller falls back to
// forking git, which is always correct.
//
// See docs/fork-elimination.md for the full assessment.

// ErrNeedsGit signals that this pack contains something the Go fast path
// cannot resolve (an external delta base it could not fetch, an
// over-deep delta chain, an oversize expansion). The caller must fall
// back to git receive-pack.
var ErrNeedsGit = errors.New("pack needs git (unresolvable delta/thin)")

// BaseFunc supplies an object that lives outside the pack (a thin pack's
// delta base): its git type ("commit","tree","blob","tag") and full
// content. Returning an error marks the base unavailable.
type BaseFunc func(oid string) (typ string, data []byte, err error)

// Resolution guardrails: the wire pack is already capped small by the
// caller; these bound what a hostile delta stream can expand it into.
const (
	maxDeltaPasses   = 64       // covers any sane chain depth
	maxResolvedBytes = 64 << 20 // total expanded content across the pack
	maxObjectBytes   = 64 << 20 // one object's expanded size
)

// Git object type codes and format primitives live in the leaf packfmt
// package (shared with the random-access remote reader); these aliases keep
// this file's call sites unchanged.
const (
	pkCommit   = packfmt.Commit
	pkTree     = packfmt.Tree
	pkBlob     = packfmt.Blob
	pkTag      = packfmt.Tag
	pkOfsDelta = packfmt.OfsDelta
	pkRefDelta = packfmt.RefDelta
)

var typeName = map[int]string{
	pkCommit: "commit", pkTree: "tree", pkBlob: "blob", pkTag: "tag",
}

// PackObject is one fully-materialized (non-delta) object from a pack,
// with the location and checksum an idx needs to describe it.
type PackObject struct {
	Type   string // commit|tree|blob|tag
	OID    string // sha1 of "type size\x00" + Data
	Data   []byte
	Offset int64  // byte offset of the object header within the pack
	CRC    uint32 // CRC32 (IEEE) of the packed entry (header + zlib stream)
}

// Pack is a fully-parsed, checksum-verified packfile.
type Pack struct {
	Objects []PackObject
	Trailer [20]byte // the pack's own SHA-1; becomes the idx pack-checksum
}

// ReadPack parses a complete packfile from r and resolves in-pack deltas.
// External (thin) delta bases are unresolvable without a BaseFunc, so a
// thin pack returns ErrNeedsGit - use ReadPackThin for those.
func ReadPack(r io.Reader) (*Pack, error) {
	wire, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	p, _, err := ReadPackThin(wire, nil)
	return p, err
}

// rawEntry is one wire entry before delta resolution.
type rawEntry struct {
	typ     int
	off     int64
	crc     uint32
	data    []byte // full content, or the raw delta stream
	baseOff int64  // OFS_DELTA: absolute offset of the base entry
	baseOID string // REF_DELTA: base object id

	rTyp  string // resolved
	rData []byte
	oid   string
}

// ReadPackThin parses a complete packfile, verifies the trailing SHA-1,
// and resolves every delta: OFS_DELTA and in-pack REF_DELTA from the pack
// itself, external REF_DELTA (thin pack) via base. It returns the parsed
// objects and the bytes the caller must store: the wire bytes unchanged
// for a self-contained pack, or - when thin bases were pulled in - a FIXED
// pack with those bases appended, the object count patched, and the
// trailer recomputed (what `git index-pack --fix-thin` produces). The
// returned Pack.Trailer always matches the returned bytes. Unresolvable
// deltas return ErrNeedsGit. Every allocation is bounded by what actually
// decompresses plus the resolution caps.
func ReadPackThin(wire []byte, base BaseFunc) (*Pack, []byte, error) {
	br := &countReader{r: bytes.NewReader(wire), packHash: sha1.New()}

	var magic [4]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, nil, fmt.Errorf("pack header: %w", err)
	}
	if string(magic[:]) != "PACK" {
		return nil, nil, fmt.Errorf("not a packfile (magic %q)", magic)
	}
	var version, count uint32
	if err := binary.Read(br, binary.BigEndian, &version); err != nil {
		return nil, nil, err
	}
	if err := binary.Read(br, binary.BigEndian, &count); err != nil {
		return nil, nil, err
	}
	if version != 2 && version != 3 {
		return nil, nil, fmt.Errorf("unsupported pack version %d", version)
	}
	if count > 1_000_000 {
		return nil, nil, fmt.Errorf("implausible object count %d", count)
	}

	entries := make([]*rawEntry, 0, count)
	byOff := make(map[int64]*rawEntry, count)
	resolvedByOID := make(map[string]*rawEntry, count)
	totalResolved := int64(0)
	for i := uint32(0); i < count; i++ {
		e := &rawEntry{off: br.n}
		br.objCRC = crc32.NewIEEE()
		typ, declaredSize, err := readObjHeader(br)
		if err != nil {
			return nil, nil, fmt.Errorf("object %d header: %w", i, err)
		}
		e.typ = typ
		switch typ {
		case pkOfsDelta:
			rel, err := readOfsDeltaOffset(br)
			if err != nil {
				return nil, nil, fmt.Errorf("object %d ofs-delta: %w", i, err)
			}
			e.baseOff = e.off - rel
			if e.baseOff < 0 {
				return nil, nil, fmt.Errorf("object %d: ofs-delta base before pack start", i)
			}
		case pkRefDelta:
			var raw [20]byte
			if _, err := io.ReadFull(br, raw[:]); err != nil {
				return nil, nil, fmt.Errorf("object %d ref-delta base: %w", i, err)
			}
			e.baseOID = fmt.Sprintf("%x", raw)
		default:
			if _, ok := typeName[typ]; !ok {
				return nil, nil, fmt.Errorf("object %d: unknown type %d", i, typ)
			}
		}
		data, err := readZlib(br)
		if err != nil {
			return nil, nil, fmt.Errorf("object %d inflate: %w", i, err)
		}
		// The header's declared size must equal the inflated length. git's
		// index-pack enforces this; without it a pack whose declared size
		// disagrees with its content passes every other check (the OID is
		// over the inflated bytes, the trailer over the wire bytes) and is
		// stored verbatim - then cat-file/verify-pack/repack choke on it and
		// maintenance for the repo is wedged. For a delta entry the declared
		// size is the delta-stream length; applyDelta checks the RESULT size.
		if declaredSize != int64(len(data)) {
			return nil, nil, fmt.Errorf("object %d: declared size %d != inflated %d", i, declaredSize, len(data))
		}
		e.data = data
		e.crc = br.objCRC.Sum32()
		br.objCRC = nil
		if name, ok := typeName[typ]; ok {
			e.rTyp, e.rData = name, data
			e.oid = oidFor(name, data)
			resolvedByOID[e.oid] = e
			totalResolved += int64(len(data))
			if totalResolved > maxResolvedBytes {
				return nil, nil, ErrNeedsGit // aggregate expansion cap; git spools to disk
			}
		}
		entries = append(entries, e)
		byOff[e.off] = e
	}

	// Trailer: SHA-1 of everything before it. packHash has hashed exactly
	// the pre-trailer bytes; the trailer is read raw (not fed to the hash).
	want := br.packHash.Sum(nil)
	var got [20]byte
	if _, err := io.ReadFull(br.r, got[:]); err != nil {
		return nil, nil, fmt.Errorf("pack trailer: %w", err)
	}
	if !bytes.Equal(want, got[:]) {
		return nil, nil, fmt.Errorf("pack checksum mismatch: computed %x, trailer %x", want, got)
	}

	// Resolve deltas in passes: each pass materializes every delta whose
	// base is now known. External (thin) bases are fetched once and become
	// the objects we later append. No progress in a pass = an unresolvable
	// chain -> ErrNeedsGit (git handles it via its own fallback path).
	type extBase struct {
		typ  string
		data []byte
	}
	external := map[string]extBase{}
	var extOrder []string
	for pass := 0; ; pass++ {
		if pass > maxDeltaPasses {
			return nil, nil, ErrNeedsGit
		}
		progress, pending := false, false
		var deferredRef []*rawEntry // REF_DELTA whose base isn't in-pack YET
		for _, e := range entries {
			if e.rData != nil {
				continue
			}
			var bTyp string
			var bData []byte
			switch e.typ {
			case pkOfsDelta:
				b := byOff[e.baseOff]
				if b == nil {
					return nil, nil, fmt.Errorf("ofs-delta base offset %d not an object", e.baseOff)
				}
				if b.rData == nil {
					pending = true
					continue
				}
				bTyp, bData = b.rTyp, b.rData
			case pkRefDelta:
				if b := resolvedByOID[e.baseOID]; b != nil {
					bTyp, bData = b.rTyp, b.rData
				} else if eb, ok := external[e.baseOID]; ok {
					bTyp, bData = eb.typ, eb.data
				} else {
					// Base not resolved yet. It might be an in-pack delta that
					// resolves in a later pass - do NOT fetch it externally now,
					// or a thin base would be appended AND the in-pack object
					// resolved, duplicating it in the idx. Defer; only fetch
					// externally once no in-pack progress remains.
					pending = true
					deferredRef = append(deferredRef, e)
					continue
				}
			}
			out, err := applyDelta(bData, e.data)
			if err != nil {
				return nil, nil, fmt.Errorf("delta at %d: %w", e.off, err)
			}
			totalResolved += int64(len(out))
			if totalResolved > maxResolvedBytes {
				return nil, nil, ErrNeedsGit
			}
			e.rTyp, e.rData = bTyp, out
			e.oid = oidFor(bTyp, out)
			resolvedByOID[e.oid] = e
			progress = true
		}
		if !pending {
			break
		}
		if !progress {
			// In-pack resolution has reached a fixpoint; any still-deferred
			// REF_DELTA base is genuinely external (thin pack). Fetch them now.
			if base == nil || len(deferredRef) == 0 {
				return nil, nil, ErrNeedsGit
			}
			fetched := false
			for _, e := range deferredRef {
				if _, ok := external[e.baseOID]; ok {
					continue
				}
				typ, data, err := base(e.baseOID)
				if err != nil || typeCode(typ) == 0 {
					return nil, nil, ErrNeedsGit // base unavailable -> git
				}
				external[e.baseOID] = extBase{typ: typ, data: data}
				extOrder = append(extOrder, e.baseOID)
				fetched = true
			}
			if !fetched {
				return nil, nil, ErrNeedsGit
			}
		}
	}

	objs := make([]PackObject, 0, len(entries)+len(external))
	for _, e := range entries {
		objs = append(objs, PackObject{
			Type: e.rTyp, OID: e.oid, Data: e.rData, Offset: e.off, CRC: e.crc,
		})
	}

	p := &Pack{Objects: objs}
	if len(external) == 0 {
		copy(p.Trailer[:], got[:])
		return p, wire, nil
	}

	// Fix the thin pack: append each fetched base as a full object, patch
	// the object count, recompute the trailer. The stored pack must be
	// self-contained - its REF_DELTAs now resolve against in-pack bases.
	body := make([]byte, len(wire)-20, len(wire))
	copy(body, wire[:len(wire)-20])
	binary.BigEndian.PutUint32(body[8:12], count+uint32(len(external)))
	for _, oid := range extOrder {
		eb := external[oid]
		entry := encodeFullObject(typeCode(eb.typ), eb.data)
		off := int64(len(body))
		p.Objects = append(p.Objects, PackObject{
			Type: eb.typ, OID: oid, Data: eb.data,
			Offset: off, CRC: crc32.ChecksumIEEE(entry),
		})
		body = append(body, entry...)
	}
	trailer := sha1.Sum(body)
	body = append(body, trailer[:]...)
	p.Trailer = trailer
	return p, body, nil
}

// WritePack encodes objects as a self-contained v2 pack of full
// (undeltified) entries with a correct trailer - the emitter for merged
// fetch responses built from several stored receive packs. Small-object
// use only: no delta compression is attempted.
func WritePack(objs []PackObject) []byte {
	var b bytes.Buffer
	b.WriteString("PACK")
	binary.Write(&b, binary.BigEndian, uint32(2))
	binary.Write(&b, binary.BigEndian, uint32(len(objs)))
	for _, o := range objs {
		b.Write(encodeFullObject(typeCode(o.Type), o.Data))
	}
	sum := sha1.Sum(b.Bytes())
	b.Write(sum[:])
	return b.Bytes()
}

// typeCode maps a git type name to its pack header code (0 if unknown).
func typeCode(name string) int {
	for code, n := range typeName {
		if n == name {
			return code
		}
	}
	return 0
}

// encodeFullObject emits one undeltified pack entry: the type+size
// varint header followed by the zlib-compressed content.
func encodeFullObject(typ int, data []byte) []byte {
	size := len(data)
	hdr := []byte{byte(typ<<4) | byte(size&0x0f)}
	size >>= 4
	for size > 0 {
		hdr[len(hdr)-1] |= 0x80
		hdr = append(hdr, byte(size&0x7f))
		size >>= 7
	}
	var b bytes.Buffer
	b.Write(hdr)
	zw := zlib.NewWriter(&b)
	zw.Write(data)
	zw.Close()
	return b.Bytes()
}

// The pack-format primitives now live in the leaf packfmt package (shared
// with internal/packstore). These thin wrappers keep this file's call sites
// unchanged and give packfmt a single implementation.
func readOfsDeltaOffset(r io.ByteReader) (int64, error)            { return packfmt.ReadOfsDeltaOffset(r) }
func applyDelta(baseData, delta []byte) ([]byte, error)            { return packfmt.ApplyDelta(baseData, delta) }
func readObjHeader(r io.ByteReader) (typ int, size int64, e error) { return packfmt.ReadObjHeader(r) }
func readZlib(r io.Reader) ([]byte, error)                         { return packfmt.ReadZlibStream(r) }

func oidFor(typ string, data []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", typ, len(data))
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// countReader adapts an io.Reader to io.ByteReader (which zlib and the
// header decoder both want) without over-reading: because it satisfies
// io.ByteReader, flate reads byte-at-a-time and never swallows bytes past
// its stream, so the next object header stays intact. It feeds every byte
// consumed to packHash (pack-trailer verification) and, when set, to
// objCRC (the current object's idx CRC).
type countReader struct {
	r        io.Reader
	n        int64
	packHash hash.Hash
	objCRC   hash.Hash32
}

func (c *countReader) feed(b []byte) {
	c.packHash.Write(b)
	if c.objCRC != nil {
		c.objCRC.Write(b)
	}
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.feed(p[:n])
		c.n += int64(n)
	}
	return n, err
}

func (c *countReader) ReadByte() (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(c.r, b[:]); err != nil {
		return 0, err
	}
	c.feed(b[:])
	c.n++
	return b[0], nil
}
