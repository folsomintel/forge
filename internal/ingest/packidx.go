package ingest

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

// WriteIdxV2 builds a git pack index (v2) for an already-parsed pack, so
// upload-pack can serve what the fast path stored without ever forking
// index-pack. Layout: magic + version, 256-entry fanout, sorted OID
// table, CRC32 table, offset table (all in OID order), the pack's own
// checksum, then the idx checksum. Offsets are 32-bit; a pack with an
// object past 2GiB (MSB would be needed) is rejected - the fast path
// only runs on small pushes anyway.
func WriteIdxV2(p *Pack) ([]byte, error) {
	objs := make([]PackObject, len(p.Objects))
	copy(objs, p.Objects)
	sort.Slice(objs, func(i, j int) bool { return objs[i].OID < objs[j].OID })

	var b bytes.Buffer
	b.Write([]byte{0xff, 0x74, 0x4f, 0x63}) // \377tOc
	binary.Write(&b, binary.BigEndian, uint32(2))

	// Fanout: for each possible first byte v, the number of objects whose
	// first OID byte is <= v (cumulative).
	var fanout [256]uint32
	for _, o := range objs {
		fb := oidFirstByte(o.OID)
		for v := int(fb); v < 256; v++ {
			fanout[v]++
		}
	}
	for _, c := range fanout {
		binary.Write(&b, binary.BigEndian, c)
	}

	// Sorted OID table.
	for _, o := range objs {
		raw, err := hex.DecodeString(o.OID)
		if err != nil || len(raw) != 20 {
			return nil, fmt.Errorf("bad OID %q", o.OID)
		}
		b.Write(raw)
	}
	// CRC32 table (same order).
	for _, o := range objs {
		binary.Write(&b, binary.BigEndian, o.CRC)
	}
	// Offset table (same order).
	for _, o := range objs {
		if o.Offset >= 1<<31 {
			return nil, fmt.Errorf("offset %d exceeds 2GiB idx-v2 limit", o.Offset)
		}
		binary.Write(&b, binary.BigEndian, uint32(o.Offset))
	}

	// Pack checksum, then idx checksum over everything so far.
	b.Write(p.Trailer[:])
	sum := sha1.Sum(b.Bytes())
	b.Write(sum[:])
	return b.Bytes(), nil
}

func oidFirstByte(oid string) byte {
	// oid is 40 lowercase hex chars; the first byte is the first 2.
	hi := hexVal(oid[0])<<4 | hexVal(oid[1])
	return hi
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}
