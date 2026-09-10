// Package packfmt holds the low-level git packfile object-format primitives -
// object-header and OFS-delta varint decoding, delta application, and bounded
// zlib inflation. It is a leaf (stdlib only) so both the pack ingester
// (internal/ingest) and the random-access remote reader (internal/packstore)
// share one implementation without an import cycle.
package packfmt

import (
	"compress/zlib"
	"errors"
	"fmt"
	"io"
)

// MaxObjectBytes caps one object's expanded size (zlib-bomb / runaway-delta
// guard).
const MaxObjectBytes = 64 << 20

// Git object type codes as they appear in a packfile object header.
const (
	Commit   = 1
	Tree     = 2
	Blob     = 3
	Tag      = 4
	OfsDelta = 6
	RefDelta = 7
)

var typeName = map[int]string{Commit: "commit", Tree: "tree", Blob: "blob", Tag: "tag"}

// TypeName maps a base object type code to its git name, "" if not a base type.
func TypeName(typ int) string { return typeName[typ] }

// ReadObjHeader decodes the variable-length type+size header that precedes
// each packed object. The low 3 bits of the first byte are the type; size is
// little-endian base-128 with the top bit as continuation.
func ReadObjHeader(r io.ByteReader) (typ int, size int64, err error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	typ = int((b >> 4) & 7)
	size = int64(b & 0x0f)
	shift := uint(4)
	for b&0x80 != 0 {
		b, err = r.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		size |= int64(b&0x7f) << shift
		shift += 7
		if shift > 60 {
			return 0, 0, errors.New("object size overflow")
		}
	}
	return typ, size, nil
}

// ReadOfsDeltaOffset decodes the OFS_DELTA base-offset varint (git's "offset
// encoding": each continuation adds 1 before shifting).
func ReadOfsDeltaOffset(r io.ByteReader) (int64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	v := int64(b & 0x7f)
	for b&0x80 != 0 {
		b, err = r.ReadByte()
		if err != nil {
			return 0, err
		}
		v = ((v + 1) << 7) | int64(b&0x7f)
		if v > 1<<40 {
			return 0, errors.New("ofs-delta offset overflow")
		}
	}
	return v, nil
}

// ApplyDelta materializes git's binary delta format: two size varints, then
// copy (bit 7 set: base offset/length from the flagged bytes) and insert
// (literal run) opcodes.
func ApplyDelta(baseData, delta []byte) ([]byte, error) {
	i := 0
	readSize := func() (int64, error) {
		var v int64
		shift := uint(0)
		for {
			if i >= len(delta) {
				return 0, errors.New("truncated delta header")
			}
			b := delta[i]
			i++
			v |= int64(b&0x7f) << shift
			shift += 7
			if b&0x80 == 0 {
				return v, nil
			}
			if shift > 60 {
				return 0, errors.New("delta size overflow")
			}
		}
	}
	srcSize, err := readSize()
	if err != nil {
		return nil, err
	}
	if srcSize != int64(len(baseData)) {
		return nil, fmt.Errorf("delta base size %d != actual %d", srcSize, len(baseData))
	}
	dstSize, err := readSize()
	if err != nil {
		return nil, err
	}
	if dstSize > MaxObjectBytes {
		return nil, fmt.Errorf("delta result %d exceeds cap", dstSize)
	}
	out := make([]byte, 0, dstSize)
	for i < len(delta) {
		op := delta[i]
		i++
		switch {
		case op&0x80 != 0: // copy from base
			var cpOff, cpLen int64
			for bit := 0; bit < 4; bit++ {
				if op&(1<<bit) != 0 {
					if i >= len(delta) {
						return nil, errors.New("truncated copy offset")
					}
					cpOff |= int64(delta[i]) << (8 * bit)
					i++
				}
			}
			for bit := 0; bit < 3; bit++ {
				if op&(1<<(4+bit)) != 0 {
					if i >= len(delta) {
						return nil, errors.New("truncated copy length")
					}
					cpLen |= int64(delta[i]) << (8 * bit)
					i++
				}
			}
			if cpLen == 0 {
				cpLen = 0x10000
			}
			if cpOff < 0 || cpLen < 0 || cpOff+cpLen > int64(len(baseData)) {
				return nil, errors.New("copy out of base bounds")
			}
			out = append(out, baseData[cpOff:cpOff+cpLen]...)
		case op != 0: // insert literal
			n := int(op)
			if i+n > len(delta) {
				return nil, errors.New("truncated insert")
			}
			out = append(out, delta[i:i+n]...)
			i += n
		default:
			return nil, errors.New("delta opcode 0 is reserved")
		}
		if int64(len(out)) > dstSize {
			return nil, errors.New("delta output exceeds declared size")
		}
	}
	if int64(len(out)) != dstSize {
		return nil, fmt.Errorf("delta produced %d bytes, want %d", len(out), dstSize)
	}
	return out, nil
}

// ReadZlibStream inflates one object stream, refusing to expand past the
// per-object cap - a zlib bomb must not be able to allocate gigabytes.
func ReadZlibStream(r io.Reader) ([]byte, error) {
	zr, err := zlib.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	data, err := io.ReadAll(io.LimitReader(zr, MaxObjectBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxObjectBytes {
		return nil, fmt.Errorf("object exceeds %d byte cap", MaxObjectBytes)
	}
	return data, nil
}
