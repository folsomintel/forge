package packstore

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/folsomintel/forge/internal/packfmt"
)

// Random-access object reads from a remote pack: resolve an oid to its byte
// offset via the (local, small) .idx, then inflate just that object - and its
// delta base chain - through the block Reader. Nothing but the requested
// objects (and their bases) is paged in, so a repo far larger than local disk
// serves object reads from the bucket.

const maxDeltaDepth = 64 // guards a cyclic/pathological delta chain

// Index is a parsed pack .idx (v2), kept as the raw bytes with computed
// section offsets so an oid lookup allocates nothing. The idx is small enough
// to keep local even for a remote-placed repo.
type Index struct {
	raw    []byte
	n      int
	oidOff int // start of the sorted 20-byte oid table
	offOff int // start of the 4-byte offset table
	bigOff int // start of the 8-byte large-offset table
}

var idxMagicV2 = []byte{0xff, 't', 'O', 'c'}

// ParseIndex validates and indexes a v2 pack index.
func ParseIndex(data []byte) (*Index, error) {
	if len(data) < 8+256*4+28 {
		return nil, fmt.Errorf("packstore: idx too short")
	}
	if string(data[:4]) != string(idxMagicV2) {
		return nil, fmt.Errorf("packstore: not a v2 idx (bad magic)")
	}
	if binary.BigEndian.Uint32(data[4:8]) != 2 {
		return nil, fmt.Errorf("packstore: unsupported idx version")
	}
	fanoutOff := 8
	n := int(binary.BigEndian.Uint32(data[fanoutOff+255*4:]))
	oidOff := fanoutOff + 256*4
	crcOff := oidOff + 20*n
	offOff := crcOff + 4*n
	bigOff := offOff + 4*n
	if bigOff > len(data) {
		return nil, fmt.Errorf("packstore: idx truncated (n=%d)", n)
	}
	return &Index{raw: data, n: n, oidOff: oidOff, offOff: offOff, bigOff: bigOff}, nil
}

// Count is the number of objects in the pack.
func (ix *Index) Count() int { return ix.n }

// oidAt returns the i-th sorted 20-byte oid.
func (ix *Index) oidAt(i int) []byte { return ix.raw[ix.oidOff+i*20 : ix.oidOff+i*20+20] }

// Offset returns the pack byte offset of oidHex, or false if absent. Binary
// search over the sorted oid table, narrowed by the first-byte fanout.
func (ix *Index) Offset(oidHex string) (int64, bool) {
	target, err := hex.DecodeString(oidHex)
	if err != nil || len(target) != 20 {
		return 0, false
	}
	// Fanout buckets: [fanout[b-1], fanout[b]) bound the first-byte==b range.
	b := target[0]
	lo := 0
	if b > 0 {
		lo = int(binary.BigEndian.Uint32(ix.raw[8+int(b-1)*4:]))
	}
	hi := int(binary.BigEndian.Uint32(ix.raw[8+int(b)*4:]))
	for lo < hi {
		mid := (lo + hi) / 2
		switch cmp := compare20(ix.oidAt(mid), target); {
		case cmp == 0:
			return ix.offsetAt(mid), true
		case cmp < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return 0, false
}

func (ix *Index) offsetAt(i int) int64 {
	v := binary.BigEndian.Uint32(ix.raw[ix.offOff+i*4:])
	if v&0x80000000 == 0 {
		return int64(v)
	}
	big := int(v & 0x7fffffff)
	return int64(binary.BigEndian.Uint64(ix.raw[ix.bigOff+big*8:]))
}

func compare20(a, b []byte) int {
	for i := 0; i < 20; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// ObjectReader serves objects from a pack (any io.ReaderAt + its size) using a
// parsed Index. The ReaderAt may be a remote block Reader (bucket) or a local
// file - the history pack is read locally, the gc pack remotely. Safe for
// concurrent use (each call is independent).
type ObjectReader struct {
	ra   io.ReaderAt
	size int64
	idx  *Index
}

// NewObjectReader pairs a pack (ReaderAt + total size) with its parsed index.
func NewObjectReader(ra io.ReaderAt, size int64, idx *Index) *ObjectReader {
	return &ObjectReader{ra: ra, size: size, idx: idx}
}

// Object returns the git type ("commit"/"tree"/"blob"/"tag") and full
// materialized bytes of oid, resolving OFS/REF deltas through the pack.
func (o *ObjectReader) Object(oidHex string) (typ string, data []byte, err error) {
	off, ok := o.idx.Offset(oidHex)
	if !ok {
		return "", nil, fmt.Errorf("packstore: object %s not in pack", oidHex)
	}
	return o.at(off, 0)
}

// at materializes the object whose header starts at pack offset off.
func (o *ObjectReader) at(off int64, depth int) (string, []byte, error) {
	if depth > maxDeltaDepth {
		return "", nil, fmt.Errorf("packstore: delta chain too deep")
	}
	// A fresh bufio over a section from off: ReadByte for the header/varints,
	// then the same reader feeds zlib. Over-read past the object end is
	// harmless (the reader is discarded after this call).
	sr := io.NewSectionReader(o.ra, off, o.size-off)
	br := bufio.NewReaderSize(sr, 64<<10)

	typ, _, err := packfmt.ReadObjHeader(br)
	if err != nil {
		return "", nil, err
	}
	switch typ {
	case packfmt.Commit, packfmt.Tree, packfmt.Blob, packfmt.Tag:
		data, err := packfmt.ReadZlibStream(br)
		if err != nil {
			return "", nil, err
		}
		return packfmt.TypeName(typ), data, nil

	case packfmt.OfsDelta:
		rel, err := packfmt.ReadOfsDeltaOffset(br)
		if err != nil {
			return "", nil, err
		}
		baseOff := off - rel
		if baseOff < 0 {
			return "", nil, fmt.Errorf("packstore: ofs-delta base before pack start")
		}
		baseType, baseData, err := o.at(baseOff, depth+1)
		if err != nil {
			return "", nil, err
		}
		delta, err := packfmt.ReadZlibStream(br)
		if err != nil {
			return "", nil, err
		}
		out, err := packfmt.ApplyDelta(baseData, delta)
		if err != nil {
			return "", nil, err
		}
		return baseType, out, nil // a delta object has its base's type

	case packfmt.RefDelta:
		var baseOID [20]byte
		if _, err := io.ReadFull(br, baseOID[:]); err != nil {
			return "", nil, err
		}
		baseOff, ok := o.idx.Offset(hex.EncodeToString(baseOID[:]))
		if !ok {
			return "", nil, fmt.Errorf("packstore: ref-delta base %x not in pack", baseOID)
		}
		baseType, baseData, err := o.at(baseOff, depth+1)
		if err != nil {
			return "", nil, err
		}
		delta, err := packfmt.ReadZlibStream(br)
		if err != nil {
			return "", nil, err
		}
		out, err := packfmt.ApplyDelta(baseData, delta)
		if err != nil {
			return "", nil, err
		}
		return baseType, out, nil

	default:
		return "", nil, fmt.Errorf("packstore: unknown object type %d", typ)
	}
}
