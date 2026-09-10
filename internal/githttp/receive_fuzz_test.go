package githttp

import (
	"bytes"
	"testing"
)

// parsePktCommands is the first parser an authenticated push body hits.
// It must never panic, and when it accepts, its outputs must satisfy the
// bounds tryGoReceive indexes with.
func FuzzParsePktCommands(f *testing.F) {
	// A realistic command section: one update + capabilities, flush, "PACK".
	var b bytes.Buffer
	writePkt(&b, "0000000000000000000000000000000000000000 1111111111111111111111111111111111111111 refs/heads/main\x00report-status side-band-64k\n")
	b.WriteString("0000PACK....")
	f.Add(b.Bytes())
	f.Add([]byte("0000"))
	f.Add([]byte(""))
	f.Add([]byte("0004"))
	f.Fuzz(func(t *testing.T, body []byte) {
		cmds, packOff, _, ok := parsePktCommands(body)
		if !ok {
			return
		}
		if packOff < 0 || packOff > len(body) {
			t.Fatalf("packOff %d out of range [0,%d]", packOff, len(body))
		}
		for _, c := range cmds {
			if c.old == "" || c.new == "" || c.ref == "" {
				t.Fatalf("accepted empty command field: %+v", c)
			}
		}
	})
}

// writeReportStatus must hold for whatever parsePktCommands accepted.
func FuzzWriteReportStatus(f *testing.F) {
	f.Add([]byte("refs/heads/main"), true, true)
	f.Add([]byte("refs/tags/v1\xff\x00weird"), false, false)
	f.Fuzz(func(t *testing.T, ref []byte, sideband, reject bool) {
		var out bytes.Buffer
		var casErr error
		if reject {
			casErr = errTestCAS
		}
		writeReportStatus(&out, sideband, []pushCmd{{old: zeroOID, new: zeroOID, ref: string(ref)}}, casErr)
		if out.Len() == 0 {
			t.Fatal("empty report")
		}
	})
}

var errTestCAS = bytes.ErrTooLarge // any non-nil error triggers the ng path
