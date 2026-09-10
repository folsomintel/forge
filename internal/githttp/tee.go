package githttp

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/folsomintel/forge/internal/ingest"
)

// Staged tee push: the receive-pack request body is pkt-line framed command
// sections followed by the raw packfile. We pass every byte through to git
// untouched, and once the pack section starts, tee those bytes into a
// staged blob upload that overlaps the client transfer (ingest.Stager).
//
// Layout handled: commands section (ends at flush), an optional
// push-options section (ends at flush) when the first command line's
// capability list includes push-options. Push certs change the layout and
// are rare - we skip teeing entirely and let the normal upload run.

// teeReceivePack returns the reader to hand to git and a cancel func that
// must be called after git exits (unblocks the pump if git bailed early).
func teeReceivePack(body io.Reader, stage *ingest.StageWriter) (io.Reader, func()) {
	pr, pw := io.Pipe()
	go func() {
		err := pumpReceivePack(body, pw, stage)
		pw.CloseWithError(err)
		if err != nil && err != io.EOF {
			stage.Abort(err)
		} else {
			stage.Close()
		}
	}()
	return pr, func() { pr.CloseWithError(fmt.Errorf("receive-pack exited")) }
}

func pumpReceivePack(body io.Reader, out io.Writer, stage *ingest.StageWriter) error {
	br := bufio.NewReaderSize(body, 64<<10)
	sections := 1
	teeing := true
	first := true
	hdr := make([]byte, 4)
	for sections > 0 {
		if _, err := io.ReadFull(br, hdr); err != nil {
			if err == io.EOF && first {
				return io.EOF // empty body
			}
			return err
		}
		if _, err := out.Write(hdr); err != nil {
			return err
		}
		n, err := pktLen(hdr)
		if err != nil {
			return err
		}
		if n == 0 { // flush-pkt: section boundary
			sections--
			continue
		}
		payload := make([]byte, n-4)
		if _, err := io.ReadFull(br, payload); err != nil {
			return err
		}
		if _, err := out.Write(payload); err != nil {
			return err
		}
		if first {
			first = false
			// "<old> <new> <ref>\0<caps>" - the caps decide the layout.
			if i := bytes.IndexByte(payload, 0); i >= 0 {
				caps := string(payload[i+1:])
				if hasCap(caps, "push-options") {
					sections = 2
				}
				if hasCap(caps, "push-cert") {
					teeing = false // signed pushes restructure the stream
				}
			}
		}
	}
	// Everything after the final flush is the packfile (absent for
	// ref-delete-only pushes).
	dst := out
	if teeing {
		dst = io.MultiWriter(out, stage)
	}
	_, err := io.Copy(dst, br)
	return err
}

func pktLen(hdr []byte) (int, error) {
	v, err := hex.DecodeString(string(hdr))
	if err != nil {
		return 0, fmt.Errorf("bad pkt-line length %q", hdr)
	}
	n := int(v[0])<<8 | int(v[1])
	if n == 1 || n == 2 || n == 3 {
		return 0, nil // delim/response-end: treat as boundary-less specials
	}
	if n != 0 && n < 4 {
		return 0, fmt.Errorf("bad pkt-line length %d", n)
	}
	return n, nil
}

func hasCap(caps, want string) bool {
	for _, c := range strings.Fields(caps) {
		if c == want {
			return true
		}
	}
	return false
}
