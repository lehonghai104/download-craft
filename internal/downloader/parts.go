package downloader

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
)

const stateFile = "state.json"

// part is one byte range of the file, downloaded into its own file.
type part struct {
	Index  int   `json:"index"`
	Start  int64 `json:"start"`
	Length int64 `json:"length"` // -1 when the server did not report a size

	path string
	done atomic.Int64 // bytes written to path so far
}

func (p *part) end() int64 { return p.Start + p.Length - 1 }

// split divides size bytes into at most n contiguous parts, none smaller
// than minPart (except a file that is smaller than minPart altogether).
func split(size int64, n int, minPart int64) []*part {
	if maxParts := (size + minPart - 1) / minPart; int64(n) > maxParts {
		n = int(maxParts)
	}
	n = max(n, 1)
	chunk := size / int64(n)
	parts := make([]*part, n)
	for i := range parts {
		p := &part{Index: i, Start: int64(i) * chunk, Length: chunk}
		if i == n-1 {
			p.Length = size - p.Start
		}
		parts[i] = p
	}
	return parts
}

// state is saved next to the part files so an interrupted download can be
// resumed with the same part layout.
type state struct {
	URL          string  `json:"url"`
	Size         int64   `json:"size"`
	ETag         string  `json:"etag,omitempty"`
	LastModified string  `json:"last_modified,omitempty"`
	Parts        []*part `json:"parts"`
}

func (d *Download) saveState() error {
	data, err := json.MarshalIndent(state{
		URL:          d.opts.URL,
		Size:         d.remote.size,
		ETag:         d.remote.etag,
		LastModified: d.remote.lastModified,
		Parts:        d.parts,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(d.dir, stateFile), data, 0o644)
}

// loadState returns the parts of an earlier download of the same file, or
// nil if there is none or the file on the server has changed since.
func (d *Download) loadState() []*part {
	data, err := os.ReadFile(filepath.Join(d.dir, stateFile))
	if err != nil {
		return nil
	}
	var st state
	if json.Unmarshal(data, &st) != nil {
		return nil
	}
	r := d.remote
	if st.Size != r.size || st.ETag != r.etag || st.LastModified != r.lastModified {
		return nil
	}
	// With a validator, a refreshed link (e.g. an expired signed URL) can
	// resume; without one, the URL is the only evidence it is the same file.
	if r.etag == "" && r.lastModified == "" && st.URL != d.opts.URL {
		return nil
	}
	var next int64
	for i, p := range st.Parts {
		if p.Index != i || p.Start != next || p.Length <= 0 {
			return nil
		}
		next += p.Length
	}
	if next != r.size {
		return nil
	}
	return st.Parts
}
