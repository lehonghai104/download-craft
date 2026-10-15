// Package downloader fetches a file over several parallel HTTP range
// requests, the way Internet Download Manager does: each range is written to
// its own part file, and once every range is complete the parts are joined
// into the final file. Interrupted downloads resume from the parts on disk.
package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultConnections  = 8
	defaultMinPartSize  = 1 << 20
	defaultStallTimeout = 30 * time.Second
	partsSuffix         = ".parts"
	userAgent           = "download-craft/1.0"
)

// retryDelay is the back-off unit between attempts on a failing part: the
// n-th consecutive failure waits n times this long, up to maxBackoffSteps
// times.
var retryDelay = time.Second

const maxBackoffSteps = 10

var errStalled = errors.New("connection stalled")

// Options configures a download.
type Options struct {
	URL string
	// Output is the file to write. When empty, the name comes from the
	// server's Content-Disposition header or the URL path.
	Output string
	// Dir is the directory a relative Output, or the derived name, goes in.
	Dir string
	// Connections is the number of parts downloaded in parallel.
	Connections int
	// Retries is how many consecutive failures a part tolerates before the
	// download gives up. A failure after some progress restarts the count.
	Retries int
	Header  http.Header
	// Overwrite replaces an existing output file instead of failing.
	Overwrite bool
	// UniqueName picks "name (1).ext", "name (2).ext", … when the derived
	// name is taken by a file or an unfinished download, instead of failing
	// or resuming it.
	UniqueName bool
	// MinPartSize keeps small files from being split into tiny parts.
	MinPartSize int64
	// StallTimeout aborts and retries a connection that receives no data
	// for this long.
	StallTimeout time.Duration
	// Client defaults to one that opens a separate HTTP/1.1 connection per part.
	Client *http.Client
}

// Download is a download whose server has been probed and whose file has
// been split into parts. Create one with New, then call Run.
type Download struct {
	opts    Options
	client  *http.Client
	remote  *remoteFile
	path    string
	dir     string // holds the part files and the resume state
	parts   []*part
	resumed int64
	joining atomic.Bool
}

// Stats is a snapshot of a download's progress.
type Stats struct {
	Size       int64 // -1 when the server did not report it
	Downloaded int64
	Joining    bool
	Parts      []PartStats
}

// PartStats is the progress of one part. Length is -1 when the size is unknown.
type PartStats struct {
	Start, Length, Done int64
}

// IncompleteError reports a download that stopped before finishing but kept
// its parts on disk, so downloading the same file to the same path again
// resumes where it stopped.
type IncompleteError struct {
	Err      error
	PartsDir string
}

func (e *IncompleteError) Error() string { return e.Err.Error() }
func (e *IncompleteError) Unwrap() error { return e.Err }

// permanentError marks a failure that retrying cannot fix.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// New probes the server and splits the file into parts. Parts left behind by
// an interrupted download of the same file to the same path are reused.
func New(ctx context.Context, opts Options) (*Download, error) {
	if opts.Connections < 1 {
		opts.Connections = defaultConnections
	}
	if opts.MinPartSize <= 0 {
		opts.MinPartSize = defaultMinPartSize
	}
	if opts.StallTimeout <= 0 {
		opts.StallTimeout = defaultStallTimeout
	}
	client := opts.Client
	if client == nil {
		client = NewClient(opts.Connections)
	}

	remote, err := probe(ctx, client, opts.URL, opts.Header)
	if err != nil {
		return nil, err
	}
	d := &Download{opts: opts, client: client, remote: remote}
	claimed, err := d.resolvePath()
	if err != nil {
		return nil, err
	}
	if err := d.prepare(); err != nil {
		if claimed {
			os.RemoveAll(d.dir)
		}
		return nil, err
	}
	return d, nil
}

// NewClient returns an HTTP client suited to parallel downloads.
func NewClient(connections int) *http.Client {
	// HTTP/2 would multiplex every part over one TCP connection, and
	// separate connections are the point: servers and networks often
	// throttle each connection. The transport is built from scratch because
	// a clone of http.DefaultTransport inherits a TLS config that still
	// offers HTTP/2 to servers.
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		Protocols:             &protocols,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   connections,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		// Transparent gzip would make byte counts disagree with Content-Length.
		DisableCompression: true,
	}}
}

// resolvePath decides the output path. It reports whether it created the
// parts directory to claim a unique name.
func (d *Download) resolvePath() (claimed bool, err error) {
	name := d.opts.Output
	derived := name == ""
	if derived {
		name = d.remote.name
	}
	if d.opts.Dir != "" && !filepath.IsAbs(name) {
		name = filepath.Join(d.opts.Dir, name)
	}
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return false, err
	}

	if derived && d.opts.UniqueName {
		ext := filepath.Ext(name)
		base := strings.TrimSuffix(name, ext)
		for i := 1; ; i++ {
			// Creating the parts directory claims the name atomically, so
			// concurrent downloads of the same file cannot pick the same one.
			// The second check catches a download that finished between the
			// first check and the claim: its file appears before its parts
			// directory goes away.
			if !exists(name) {
				err := os.Mkdir(name+partsSuffix, 0o755)
				if err == nil && !exists(name) {
					claimed = true
					break
				}
				if err == nil {
					os.Remove(name + partsSuffix)
				} else if !errors.Is(err, fs.ErrExist) {
					return false, err
				}
			}
			name = fmt.Sprintf("%s (%d)%s", base, i, ext)
		}
	} else if !d.opts.Overwrite && exists(name) {
		return false, fmt.Errorf("%s: %w", name, fs.ErrExist)
	}
	d.path, d.dir = name, name+partsSuffix
	return claimed, nil
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// prepare lays out the parts, picking up any already on disk.
func (d *Download) prepare() error {
	if d.remote.rangeable {
		d.parts = d.loadState()
	}
	if d.parts == nil {
		// Start afresh, dropping parts of a different version of the file.
		if err := emptyDir(d.dir); err != nil {
			return err
		}
		if d.remote.rangeable {
			d.parts = split(d.remote.size, d.opts.Connections, d.opts.MinPartSize)
			if err := d.saveState(); err != nil {
				return err
			}
		} else {
			d.parts = []*part{{Length: d.remote.size}}
		}
	}
	for _, p := range d.parts {
		p.path = filepath.Join(d.dir, fmt.Sprintf("part-%03d", p.Index))
		if fi, err := os.Stat(p.path); err == nil && fi.Size() <= p.Length {
			p.done.Store(fi.Size())
			d.resumed += fi.Size()
		}
	}
	return nil
}

// emptyDir creates dir or deletes what is in it. It never removes dir
// itself, since its existence is what claims a unique output name.
func emptyDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Path is the file the download is saved to.
func (d *Download) Path() string { return d.path }

// Size is the file size, or -1 if the server did not report it.
func (d *Download) Size() int64 { return d.remote.size }

// Resumable reports whether the server supports ranges, which is what
// allows splitting the file, resuming it, and retrying a part mid-way.
func (d *Download) Resumable() bool { return d.remote.rangeable }

// Resumed is how many bytes were already on disk from an earlier attempt.
func (d *Download) Resumed() int64 { return d.resumed }

// Stats returns the current progress. It is safe to call while Run is running.
func (d *Download) Stats() Stats {
	s := Stats{Size: d.remote.size, Joining: d.joining.Load(), Parts: make([]PartStats, len(d.parts))}
	for i, p := range d.parts {
		done := p.done.Load()
		s.Parts[i] = PartStats{Start: p.Start, Length: p.Length, Done: done}
		s.Downloaded += done
	}
	return s
}

// Run downloads the remaining parts in parallel and joins them into the
// output file. If it fails or ctx is canceled and the download is
// resumable, the parts stay on disk and the error is an *IncompleteError.
func (d *Download) Run(ctx context.Context) error {
	if err := d.fetchAll(ctx); err != nil {
		if !d.remote.rangeable {
			os.RemoveAll(d.dir) // nothing can be resumed without ranges
			return err
		}
		return &IncompleteError{Err: err, PartsDir: d.dir}
	}
	d.joining.Store(true)
	return d.join()
}

// Discard deletes the partial download. Call it only while Run is not running.
func (d *Download) Discard() error { return os.RemoveAll(d.dir) }

// fetchAll downloads every part concurrently and stops them all at the first
// part that fails for good.
func (d *Download) fetchAll(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	for _, p := range d.parts {
		wg.Go(func() {
			if err := d.fetchPart(ctx, p); err != nil {
				once.Do(func() {
					firstErr = err
					cancel()
				})
			}
		})
	}
	wg.Wait()
	return firstErr
}

// fetchPart downloads one part, retrying from where it stopped when a
// connection drops or stalls.
//
// Servers often cap connections per client and turn the extra ones away
// until others finish, so a part only counts a failure towards giving up
// when the download as a whole has made no progress since its last failure.
func (d *Download) fetchPart(ctx context.Context, p *part) error {
	failures, backoff := 0, 0
	lastTotal := d.downloaded()
	for {
		partBefore := p.done.Load()
		err := d.fetchOnce(ctx, p)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.As(err, new(permanentError)) {
			return fmt.Errorf("part %d: %w", p.Index, err)
		}
		if total := d.downloaded(); total > lastTotal {
			failures, lastTotal = 0, total
		}
		failures++
		if failures > d.opts.Retries {
			return fmt.Errorf("part %d: %w", p.Index, err)
		}
		if p.done.Load() > partBefore {
			backoff = 0
		}
		backoff = min(backoff+1, maxBackoffSteps)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(backoff) * retryDelay):
		}
	}
}

func (d *Download) downloaded() int64 {
	var n int64
	for _, p := range d.parts {
		n += p.done.Load()
	}
	return n
}

// fetchOnce makes one request for the rest of the part and appends the
// response to the part file.
func (d *Download) fetchOnce(ctx context.Context, p *part) error {
	f, err := os.OpenFile(p.path, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return permanentError{err}
	}
	defer f.Close()

	done := p.done.Load()
	if !d.remote.rangeable {
		done = 0 // without ranges every attempt starts from the beginning
		p.done.Store(0)
	}
	if p.Length >= 0 && done >= p.Length {
		return f.Close()
	}
	if err := f.Truncate(done); err != nil {
		return permanentError{err}
	}
	if _, err := f.Seek(done, io.SeekStart); err != nil {
		return permanentError{err}
	}

	// A client timeout would cap the whole transfer, so instead give up only
	// when no data has arrived for StallTimeout.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	watchdog := time.AfterFunc(d.opts.StallTimeout, func() { cancel(errStalled) })
	defer watchdog.Stop()
	stalled := func(err error) error {
		if errors.Is(context.Cause(ctx), errStalled) {
			return errStalled
		}
		return err
	}

	req, err := newRequest(ctx, d.opts.URL, d.opts.Header)
	if err != nil {
		return permanentError{err}
	}
	if d.remote.rangeable {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", p.Start+done, p.end()))
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return stalled(err)
	}
	defer resp.Body.Close()
	if err := d.checkResponse(resp, p.Start+done); err != nil {
		return err
	}

	buf := make([]byte, 128<<10)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			watchdog.Reset(d.opts.StallTimeout)
			if p.Length >= 0 {
				n = int(min(int64(n), p.Length-p.done.Load())) // never write past the range
			}
			if _, err := f.Write(buf[:n]); err != nil {
				return permanentError{err}
			}
			if p.done.Add(int64(n)) == p.Length {
				return f.Close()
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return stalled(readErr)
		}
	}
	if p.Length >= 0 {
		return io.ErrUnexpectedEOF
	}
	return f.Close()
}

func (d *Download) checkResponse(resp *http.Response, wantStart int64) error {
	if !d.remote.rangeable {
		if resp.StatusCode != http.StatusOK {
			return statusError(resp)
		}
		return nil
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		return permanentError{errors.New("server stopped honouring range requests")}
	default:
		return statusError(resp)
	}
	start, _, _, err := parseContentRange(resp.Header.Get("Content-Range"))
	if err != nil {
		return err
	}
	if start != wantStart {
		return fmt.Errorf("server sent bytes from %d, want %d", start, wantStart)
	}
	return nil
}

// statusError treats client errors such as 403 or 404 as permanent, except
// for those that invite a retry.
func statusError(resp *http.Response) error {
	err := fmt.Errorf("server responded %s", resp.Status)
	c := resp.StatusCode
	if c >= 400 && c < 500 && c != http.StatusRequestTimeout && c != http.StatusTooManyRequests {
		return permanentError{err}
	}
	return err
}

// join concatenates the parts, in order, into the output file. The first
// part is renamed rather than copied, and each later part is deleted once
// appended, so joining needs little more disk space than the file itself.
// If joining is interrupted, the parts it consumed are simply downloaded
// again on the next run.
func (d *Download) join() error {
	for _, p := range d.parts {
		if p.Length < 0 {
			continue
		}
		fi, err := os.Stat(p.path)
		if err != nil {
			return err
		}
		if fi.Size() != p.Length {
			return fmt.Errorf("part %d has %d bytes, want %d", p.Index, fi.Size(), p.Length)
		}
	}

	joined := filepath.Join(d.dir, "joined")
	if err := os.Rename(d.parts[0].path, joined); err != nil {
		return err
	}
	f, err := os.OpenFile(joined, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	// Seek rather than O_APPEND, so io.Copy can use copy_file_range.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	for _, p := range d.parts[1:] {
		if err := appendFile(f, p.path); err != nil {
			return err
		}
		os.Remove(p.path)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(joined, d.path); err != nil {
		return err
	}
	return os.RemoveAll(d.dir)
}

func appendFile(dst *os.File, path string) error {
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()
	_, err = io.Copy(dst, src)
	return err
}
