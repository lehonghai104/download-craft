package downloader

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() { retryDelay = 10 * time.Millisecond }

var modTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func randomData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// serveRanges serves data with full Range support.
func serveRanges(data []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "data.bin", modTime, bytes.NewReader(data))
	}
}

// cutWriter aborts the connection after limit bytes of the body.
type cutWriter struct {
	http.ResponseWriter
	limit int
}

func (w *cutWriter) Write(b []byte) (int, error) {
	if len(b) > w.limit {
		b = b[:w.limit]
	}
	n, _ := w.ResponseWriter.Write(b)
	w.limit -= n
	if w.limit == 0 {
		w.ResponseWriter.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}
	return n, nil
}

func isProbe(r *http.Request) bool { return r.Header.Get("Range") == "bytes=0-0" }

func testOptions(t *testing.T, url string) Options {
	return Options{
		URL:          url + "/files/data.bin",
		Dir:          t.TempDir(),
		Connections:  8,
		Retries:      3,
		MinPartSize:  64 << 10,
		StallTimeout: time.Second,
	}
}

func run(t *testing.T, opts Options) (*Download, error) {
	t.Helper()
	d, err := New(context.Background(), opts)
	if err != nil {
		return nil, err
	}
	return d, d.Run(context.Background())
}

func checkFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: content differs (got %d bytes, want %d)", path, len(got), len(want))
	}
	if exists(path + partsSuffix) {
		t.Errorf("parts directory %s was not removed", path+partsSuffix)
	}
}

func TestParallelDownload(t *testing.T) {
	data := randomData(t, 3<<20+123)
	var ranged atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isProbe(r) && r.Header.Get("Range") != "" {
			ranged.Add(1)
		}
		serveRanges(data)(w, r)
	}))
	defer srv.Close()

	d, err := run(t, testOptions(t, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(d.parts); n != 8 {
		t.Errorf("got %d parts, want 8", n)
	}
	if n := ranged.Load(); n != 8 {
		t.Errorf("got %d range requests, want 8", n)
	}
	if got := filepath.Base(d.Path()); got != "data.bin" {
		t.Errorf("file name %q, want data.bin", got)
	}
	checkFile(t, d.Path(), data)
}

func TestSmallFileUsesFewerParts(t *testing.T) {
	data := randomData(t, 100<<10)
	srv := httptest.NewServer(serveRanges(data))
	defer srv.Close()

	d, err := run(t, testOptions(t, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(d.parts); n != 2 {
		t.Errorf("got %d parts, want 2 (min part size 64 KiB)", n)
	}
	checkFile(t, d.Path(), data)
}

func TestServerWithoutRanges(t *testing.T) {
	data := randomData(t, 1<<20)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Write(data)
	}))
	defer srv.Close()

	d, err := run(t, testOptions(t, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	if d.Resumable() || len(d.parts) != 1 {
		t.Errorf("resumable=%v parts=%d, want a single non-resumable part", d.Resumable(), len(d.parts))
	}
	checkFile(t, d.Path(), data)
}

func TestUnknownSize(t *testing.T) {
	data := randomData(t, 300<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(data[:1000]) // flushing before the end forces chunked encoding
		w.(http.Flusher).Flush()
		w.Write(data[1000:])
	}))
	defer srv.Close()

	d, err := run(t, testOptions(t, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	if d.Size() != -1 {
		t.Errorf("size %d, want -1", d.Size())
	}
	checkFile(t, d.Path(), data)
}

func TestEmptyFile(t *testing.T) {
	srv := httptest.NewServer(serveRanges(nil))
	defer srv.Close()

	d, err := run(t, testOptions(t, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	checkFile(t, d.Path(), nil)
}

func TestRetriesDroppedConnections(t *testing.T) {
	data := randomData(t, 2<<20)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drop the first connection of every part after 10 KiB.
		if !isProbe(r) && requests.Add(1) <= 8 {
			w = &cutWriter{ResponseWriter: w, limit: 10 << 10}
		}
		serveRanges(data)(w, r)
	}))
	defer srv.Close()

	d, err := run(t, testOptions(t, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	checkFile(t, d.Path(), data)
}

func TestRetriesStalledConnections(t *testing.T) {
	data := randomData(t, 1<<20)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isProbe(r) && requests.Add(1) == 1 {
			w.WriteHeader(http.StatusPartialContent)
			w.(http.Flusher).Flush()
			<-r.Context().Done() // hang until the client gives up
			return
		}
		serveRanges(data)(w, r)
	}))
	defer srv.Close()

	opts := testOptions(t, srv.URL)
	opts.StallTimeout = 200 * time.Millisecond
	d, err := run(t, opts)
	if err != nil {
		t.Fatal(err)
	}
	checkFile(t, d.Path(), data)
}

// slowWriter paces writes so transfers overlap even on loopback.
type slowWriter struct{ http.ResponseWriter }

func (w slowWriter) Write(b []byte) (int, error) {
	var n int
	for len(b) > 0 {
		chunk := b[:min(len(b), 16<<10)]
		m, err := w.ResponseWriter.Write(chunk)
		n += m
		if err != nil {
			return n, err
		}
		b = b[m:]
		time.Sleep(2 * time.Millisecond)
	}
	return n, nil
}

func TestServerLimitingConnections(t *testing.T) {
	// The server turns away all but two connections at a time; the other
	// parts must keep retrying while those two make progress.
	data := randomData(t, 2<<20)
	var active atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProbe(r) {
			serveRanges(data)(w, r)
			return
		}
		if active.Add(1) > 2 {
			active.Add(-1)
			http.Error(w, "too many connections", http.StatusServiceUnavailable)
			return
		}
		defer active.Add(-1)
		serveRanges(data)(slowWriter{w}, r)
	}))
	defer srv.Close()

	opts := testOptions(t, srv.URL)
	opts.Retries = 2
	d, err := run(t, opts)
	if err != nil {
		t.Fatal(err)
	}
	checkFile(t, d.Path(), data)
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProbe(r) {
			serveRanges(make([]byte, 1<<20))(w, r)
			return
		}
		requests.Add(1)
		http.Error(w, "gone", http.StatusForbidden)
	}))
	defer srv.Close()

	opts := testOptions(t, srv.URL)
	opts.Connections = 1
	_, err := run(t, opts)
	if err == nil {
		t.Fatal("download succeeded, want 403 error")
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("made %d requests, want 1 (403 should not be retried)", n)
	}
}

func TestResumeAfterInterrupt(t *testing.T) {
	data := randomData(t, 2<<20)
	var broken atomic.Bool
	broken.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if broken.Load() && !isProbe(r) {
			w = &cutWriter{ResponseWriter: w, limit: 20 << 10}
		}
		serveRanges(data)(w, r)
	}))
	defer srv.Close()

	opts := testOptions(t, srv.URL)
	opts.Retries = 0
	_, err := run(t, opts)
	var inc *IncompleteError
	if !errors.As(err, &inc) {
		t.Fatalf("got error %v, want *IncompleteError", err)
	}

	broken.Store(false)
	d, err := New(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if d.Resumed() == 0 {
		t.Error("nothing was resumed")
	}
	if err := d.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkFile(t, d.Path(), data)
}

func TestCancelKeepsParts(t *testing.T) {
	data := randomData(t, 2<<20)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isProbe(r) {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		serveRanges(data)(w, r)
	}))
	defer srv.Close()
	defer close(release)

	opts := testOptions(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	d, err := New(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(100*time.Millisecond, cancel)
	err = d.Run(ctx)
	if !errors.Is(err, context.Canceled) || !errors.As(err, new(*IncompleteError)) {
		t.Fatalf("got %v, want a canceled *IncompleteError", err)
	}
	if !exists(filepath.Join(d.dir, stateFile)) {
		t.Error("resume state was removed")
	}
	if err := d.Discard(); err != nil || exists(d.dir) {
		t.Errorf("Discard: %v, dir still exists: %v", err, exists(d.dir))
	}
}

func TestChangedFileStartsOver(t *testing.T) {
	oldData, newData := randomData(t, 2<<20), randomData(t, 2<<20+1)
	var current atomic.Pointer[[]byte]
	current.Store(&oldData)
	var broken atomic.Bool
	broken.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if broken.Load() && !isProbe(r) {
			w = &cutWriter{ResponseWriter: w, limit: 20 << 10}
		}
		serveRanges(*current.Load())(w, r)
	}))
	defer srv.Close()

	opts := testOptions(t, srv.URL)
	opts.Retries = 0
	if _, err := run(t, opts); err == nil {
		t.Fatal("first run succeeded, want an interruption")
	}

	broken.Store(false)
	current.Store(&newData)
	d, err := run(t, opts)
	if err != nil {
		t.Fatal(err)
	}
	if d.Resumed() != 0 {
		t.Errorf("resumed %d bytes of a file that has changed", d.Resumed())
	}
	checkFile(t, d.Path(), newData)
}

func TestFileName(t *testing.T) {
	for _, tc := range []struct{ disposition, want string }{
		{`attachment; filename="report final.pdf"`, "report final.pdf"},
		{`attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.txt`, "résumé.txt"},
		{`attachment; filename="../../etc/evil.txt"`, "evil.txt"},
		{`attachment; filename=".."`, "data.bin"},
		{"", "data.bin"},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tc.disposition != "" {
				w.Header().Set("Content-Disposition", tc.disposition)
			}
			serveRanges([]byte("hello"))(w, r)
		}))
		opts := testOptions(t, srv.URL)
		d, err := run(t, opts)
		srv.Close()
		if err != nil {
			t.Fatal(err)
		}
		if want := filepath.Join(opts.Dir, tc.want); d.Path() != want {
			t.Errorf("Content-Disposition %q: saved to %s, want %s", tc.disposition, d.Path(), want)
		}
	}
}

func TestExistingOutput(t *testing.T) {
	data := randomData(t, 1000)
	srv := httptest.NewServer(serveRanges(data))
	defer srv.Close()

	opts := testOptions(t, srv.URL)
	existing := filepath.Join(opts.Dir, "data.bin")
	os.WriteFile(existing, []byte("old"), 0o644)

	if _, err := New(context.Background(), opts); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("got %v, want fs.ErrExist", err)
	}

	opts.Overwrite = true
	if _, err := run(t, opts); err != nil {
		t.Fatal(err)
	}
	checkFile(t, existing, data)
}

func TestUniqueNames(t *testing.T) {
	data := randomData(t, 200<<10)
	srv := httptest.NewServer(serveRanges(data))
	defer srv.Close()

	opts := testOptions(t, srv.URL)
	opts.UniqueName = true
	os.WriteFile(filepath.Join(opts.Dir, "data.bin"), []byte("old"), 0o644)

	// Downloads of the same file started together must not share a name.
	var wg sync.WaitGroup
	paths := make([]string, 3)
	for i := range paths {
		wg.Go(func() {
			d, err := run(t, opts)
			if err != nil {
				t.Error(err)
				return
			}
			paths[i] = d.Path()
		})
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, p := range paths {
		if seen[p] {
			t.Errorf("two downloads saved to %s", p)
		}
		seen[p] = true
	}
	for _, name := range []string{"data (1).bin", "data (2).bin", "data (3).bin"} {
		checkFile(t, filepath.Join(opts.Dir, name), data)
	}
}

func TestClientUsesHTTP1(t *testing.T) {
	var protos sync.Map
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protos.Store(r.Proto, true)
		serveRanges(randomData(t, 1<<20))(w, r)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	// Trust the test certificate without replacing the client's TLS config,
	// which is what decides whether HTTP/2 gets negotiated.
	client := NewClient(4)
	tr := client.Transport.(*http.Transport)
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.RootCAs = x509.NewCertPool()
	tr.TLSClientConfig.RootCAs.AddCert(srv.Certificate())
	opts := testOptions(t, srv.URL)
	opts.Client = client
	if _, err := run(t, opts); err != nil {
		t.Fatal(err)
	}
	protos.Range(func(proto, _ any) bool {
		if proto != "HTTP/1.1" {
			t.Errorf("request used %s, want HTTP/1.1", proto)
		}
		return true
	})
}

func TestSplit(t *testing.T) {
	for _, tc := range []struct {
		size    int64
		n       int
		want    int
		minPart int64
	}{
		{size: 100, n: 8, minPart: 1, want: 8},
		{size: 7, n: 8, minPart: 1, want: 7},
		{size: 10 << 20, n: 8, minPart: 1 << 20, want: 8},
		{size: 3<<20 + 1, n: 8, minPart: 1 << 20, want: 4},
		{size: 1, n: 8, minPart: 1 << 20, want: 1},
	} {
		parts := split(tc.size, tc.n, tc.minPart)
		if len(parts) != tc.want {
			t.Errorf("split(%d, %d): %d parts, want %d", tc.size, tc.n, len(parts), tc.want)
		}
		var next int64
		for _, p := range parts {
			if p.Start != next || p.Length <= 0 {
				t.Errorf("split(%d, %d): part %d is %d+%d, want start %d", tc.size, tc.n, p.Index, p.Start, p.Length, next)
			}
			next += p.Length
		}
		if next != tc.size {
			t.Errorf("split(%d, %d): parts cover %d bytes", tc.size, tc.n, next)
		}
	}
}

func TestParseContentRange(t *testing.T) {
	for _, tc := range []struct {
		in                string
		start, end, total int64
		wantErr           bool
	}{
		{in: "bytes 0-0/1234", start: 0, end: 0, total: 1234},
		{in: "bytes 100-199/*", start: 100, end: 199, total: -1},
		{in: "bytes */0", start: -1, end: -1, total: 0},
		{in: "bytes 5-1/10", wantErr: true},
		{in: "items 0-1/2", wantErr: true},
		{in: "bytes 0-1", wantErr: true},
	} {
		start, end, total, err := parseContentRange(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("%q: error %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && (start != tc.start || end != tc.end || total != tc.total) {
			t.Errorf("%q: got %d %d %d, want %d %d %d", tc.in, start, end, total, tc.start, tc.end, tc.total)
		}
	}
}
