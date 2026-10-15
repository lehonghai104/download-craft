package webui

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fileServer serves data with Range support. While held, each transfer
// stops after its first 16 KiB until released, which keeps downloads
// running long enough to pause or stop them.
type fileServer struct {
	*httptest.Server
	data []byte

	mu      sync.Mutex
	release chan struct{}
}

func newFileServer(t *testing.T, size int) *fileServer {
	data := make([]byte, size)
	rand.Read(data)
	fs := &fileServer{data: data}
	fs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.mu.Lock()
		release := fs.release
		fs.mu.Unlock()
		if release != nil && r.Header.Get("Range") != "bytes=0-0" {
			w = &heldWriter{ResponseWriter: w, r: r, release: release, free: 16 << 10}
		}
		http.ServeContent(w, r, "file.bin", time.Unix(1700000000, 0), bytes.NewReader(fs.data))
	}))
	t.Cleanup(fs.Close)
	return fs
}

func (fs *fileServer) hold() {
	fs.mu.Lock()
	fs.release = make(chan struct{})
	fs.mu.Unlock()
}

func (fs *fileServer) unhold() {
	fs.mu.Lock()
	if fs.release != nil {
		close(fs.release)
		fs.release = nil
	}
	fs.mu.Unlock()
}

type heldWriter struct {
	http.ResponseWriter
	r       *http.Request
	release chan struct{}
	free    int
}

func (w *heldWriter) Write(b []byte) (int, error) {
	if w.free <= 0 {
		select {
		case <-w.release:
		case <-w.r.Context().Done():
			return 0, w.r.Context().Err()
		}
		return w.ResponseWriter.Write(b)
	}
	b = b[:min(len(b), w.free)]
	n, err := w.ResponseWriter.Write(b)
	w.free -= n
	w.ResponseWriter.(http.Flusher).Flush()
	return n, err
}

func newManager(t *testing.T, listFile string) *Manager {
	t.Helper()
	m, err := NewManager(t.TempDir(), 4, listFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Shutdown)
	return m
}

func waitFor(t *testing.T, m *Manager, id int, state string) jobView {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		v := m.find(id).view(time.Now())
		if v.State == state {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatalf("download %d is %s (%s), want %s", id, v.State, v.Error, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForData waits until a running download has received some bytes.
func waitForData(t *testing.T, m *Manager, id int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for m.find(id).view(time.Now()).Downloaded == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("download %d received no data", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func checkFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: got %d bytes, want %d matching bytes", path, len(got), len(want))
	}
}

func TestDownloadCompletes(t *testing.T) {
	srv := newFileServer(t, 5<<20)
	m := newManager(t, "")

	v := m.Add(srv.URL+"/file.bin", 4)
	v = waitFor(t, m, v.ID, stateCompleted)
	if v.Downloaded != v.Size || len(v.Parts) != 4 {
		t.Errorf("downloaded %d of %d in %d parts, want all in 4 parts", v.Downloaded, v.Size, len(v.Parts))
	}
	checkFile(t, v.Path, srv.data)

	m.Remove(m.find(v.ID))
	if len(m.List()) != 0 {
		t.Error("download still listed after Remove")
	}
	checkFile(t, v.Path, srv.data) // removing a completed download keeps the file
}

func TestPauseAndResume(t *testing.T) {
	srv := newFileServer(t, 5<<20)
	srv.hold()
	m := newManager(t, "")

	v := m.Add(srv.URL+"/file.bin", 4)
	waitForData(t, m, v.ID)
	if err := m.Pause(m.find(v.ID)); err != nil {
		t.Fatal(err)
	}
	v = waitFor(t, m, v.ID, statePaused)
	if v.Downloaded == 0 {
		t.Fatal("nothing downloaded before pausing")
	}
	if err := m.Pause(m.find(v.ID)); err != errWrongState {
		t.Errorf("pausing a paused download: got %v, want errWrongState", err)
	}

	srv.unhold()
	if err := m.Resume(m.find(v.ID)); err != nil {
		t.Fatal(err)
	}
	if got := m.find(v.ID).view(time.Now()).Downloaded; got < v.Downloaded {
		t.Errorf("resumed at %d bytes, had %d when paused", got, v.Downloaded)
	}
	v = waitFor(t, m, v.ID, stateCompleted)
	checkFile(t, v.Path, srv.data)
}

func TestStopDeletesPartialDownload(t *testing.T) {
	srv := newFileServer(t, 5<<20)
	srv.hold()
	defer srv.unhold()
	m := newManager(t, "")

	v := m.Add(srv.URL+"/file.bin", 4)
	v = waitFor(t, m, v.ID, stateDownloading)
	if err := m.Stop(m.find(v.ID)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, m, v.ID, stateStopped)
	entries, _ := os.ReadDir(filepath.Dir(v.Path))
	if len(entries) != 0 {
		t.Errorf("download directory still holds %v", entries)
	}
}

func TestSameLinkTwiceGetsTwoFiles(t *testing.T) {
	srv := newFileServer(t, 1<<20)
	m := newManager(t, "")

	a := m.Add(srv.URL+"/file.bin", 4)
	b := m.Add(srv.URL+"/file.bin", 4)
	a = waitFor(t, m, a.ID, stateCompleted)
	b = waitFor(t, m, b.ID, stateCompleted)
	if a.Path == b.Path {
		t.Fatalf("both downloads saved to %s", a.Path)
	}
	checkFile(t, a.Path, srv.data)
	checkFile(t, b.Path, srv.data)
}

func TestListSurvivesRestart(t *testing.T) {
	srv := newFileServer(t, 5<<20)
	srv.hold()
	listFile := filepath.Join(t.TempDir(), "downloads.json")
	dir := t.TempDir()

	m1, err := NewManager(dir, 4, listFile)
	if err != nil {
		t.Fatal(err)
	}
	done := m1.Add(srv.URL+"/done.bin", 2)
	srv.unhold()
	waitFor(t, m1, done.ID, stateCompleted)
	srv.hold()
	running := m1.Add(srv.URL+"/file.bin", 4)
	waitForData(t, m1, running.ID)
	m1.Shutdown() // as on Ctrl+C: running downloads are paused

	m2, err := NewManager(dir, 4, listFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m2.Shutdown)
	list := m2.List()
	if len(list) != 2 || list[0].State != statePaused || list[1].State != stateCompleted {
		t.Fatalf("restored list %+v, want a paused and a completed download", list)
	}
	if list[0].Downloaded == 0 || list[0].Size != int64(len(srv.data)) {
		t.Errorf("restored progress %d of %d, want some of %d", list[0].Downloaded, list[0].Size, len(srv.data))
	}

	srv.unhold()
	if err := m2.Resume(m2.find(running.ID)); err != nil {
		t.Fatal(err)
	}
	v := waitFor(t, m2, running.ID, stateCompleted)
	checkFile(t, v.Path, srv.data)
	if next := m2.Add(srv.URL+"/file.bin", 1); next.ID <= running.ID {
		t.Errorf("new download got id %d, which may clash with restored ids", next.ID)
	}
}

func TestHTTPAPI(t *testing.T) {
	srv := newFileServer(t, 1<<20)
	m := newManager(t, "")
	ui := httptest.NewServer(m.Handler(true))
	defer ui.Close()

	post := func(path, body string, header map[string]string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, ui.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range header {
			if k == "Host" {
				req.Host = v
			} else {
				req.Header.Set(k, v)
			}
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}

	if resp := post("/api/downloads", `{"url":"ftp://example.com/x"}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("non-http link: status %d, want 400", resp.StatusCode)
	}
	if resp := post("/api/downloads", `{"url":"`+srv.URL+`/a.bin"}`, map[string]string{"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"}); resp.StatusCode != http.StatusForbidden {
		t.Errorf("cross-site request: status %d, want 403", resp.StatusCode)
	}
	if resp := post("/api/downloads", `{"url":"`+srv.URL+`/a.bin"}`, map[string]string{"Host": "evil.example"}); resp.StatusCode != http.StatusForbidden {
		t.Errorf("non-loopback Host: status %d, want 403", resp.StatusCode)
	}
	if len(m.List()) != 0 {
		t.Fatal("a rejected request started a download")
	}

	resp := post("/api/downloads", `{"url":"`+srv.URL+`/a.bin","connections":2}`, map[string]string{"Origin": ui.URL, "Sec-Fetch-Site": "same-origin"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("same-origin add: status %d, want 201", resp.StatusCode)
	}
	waitFor(t, m, 1, stateCompleted)
	if resp := post("/api/downloads/1/pause", "", nil); resp.StatusCode != http.StatusConflict {
		t.Errorf("pausing a completed download: status %d, want 409", resp.StatusCode)
	}

	get, err := http.Get(ui.URL + "/api/downloads")
	if err != nil {
		t.Fatal(err)
	}
	defer get.Body.Close()
	var list struct {
		Downloads []jobView `json:"downloads"`
	}
	if err := json.NewDecoder(get.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Downloads) != 1 || list.Downloads[0].Connections != 2 || list.Downloads[0].Name != "a.bin" {
		t.Errorf("listed %+v, want a.bin with 2 connections", list.Downloads)
	}
}
