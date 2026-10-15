// Package webui is a small download manager served as a web page: add a
// link, watch the parts download, and pause, continue or stop each download.
package webui

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"download-craft/internal/downloader"
)

// Download states as shown in the UI.
const (
	stateStarting    = "starting"
	stateDownloading = "downloading"
	stateJoining     = "joining"
	statePausing     = "pausing"
	statePaused      = "paused"
	stateStopping    = "stopping"
	stateStopped     = "stopped"
	stateCompleted   = "completed"
	stateFailed      = "failed"
)

var errWrongState = errors.New("not possible in the download's current state")

// Manager runs downloads and remembers them across restarts.
type Manager struct {
	dir         string
	connections int
	listFile    string // where the download list is saved; "" to not save it
	retries     int

	mu     sync.Mutex
	jobs   []*job // newest first
	nextID int
	wg     sync.WaitGroup // running downloads
}

type job struct {
	id          int
	url         string
	connections int

	mu     sync.Mutex
	state  string
	err    string
	path   string               // output path, once known
	dl     *downloader.Download // the current or latest run; nil until probed
	saved  jobView              // progress restored from the list file
	cancel context.CancelFunc   // non-nil while a run is in progress
	intent string               // statePaused or stateStopped once the user cancels a run
	meter  downloader.SpeedMeter
}

// jobView is a download as the UI sees it, and as it is saved.
type jobView struct {
	ID          int        `json:"id"`
	URL         string     `json:"url"`
	Name        string     `json:"name"`
	Path        string     `json:"path,omitempty"`
	Connections int        `json:"connections"`
	State       string     `json:"state"`
	Error       string     `json:"error,omitempty"`
	Size        int64      `json:"size"` // -1 when unknown
	Downloaded  int64      `json:"downloaded"`
	Speed       float64    `json:"speed,omitempty"`
	Resumable   bool       `json:"resumable"`
	Parts       []partView `json:"parts,omitempty"`
}

type partView struct {
	Start  int64 `json:"start"`
	Length int64 `json:"length"`
	Done   int64 `json:"done"`
}

// NewManager returns a manager that saves into dir and keeps its download
// list in listFile (if not empty), restoring any list saved there before.
func NewManager(dir string, connections int, listFile string) (*Manager, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m := &Manager{dir: dir, connections: connections, listFile: listFile, retries: 5}
	if err := m.load(); err != nil {
		return nil, err
	}
	return m, nil
}

// Add starts downloading rawURL.
func (m *Manager) Add(rawURL string, connections int) jobView {
	m.mu.Lock()
	m.nextID++
	j := &job{id: m.nextID, url: rawURL, connections: connections, saved: jobView{Size: -1}}
	m.jobs = slices.Insert(m.jobs, 0, j)
	m.mu.Unlock()

	j.mu.Lock()
	m.start(j)
	j.mu.Unlock()
	m.save()
	return j.view(time.Now())
}

// start launches a run of j. j.mu must be held.
func (m *Manager) start(j *job) {
	ctx, cancel := context.WithCancel(context.Background())
	j.cancel, j.intent, j.state, j.err = cancel, "", stateStarting, ""
	j.meter = downloader.SpeedMeter{}
	opts := downloader.Options{URL: j.url, Dir: m.dir, Connections: j.connections, Retries: m.retries}
	if j.path != "" {
		opts.Output = j.path // continue into the parts already on disk
	} else {
		opts.UniqueName = true
	}
	m.wg.Go(func() { m.run(ctx, j, opts) })
}

func (m *Manager) run(ctx context.Context, j *job, opts downloader.Options) {
	d, err := downloader.New(ctx, opts)
	if err == nil {
		j.mu.Lock()
		j.dl, j.path = d, d.Path()
		if j.state == stateStarting {
			j.state = stateDownloading
		}
		j.mu.Unlock()
		m.save()
		err = d.Run(ctx)
	}

	j.mu.Lock()
	j.cancel()
	j.cancel = nil
	switch {
	case err == nil:
		j.state = stateCompleted
	case j.intent == stateStopped:
		if j.dl != nil {
			j.dl.Discard()
		}
		j.state = stateStopped
	case j.intent == statePaused:
		j.state = statePaused
	default:
		j.state, j.err = stateFailed, err.Error()
	}
	j.mu.Unlock()
	m.save()
}

// Pause stops a running download, keeping its parts so it can continue.
func (m *Manager) Pause(j *job) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cancel == nil || j.intent != "" {
		return errWrongState
	}
	j.intent, j.state = statePaused, statePausing
	j.cancel()
	return nil
}

// Resume continues a paused or failed download.
func (m *Manager) Resume(j *job) error {
	j.mu.Lock()
	if j.cancel != nil || (j.state != statePaused && j.state != stateFailed) {
		j.mu.Unlock()
		return errWrongState
	}
	m.start(j)
	j.mu.Unlock()
	m.save()
	return nil
}

// Stop cancels a download and deletes what it had downloaded.
func (m *Manager) Stop(j *job) error {
	j.mu.Lock()
	switch {
	case j.cancel != nil:
		if j.intent == stateStopped {
			j.mu.Unlock()
			return errWrongState
		}
		j.intent, j.state = stateStopped, stateStopping
		j.cancel() // run discards the parts once it has exited
		j.mu.Unlock()
		return nil
	case j.state == statePaused || j.state == stateFailed:
		m.discard(j)
		j.state = stateStopped
		j.mu.Unlock()
		m.save()
		return nil
	default:
		j.mu.Unlock()
		return errWrongState
	}
}

// discard deletes the partial download of a job that is not running. j.mu
// must be held.
func (m *Manager) discard(j *job) {
	if j.dl != nil {
		j.dl.Discard()
	} else if j.path != "" {
		// Restored from the list file, so there is no Download to ask.
		os.RemoveAll(j.path + ".parts")
	}
}

// Remove drops a download from the list, stopping it first if needed. A
// completed file is kept.
func (m *Manager) Remove(j *job) {
	m.Stop(j)
	m.mu.Lock()
	m.jobs = slices.DeleteFunc(m.jobs, func(o *job) bool { return o == j })
	m.mu.Unlock()
	m.save()
}

// Shutdown pauses every running download and waits for them to stop, so
// they can continue the next time the manager starts.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	jobs := slices.Clone(m.jobs)
	m.mu.Unlock()
	for _, j := range jobs {
		m.Pause(j)
	}
	m.wg.Wait()
}

func (m *Manager) find(id int) *job {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, j := range m.jobs {
		if j.id == id {
			return j
		}
	}
	return nil
}

// List returns every download, newest first.
func (m *Manager) List() []jobView {
	m.mu.Lock()
	jobs := slices.Clone(m.jobs)
	m.mu.Unlock()
	now := time.Now()
	views := make([]jobView, len(jobs))
	for i, j := range jobs {
		views[i] = j.view(now)
	}
	return views
}

// view describes the job; now feeds the speed meter, and a zero now leaves
// the speed out.
func (j *job) view(now time.Time) jobView {
	j.mu.Lock()
	defer j.mu.Unlock()
	v := j.saved
	v.ID, v.URL, v.Connections, v.State, v.Error = j.id, j.url, j.connections, j.state, j.err
	v.Path, v.Name = j.path, nameOf(j.path, j.url)
	if j.dl == nil {
		return v
	}

	s := j.dl.Stats()
	v.Size, v.Downloaded, v.Resumable = s.Size, s.Downloaded, j.dl.Resumable()
	if j.cancel == nil && v.State != stateCompleted && !v.Resumable {
		v.Downloaded = 0 // nothing was kept, so continuing starts over
	}
	if v.State == stateCompleted && v.Size < 0 {
		v.Size = v.Downloaded
	}
	if s.Joining && v.State == stateDownloading {
		v.State = stateJoining
	}
	if j.cancel != nil && !now.IsZero() {
		v.Speed = j.meter.Update(now, s.Downloaded)
	}
	v.Parts = make([]partView, len(s.Parts))
	for i, p := range s.Parts {
		done := p.Done
		if v.Downloaded == 0 {
			done = 0
		}
		v.Parts[i] = partView{Start: p.Start, Length: p.Length, Done: done}
	}
	return v
}

func nameOf(filePath, rawURL string) string {
	if filePath != "" {
		return filepath.Base(filePath)
	}
	if u, err := url.Parse(rawURL); err == nil {
		if name := path.Base(u.Path); name != "." && name != "/" {
			return name
		}
		return u.Host
	}
	return rawURL
}

// save writes the download list so it survives a restart.
func (m *Manager) save() {
	if m.listFile == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	views := make([]jobView, len(m.jobs))
	for i, j := range m.jobs {
		v := j.view(time.Time{})
		v.Speed, v.Parts = 0, nil
		views[i] = v
	}
	data, err := json.MarshalIndent(views, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(m.listFile), 0o755); err != nil {
		return
	}
	tmp := m.listFile + ".tmp"
	if os.WriteFile(tmp, data, 0o644) == nil {
		os.Rename(tmp, m.listFile)
	}
}

// load restores the list saved by an earlier run. Downloads that were
// running then come back paused.
func (m *Manager) load() error {
	if m.listFile == "" {
		return nil
	}
	data, err := os.ReadFile(m.listFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var views []jobView
	if err := json.Unmarshal(data, &views); err != nil {
		return err
	}
	for _, v := range views {
		j := &job{id: v.ID, url: v.URL, connections: v.Connections, state: v.State, err: v.Error, path: v.Path, saved: v}
		switch v.State {
		case statePaused, stateFailed, stateCompleted, stateStopped:
		default:
			j.state = statePaused
		}
		if j.state == statePaused && !v.Resumable {
			j.saved.Downloaded = 0
		}
		m.jobs = append(m.jobs, j)
		m.nextID = max(m.nextID, v.ID)
	}
	return nil
}
