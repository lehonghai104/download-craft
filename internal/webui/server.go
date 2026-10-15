package webui

import (
	_ "embed"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

//go:embed index.html
var indexHTML []byte

const maxConnections = 32

// Handler serves the UI page and its JSON API. With loopbackOnly, requests
// must name a loopback host, which defeats DNS-rebinding attacks on a
// server that only listens locally.
func (m *Manager) Handler(loopbackOnly bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/downloads", m.handleList)
	mux.HandleFunc("POST /api/downloads", m.handleAdd)
	mux.HandleFunc("POST /api/downloads/{id}/{action}", m.handleAction)
	mux.HandleFunc("DELETE /api/downloads/{id}", m.handleRemove)

	// This server makes the machine download any URL into a local folder,
	// so other web sites must not be able to drive it from a browser.
	h := http.NewCrossOriginProtection().Handler(mux)
	if loopbackOnly {
		h = requireLoopbackHost(h)
	}
	return h
}

func requireLoopbackHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (m *Manager) handleList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"dir":         m.dir,
		"connections": m.connections,
		"downloads":   m.List(),
	})
}

func (m *Manager) handleAdd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL         string `json:"url"`
		Connections int    `json:"connections"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}
	u, err := url.Parse(strings.TrimSpace(req.URL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		writeError(w, http.StatusBadRequest, "Enter an http:// or https:// link.")
		return
	}
	conns := req.Connections
	if conns < 1 || conns > maxConnections {
		conns = m.connections
	}
	writeJSON(w, http.StatusCreated, m.Add(u.String(), conns))
}

func (m *Manager) handleAction(w http.ResponseWriter, r *http.Request) {
	j := m.jobFromPath(w, r)
	if j == nil {
		return
	}
	var err error
	switch r.PathValue("action") {
	case "pause":
		err = m.Pause(j)
	case "resume":
		err = m.Resume(j)
	case "stop":
		err = m.Stop(j)
	default:
		http.NotFound(w, r)
		return
	}
	if errors.Is(err, errWrongState) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, j.view(time.Now()))
}

func (m *Manager) handleRemove(w http.ResponseWriter, r *http.Request) {
	if j := m.jobFromPath(w, r); j != nil {
		m.Remove(j)
		w.WriteHeader(http.StatusNoContent)
	}
}

func (m *Manager) jobFromPath(w http.ResponseWriter, r *http.Request) *job {
	id, err := strconv.Atoi(r.PathValue("id"))
	var j *job
	if err == nil {
		j = m.find(id)
	}
	if j == nil {
		writeError(w, http.StatusNotFound, "no such download")
	}
	return j
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
