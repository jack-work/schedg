package webserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jack-work/schedg/internal/config"
	"github.com/jack-work/schedg/internal/priority"
	"github.com/jack-work/schedg/internal/queue"
	"github.com/jack-work/schedg/internal/sched"
	"github.com/jack-work/schedg/internal/webserver/logger"
)

type Server struct {
	cfg *config.Config
	mux *http.ServeMux
	log *logger.RingLog
}

func New(cfg *config.Config, webDir string) *Server {
	s := &Server{
		cfg: cfg,
		mux: http.NewServeMux(),
		log: logger.New(500),
	}
	s.mux.HandleFunc("GET /api/queues", s.handleQueues)
	s.mux.HandleFunc("GET /api/queues/{name}/status", s.handleStatus)
	s.mux.HandleFunc("GET /api/queues/{name}/events", s.handleSSE)
	s.mux.HandleFunc("GET /api/logs", s.handleLogsSSE)
	s.mux.Handle("GET /", http.FileServer(http.Dir(webDir)))
	return s
}

func (s *Server) ListenAndServe(addr string) error {
	s.log.Info("schedg-web listening on %s", addr)
	log.Printf("schedg-web listening on %s", addr)
	return http.ListenAndServe(addr, s)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	s.log.Info("GET /api/queues")
	type entry struct {
		Name       string `json:"name"`
		Driver     string `json:"driver"`
		Path       string `json:"path"`
		Comparator string `json:"comparator"`
	}
	out := make([]entry, 0, len(s.cfg.DBs))
	for _, db := range s.cfg.DBs {
		cmp := db.Comparator
		if cmp == "" {
			cmp = "priority-submitted"
		}
		out = append(out, entry{Name: db.Name, Driver: db.Driver, Path: db.Path, Comparator: cmp})
	}
	writeJSON(w, out)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.log.Info("GET /api/queues/%s/status", name)
	snap, err := s.snapshot(name)
	if err != nil {
		s.log.Error("snapshot %s: %v", name, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, snap)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.log.Info("SSE /api/queues/%s/events connected", name)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	send := func(snap *QueueSnapshot) {
		data, _ := json.Marshal(snap)
		w.Write([]byte("data: "))
		w.Write(data)
		w.Write([]byte("\n\n"))
		flusher.Flush()
	}

	snap, err := s.snapshot(name)
	if err != nil {
		s.log.Error("initial snapshot %s: %v", name, err)
		w.Write([]byte("event: error\ndata: " + err.Error() + "\n\n"))
		flusher.Flush()
		return
	}
	send(snap)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			s.log.Info("SSE /api/queues/%s/events disconnected", name)
			return
		case <-ticker.C:
			snap, err := s.snapshot(name)
			if err != nil {
				continue
			}
			send(snap)
		}
	}
}

func (s *Server) handleLogsSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for _, entry := range s.log.Entries() {
		data, _ := json.Marshal(entry)
		w.Write([]byte("data: "))
		w.Write(data)
		w.Write([]byte("\n\n"))
	}
	flusher.Flush()

	ch := s.log.Subscribe()
	defer s.log.Unsubscribe(ch)
	for {
		select {
		case <-r.Context().Done():
			return
		case entry := <-ch:
			data, _ := json.Marshal(entry)
			w.Write([]byte("data: "))
			w.Write(data)
			w.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

// --- snapshot ---

type TaskView struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	Priority    int64  `json:"priority"`
	Submitted   string `json:"submitted,omitempty"`
	Attempts    int    `json:"attempts,omitempty"`
	Cancels     int    `json:"cancels,omitempty"`
	Reason      string `json:"reason,omitempty"`
	LeasedAt    string `json:"leasedAt,omitempty"`
}

type DepEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type QueueSnapshot struct {
	Name       string              `json:"name"`
	Driver     string              `json:"driver"`
	Ready      []TaskView          `json:"ready"`
	Blocked    []TaskView          `json:"blocked"`
	Inflight   []TaskView          `json:"inflight"`
	Dead       []TaskView          `json:"dead"`
	Completed  []string            `json:"completed"`
	Deps       []DepEdge           `json:"deps"`
	BlockedBy  map[string][]string `json:"blockedBy"`
	Counts     map[string]int      `json:"counts"`
	SnapshotAt string              `json:"snapshotAt"`
}

var (
	queueMu    sync.Mutex
	queueCache = map[string]*queue.Queue{}
)

func (s *Server) snapshot(name string) (*QueueSnapshot, error) {
	queueMu.Lock()
	defer queueMu.Unlock()

	db, ok := s.cfg.Find(name)
	if !ok {
		return nil, fmt.Errorf("queue not found: %s", name)
	}

	// Reuse cached queue (keeps dolt introspection cache warm).
	q, cached := queueCache[name]
	if !cached {
		var err error
		q, err = queue.Open(*db)
		if err != nil {
			return nil, err
		}
		queueCache[name] = q
	} else {
		if err := q.Reload(context.Background()); err != nil {
			// Stale cache; re-open.
			q.Close()
			delete(queueCache, name)
			var err2 error
			q, err2 = queue.Open(*db)
			if err2 != nil {
				return nil, err2
			}
			queueCache[name] = q
		}
	}

	st := q.Status()
	snap := &QueueSnapshot{
		Name:   name,
		Driver: db.Driver,
		Counts: map[string]int{
			"ready": st.Ready, "blocked": st.Blocked,
			"inflight": st.Inflight, "dead": st.Dead, "completed": st.Completed,
		},
		SnapshotAt: time.Now().UTC().Format(time.RFC3339),
	}

	for _, t := range q.Ready() {
		snap.Ready = append(snap.Ready, toTaskView(t, q.Meta(t.ID)))
	}

	blockedBy := q.Blocked()
	snap.BlockedBy = blockedBy
	for id, t := range q.BlockedAll() {
		snap.Blocked = append(snap.Blocked, toTaskView(t, q.Meta(id)))
	}

	for id, t := range q.Inflight() {
		m := q.Meta(id)
		tv := toTaskView(t, m)
		if !m.LeasedAt.IsZero() {
			tv.LeasedAt = m.LeasedAt.UTC().Format(time.RFC3339)
		}
		snap.Inflight = append(snap.Inflight, tv)
	}

	for id, t := range q.Dead() {
		snap.Dead = append(snap.Dead, toTaskView(t, q.Meta(id)))
	}

	snap.Completed = q.CompletedIDs()

	for taskID, deps := range blockedBy {
		for _, depID := range deps {
			snap.Deps = append(snap.Deps, DepEdge{From: taskID, To: depID})
		}
	}

	// Ensure no nil slices in JSON.
	if snap.Ready == nil {
		snap.Ready = []TaskView{}
	}
	if snap.Blocked == nil {
		snap.Blocked = []TaskView{}
	}
	if snap.Inflight == nil {
		snap.Inflight = []TaskView{}
	}
	if snap.Dead == nil {
		snap.Dead = []TaskView{}
	}
	if snap.Completed == nil {
		snap.Completed = []string{}
	}
	if snap.Deps == nil {
		snap.Deps = []DepEdge{}
	}

	return snap, nil
}

func toTaskView(t priority.Task, m sched.Meta) TaskView {
	title, desc := "", ""
	if t.Fields != nil {
		title = t.Fields["label"]
		desc = t.Fields["description"]
	}
	tv := TaskView{
		ID:          t.ID,
		Title:       title,
		Description: desc,
		Priority:    t.Priority,
		Attempts:    m.Attempts,
		Cancels:     m.Cancels,
		Reason:      m.Reason,
	}
	if !t.Submitted.IsZero() {
		tv.Submitted = t.Submitted.UTC().Format(time.RFC3339)
	}
	return tv
}
