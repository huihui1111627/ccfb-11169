// Package server exposes the scheduler to a browser: the wheel by level, the
// imminent queue, delivered history, calendar rules and counters.
//
// The page is driven by a virtual clock: the operator can step it forward by a
// chosen amount or set it directly, and can inject, cancel and reschedule
// messages, so the whole system can be examined at any simulated instant.
package server

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"kitewheel/internal/calendar"
	"kitewheel/internal/engine"
)

//go:embed web/*
var webFS embed.FS

// Server serves the UI and its JSON API.
type Server struct {
	eng   *engine.Engine
	start int64
	mux   *http.ServeMux
	mu    sync.Mutex // serialises clock-driving mutations
}

// New creates a server for an engine. start is the real wall-clock time the
// simulation started, used to render real timestamps alongside virtual ones.
func New(eng *engine.Engine, start time.Time) (*Server, error) {
	s := &Server{eng: eng, start: start.UnixNano(), mux: http.NewServeMux()}
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		return nil, err
	}
	s.routes(http.FileServer(http.FS(sub)))
	return s, nil
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes(static http.Handler) {
	s.mux.Handle("/", static)
	s.mux.HandleFunc("/api/state", s.handleState)
	s.mux.HandleFunc("/api/advance", s.handleAdvance)
	s.mux.HandleFunc("/api/enqueue", s.handleEnqueue)
	s.mux.HandleFunc("/api/cancel", s.handleCancel)
	s.mux.HandleFunc("/api/reschedule", s.handleReschedule)
	s.mux.HandleFunc("/api/rule", s.handleRule)
	s.mux.HandleFunc("/api/rule/delete", s.handleRuleDelete)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	_ = enc.Encode(v)
}

func fail(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// WheelLevel is the per-level view sent to the page.
type WheelLevel struct {
	Index      int                 `json:"index"`
	Name       string              `json:"name"`
	Len        int64               `json:"len"`
	SpanSec    int64               `json:"span_sec"`
	HorizonSec int64               `json:"horizon_sec"`
	Count      int                 `json:"count"`
	Buckets    []engine.BucketView `json:"buckets"`
}

// State is the whole page payload.
type State struct {
	Now         int64             `json:"now"`
	Start       int64             `json:"start"`
	IsVirtual   bool              `json:"is_virtual"`
	Levels      []WheelLevel      `json:"levels"`
	Upcoming    []engine.Delivery `json:"upcoming"`
	History     []engine.Delivery `json:"history"`
	Rules       []engine.RuleView `json:"rules"`
	Stats       engine.Stats      `json:"stats"`
	Pending     int               `json:"pending"`
	MaxDue      int64             `json:"max_due"`
	GeneratedAt int64             `json:"generated_at"`
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	histN := 200
	if v := r.URL.Query().Get("history"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 5000 {
			histN = n
		}
	}
	upN := 200
	if v := r.URL.Query().Get("upcoming"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 5000 {
			upN = n
		}
	}
	wv := s.eng.WheelView(true)
	levels := make([]WheelLevel, 0, len(wv))
	for _, l := range wv {
		levels = append(levels, WheelLevel{
			Index: l.Index, Name: l.Name, Len: l.Len,
			SpanSec: l.SpanSec, HorizonSec: l.HorizonSec, Count: l.Count, Buckets: l.Buckets,
		})
	}
	st := State{
		Now:         s.eng.Now(),
		Start:       s.start,
		IsVirtual:   true,
		Levels:      levels,
		Upcoming:    s.eng.Upcoming(upN),
		History:     s.eng.History(histN),
		Rules:       s.eng.Rules(),
		Stats:       s.eng.Stats(),
		Pending:     s.eng.Pending(),
		MaxDue:      s.eng.MaxDue(),
		GeneratedAt: time.Now().UnixNano(),
	}
	writeJSON(w, st)
}

type advanceRequest struct {
	Seconds float64 `json:"seconds"`
	Until   int64   `json:"until"` // absolute unix nanoseconds, optional
	Steps   int     `json:"steps"` // advance in this many slices, optional
}

func (s *Server) handleAdvance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, errors.New("POST required"))
		return
	}
	var req advanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	target := req.Until
	if target == 0 {
		if req.Seconds <= 0 {
			fail(w, http.StatusBadRequest, errors.New("seconds must be positive"))
			return
		}
		target = s.eng.Now() + int64(req.Seconds*float64(time.Second))
	}
	if target <= s.eng.Now() {
		fail(w, http.StatusBadRequest, fmt.Errorf("target %d is not after now %d", target, s.eng.Now()))
		return
	}
	steps := req.Steps
	if steps <= 0 {
		steps = 1
	}
	from := s.eng.Now()
	each := (target - from) / int64(steps)
	if each < 1 {
		each = 1
	}
	for i := 0; i < steps && s.eng.Now() < target; i++ {
		next := from + each*int64(i+1)
		if next > target || i == steps-1 {
			next = target
		}
		if err := s.eng.AdvanceTo(next); err != nil {
			fail(w, http.StatusInternalServerError, err)
			return
		}
	}
	writeJSON(w, map[string]int64{"now": s.eng.Now()})
}

type enqueueRequest struct {
	ID       string  `json:"id"`
	Due      int64   `json:"due"`    // absolute unix nanoseconds
	InSec    float64 `json:"in_sec"` // or relative to now
	Priority int     `json:"priority"`
	Payload  string  `json:"payload"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, errors.New("POST required"))
		return
	}
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	due := req.Due
	if due == 0 {
		due = s.eng.Now() + int64(req.InSec*float64(time.Second))
	}
	if strings.TrimSpace(req.ID) == "" {
		req.ID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.eng.Enqueue(req.ID, due, req.Priority, req.Payload)
	if err != nil {
		fail(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, m)
}

type cancelRequest struct {
	ID string `json:"id"`
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, errors.New("POST required"))
		return
	}
	var req cancelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.eng.Cancel(req.ID); err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, map[string]string{"status": "cancelled", "id": req.ID})
}

type rescheduleRequest struct {
	ID       string   `json:"id"`
	Due      int64    `json:"due"`
	InSec    *float64 `json:"in_sec"`
	Priority int      `json:"priority"`
}

func (s *Server) handleReschedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, errors.New("POST required"))
		return
	}
	var req rescheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	due := req.Due
	if due == 0 && req.InSec != nil {
		due = s.eng.Now() + int64(*req.InSec*float64(time.Second))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.eng.Reschedule(req.ID, due, req.Priority)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, m)
}

func (s *Server) handleRule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, errors.New("POST required"))
		return
	}
	var rule calendar.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := s.eng.AddRule(rule)
	if err != nil {
		fail(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, out)
}

func (s *Server) handleRuleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		fail(w, http.StatusMethodNotAllowed, errors.New("POST required"))
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.eng.DeleteRule(req.ID); err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, map[string]string{"status": "deleted", "id": req.ID})
}

// Serve runs the HTTP server until it fails.
func (s *Server) Serve(addr string) error {
	log.Printf("kitewheel UI on http://%s", addr)
	return http.ListenAndServe(addr, s.mux)
}
