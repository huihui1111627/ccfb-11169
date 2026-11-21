package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kitewheel/internal/engine"
)

func newTestServer(t *testing.T) (*httptest.Server, int64) {
	t.Helper()
	base := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	eng, err := engine.New(engine.Config{Now: base.UnixNano(), History: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	s, err := New(eng, base)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, base.UnixNano()
}

func post(t *testing.T, ts *httptest.Server, path string, body any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	res, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("%s: status %d: %v", path, res.StatusCode, out)
	}
	return out
}

func TestPageServed(t *testing.T) {
	ts, _ := newTestServer(t)
	res, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
	for _, want := range []string{"<!doctype html", "分层时间轮", "即将到期", "已投递", "周期规则", "统计"} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Fatalf("page missing %q", want)
		}
	}
}

func TestStateAndMutations(t *testing.T) {
	ts, now := newTestServer(t)
	post(t, ts, "/api/enqueue", map[string]any{"id": "m1", "in_sec": 60, "priority": 3})
	post(t, ts, "/api/enqueue", map[string]any{"id": "m2", "in_sec": 60, "priority": 1})

	res, err := http.Get(ts.URL + "/api/state?history=10&upcoming=10")
	if err != nil {
		t.Fatal(err)
	}
	var st State
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if st.Pending != 2 {
		t.Fatalf("pending %d", st.Pending)
	}
	if len(st.Upcoming) != 2 || st.Upcoming[0].ID != "m1" {
		t.Fatalf("upcoming %+v", st.Upcoming)
	}
	if len(st.Levels) != 4 || st.Levels[0].Len != 60 {
		t.Fatalf("levels %+v", st.Levels)
	}

	// Advance past both: higher priority first.
	post(t, ts, "/api/advance", map[string]any{"seconds": 61})
	res, _ = http.Get(ts.URL + "/api/state?history=10&upcoming=10")
	st = State{}
	json.NewDecoder(res.Body).Decode(&st)
	res.Body.Close()
	if st.Stats.Delivered != 2 || len(st.History) != 2 {
		t.Fatalf("stats %+v history %d", st.Stats, len(st.History))
	}
	if st.History[1].ID != "m1" || st.History[0].ID != "m2" {
		t.Fatalf("delivery order: %s then %s", st.History[1].ID, st.History[0].ID)
	}

	// Reschedule and cancel through the API.
	post(t, ts, "/api/enqueue", map[string]any{"id": "m3", "in_sec": 100})
	post(t, ts, "/api/reschedule", map[string]any{"id": "m3", "in_sec": 200})
	post(t, ts, "/api/cancel", map[string]any{"id": "m3"})
	res, _ = http.Get(ts.URL + "/api/state")
	st = State{}
	json.NewDecoder(res.Body).Decode(&st)
	res.Body.Close()
	if st.Pending != 0 || st.Stats.Cancelled != 1 || st.Stats.Rescheduled != 1 {
		t.Fatalf("after cancel: pending %d stats %+v", st.Pending, st.Stats)
	}

	// A calendar rule created and deleted through the API.
	post(t, ts, "/api/rule", map[string]any{
		"id": "lastsat", "kind": "monthly_nth_weekday", "nth": -1, "weekday": 6,
		"time_of_day": "16:00", "offset_sec": -1800, "tz_offset_sec": 8 * 3600,
	})
	res, _ = http.Get(ts.URL + "/api/state")
	st = State{}
	json.NewDecoder(res.Body).Decode(&st)
	res.Body.Close()
	if len(st.Rules) != 1 || st.Rules[0].Next == nil {
		t.Fatalf("rules %+v", st.Rules)
	}
	post(t, ts, "/api/rule/delete", map[string]any{"id": "lastsat"})
	res, _ = http.Get(ts.URL + "/api/state")
	st = State{}
	json.NewDecoder(res.Body).Decode(&st)
	res.Body.Close()
	if len(st.Rules) != 0 {
		t.Fatalf("rules after delete %+v", st.Rules)
	}

	_ = now
}

func TestErrorsSurface(t *testing.T) {
	ts, _ := newTestServer(t)
	b, _ := json.Marshal(map[string]any{"id": "nope"})
	res, err := http.Post(ts.URL+"/api/cancel", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("cancel unknown: status %d", res.StatusCode)
	}
	var out map[string]string
	json.NewDecoder(res.Body).Decode(&out)
	if out["error"] == "" {
		t.Fatal("expected an error message")
	}

	// A due beyond the horizon must be refused by the enqueue endpoint.
	b, _ = json.Marshal(map[string]any{"id": "far", "in_sec": 400 * 86400})
	res, err = http.Post(ts.URL+"/api/enqueue", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("far due: status %d", res.StatusCode)
	}
}
