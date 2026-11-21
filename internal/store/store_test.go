package store

import (
	"os"
	"path/filepath"
	"testing"

	"kitewheel/internal/model"
)

func TestLogRoundTripAndTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log", "t.wal")
	lg, err := OpenLog(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := lg.Append(RecEnqueue, int64(i), []byte{byte(i), 0xaa}); err != nil {
			t.Fatal(err)
		}
	}
	if err := lg.Sync(); err != nil {
		t.Fatal(err)
	}
	lg.Close()

	// Simulate a crash in the middle of an append: append garbage that looks
	// like a truncated record.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{RecAdvance, 1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	lg, err = OpenLog(path, 0)
	if err != nil {
		t.Fatalf("torn tail should be dropped, got %v", err)
	}
	if lg.Seq() != 10 {
		t.Fatalf("seq after torn tail = %d, want 10", lg.Seq())
	}
	rec, err := lg.Append(RecCancel, 99, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if rec.Seq != 11 {
		t.Fatalf("seq = %d", rec.Seq)
	}
	st, _ := os.Stat(path)
	if st.Size() <= 0 {
		t.Fatal("empty log")
	}
	lg.Close()

	rd, err := NewReader(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	var got []int64
	for rd.Next() {
		got = append(got, rd.Record().Seq)
	}
	if rd.Err() != nil {
		t.Fatalf("read: %v", rd.Err())
	}
	if len(got) != 11 || got[0] != 1 || got[10] != 11 {
		t.Fatalf("read %d records, first %d last %v", len(got), got[0], got[len(got)-1])
	}
}

func TestLogMidFileCorruptionIsReported(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log-000000.wal")
	lg, err := OpenLog(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		lg.Append(RecEnqueue, int64(i), []byte("payload-bytes"))
	}
	lg.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a payload byte of record 2; a later intact record follows.
	off := len(data)/2 - 3
	data[off] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	// Recovery keeps the valid prefix but surfaces corruption.
	if _, _, _, err := OpenDir(dir); err == nil {
		// OpenLog is the strict reader; OpenDir uses it, so corruption in the
		// middle must be an error rather than silent truncation.
		t.Fatal("mid-file corruption should be reported")
	}
}

func TestSnapshotGeneration(t *testing.T) {
	root := t.TempDir()
	d, hdr, buckets, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if hdr != nil || len(buckets) != 0 {
		t.Fatal("fresh store should have no snapshot")
	}
	for i := 0; i < 3; i++ {
		if _, err := d.Log().Append(RecEnqueue, int64(i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	msgs := []model.Message{
		{UID: 1, Seq: 1, ID: "a", Due: 100, Payload: "one"},
		{UID: 2, Seq: 2, ID: "b", Due: 900, Payload: "two"},
	}
	if err := d.SaveSnapshot(3, []byte(`{"now":42,"next_uid":9,"next_seq":9}`), []BucketData{
		{Level: 0, Start: 100, Msgs: msgs[:1]},
		{Level: 2, Start: 0, Msgs: msgs[1:]},
	}); err != nil {
		t.Fatal(err)
	}
	if d.Gen() != 1 || d.Covered() != 3 {
		t.Fatalf("gen %d covered %d", d.Gen(), d.Covered())
	}
	// Appending to the new generation continues the global sequence.
	rec, err := d.Log().Append(RecCancel, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Seq != 4 {
		t.Fatalf("post-snapshot seq = %d, want 4", rec.Seq)
	}
	d.Close()

	d2, hdr2, buckets2, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if hdr2 == nil || hdr2.Now != 42 || hdr2.NextUID != 9 {
		t.Fatalf("header %+v", hdr2)
	}
	if len(buckets2) != 2 || buckets2[0].Msgs[0].ID != "a" || buckets2[1].Msgs[0].ID != "b" {
		t.Fatalf("buckets %+v", buckets2)
	}
	// The one record after the snapshot replays.
	rd, err := NewReader(d2.Log().Path(), d2.Log().Seq()-d2.Log().Count())
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	n := 0
	for rd.Next() {
		n++
		if rd.Record().Kind != RecCancel {
			t.Fatalf("unexpected record %v", rd.Record())
		}
	}
	if n != 1 {
		t.Fatalf("replayed %d records after snapshot, want 1", n)
	}
	// A stale generation's files are gone.
	if _, err := os.Stat(filepath.Join(root, "log-000000.wal")); !os.IsNotExist(err) {
		t.Fatalf("old log still present: %v", err)
	}
}
