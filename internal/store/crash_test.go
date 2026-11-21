package store

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRecoveryAfterTornRecordAtEveryOffset simulates a crash while writing each
// possible number of bytes of a record: the recovery must always keep exactly
// the records that were fully written and no others.
func TestRecoveryAfterTornRecordAtEveryOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log-000000.wal")
	lg, err := OpenLog(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{[]byte("first"), []byte("second-record"), []byte("third")}
	for i, p := range payloads {
		if _, err := lg.Append(RecEnqueue, int64(i), p); err != nil {
			t.Fatal(err)
		}
	}
	lg.Close()
	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Find the byte offset of record 3, then truncate the file at every offset
	// in that record and check recovery keeps exactly records 1 and 2.
	lg, _ = OpenLog(path, 0)
	_ = lg
	var offs []int64
	rd, _ := NewReader(path, 0)
	for rd.Next() {
		rec := rd.Record()
		offs = append(offs, rec.Seq)
	}
	rd.Close()
	if len(offs) != 3 {
		t.Fatalf("setup: %d records", len(offs))
	}

	// Locate the start of each record by re-scanning with the same framing.
	starts := recordStarts(whole)
	third := starts[2]
	// Only cuts strictly before the end are crashes; the full file is valid.
	for cut := third; cut < int64(len(whole)); cut++ {
		victim := filepath.Join(t.TempDir(), "log-000000.wal")
		if err := os.WriteFile(victim, whole[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		l, err := OpenLog(victim, 0)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if got := l.Count(); got != 2 {
			t.Fatalf("cut %d: recovered %d records, want 2", cut, got)
		}
		// Recovery must have truncated the torn tail away, leaving room for
		// new appends that continue the global sequence.
		rec, err := l.Append(RecCancel, 9, nil)
		if err != nil || rec.Seq != 3 {
			t.Fatalf("cut %d: append after recovery: %v seq %d", cut, err, rec.Seq)
		}
		l.Close()
	}
}

// recordStarts returns the byte offset of each record.
func recordStarts(data []byte) []int64 {
	var out []int64
	off := int64(0)
	for off < int64(len(data)) {
		out = append(out, off)
		l := int64(data[off+9]) | int64(data[off+10])<<8 | int64(data[off+11])<<16 | int64(data[off+12])<<24
		off += headerSize + l + 4
	}
	return out
}

// TestManifestNeverPointsAtMissingLog checks the publishing order: after a
// snapshot the manifest's generation has an existing log, and the previous
// generation's files are gone.
func TestManifestNeverPointsAtMissingLog(t *testing.T) {
	root := t.TempDir()
	d, _, _, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for gen := int64(1); gen <= 5; gen++ {
		if _, err := d.Log().Append(RecAdvance, gen, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := d.SaveSnapshot(d.Log().Seq(), []byte(`{"now":1}`), nil); err != nil {
			t.Fatalf("gen %d: %v", gen, err)
		}
		if d.Gen() != gen {
			t.Fatalf("generation %d, want %d", d.Gen(), gen)
		}
		if _, err := os.Stat(d.logPath(d.Gen())); err != nil {
			t.Fatalf("gen %d: manifest log missing: %v", gen, err)
		}
		if _, err := os.Stat(d.snapPath(d.Gen())); err != nil {
			t.Fatalf("gen %d: snapshot missing: %v", gen, err)
		}
	}
	d.Close()

	// Reopening resolves consistently.
	d2, hdr, _, err := OpenDir(root)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if d2.Gen() != 5 || hdr == nil {
		t.Fatalf("gen %d hdr %+v", d2.Gen(), hdr)
	}
	// Every live generation directory is exactly the current one.
	entries, _ := os.ReadDir(root)
	gens := 0
	for _, e := range entries {
		if len(e.Name()) > 4 && e.Name()[:4] == "gen-" {
			gens++
		}
	}
	if gens != 1 {
		t.Fatalf("%d generation directories left behind, want 1", gens)
	}
}
