// Package store is the append-only operation log that makes the scheduler
// survive restarts. Every mutation (add/cancel/reschedule/deliver/ack) is
// appended as one JSON line. Compaction rewrites the log keeping only
// messages that are still pending or delivered-but-unacknowledged.
package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"kitefestival/delaymsg/internal/recur"
)

// Operation codes written to the log.
const (
	OpAdd        = "add"       // message scheduled
	OpCancel     = "cancel"    // message cancelled by id
	OpReschedule = "resched"   // message moved to a new deadline (gen bumped)
	OpDelivered  = "delivered" // message fired
	OpAck        = "ack"       // delivered message confirmed by consumer
)

// Record is one log line.
type Record struct {
	Op       string      `json:"op"`
	ID       uint64      `json:"id"`
	Gen      uint64      `json:"gen,omitempty"`
	Deadline int64       `json:"deadline,omitempty"`
	Payload  string      `json:"payload,omitempty"`
	Rule     *recur.Rule `json:"rule,omitempty"`
}

// Log is an append-only record log with compaction support.
type Log struct {
	mu   sync.Mutex
	path string
	f    *os.File
	w    *bufio.Writer
	enc  *json.Encoder
	ops  int64 // records appended since open/rewrite
}

// Open creates or opens the log file for appending.
func Open(path string) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	return &Log{path: path, f: f, w: w, enc: json.NewEncoder(w)}, nil
}

// Append writes one record. It is buffered; call Flush to make it durable.
func (l *Log) Append(r Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enc.Encode(r); err != nil {
		return err
	}
	l.ops++
	return nil
}

// Flush forces buffered records to the OS and fsyncs.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Ops returns the number of records appended since open or last rewrite.
func (l *Log) Ops() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ops
}

// Replay streams every record in the log to fn, oldest first.
func (l *Log) Replay(fn func(Record) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}
	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	for {
		var r Record
		if err := dec.Decode(&r); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("日志损坏: %w", err)
		}
		if err := fn(r); err != nil {
			return err
		}
	}
}

// Rewrite atomically replaces the log with exactly the given records
// (compaction). The caller decides which records survive.
func (l *Log) Rewrite(recs []Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	tmp := l.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	enc := json.NewEncoder(w)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}
	// Reopen the append handle on the new file.
	l.f.Close()
	nf, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	l.f = nf
	l.w = bufio.NewWriterSize(nf, 1<<20)
	l.enc = json.NewEncoder(nf)
	l.ops = 0
	return nil
}

// Size returns the current log file size in bytes.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Flush()
	if st, err := l.f.Stat(); err == nil {
		return st.Size()
	}
	return 0
}

// Close flushes and closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Close()
}
