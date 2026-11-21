// Package store holds the durable pieces of the service: a generation-based
// snapshot directory, an append-only record log with CRC framing, and the
// materialized wheel-bucket files.
//
// # On-disk layout
//
//	<root>/MANIFEST            JSON {version, gen, covered}
//	<root>/snap-<gen>.json     snapshot header plus bucket index
//	<root>/gen-<gen>/...       bucket files, immutable once published
//	<root>/log-<gen>.wal       log records after the snapshot
//
// # Publishing protocol
//
// A snapshot is published by (1) writing every new-generation file, (2)
// creating the new log file, which already carries the covered sequence as its
// base, then (3) atomically renaming MANIFEST to the new generation. A crash at
// any instant therefore leaves either the old manifest with the old complete
// log, or the new manifest with an empty new log, never a mixture. Sequence
// numbers are global, so replaying the new log over the snapshot applies
// exactly the records the snapshot does not already contain.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"

	"kitewheel/internal/model"
)

// DefaultLayoutVersion is the on-disk layout version.
const DefaultLayoutVersion = 1

// Manifest points at a snapshot generation.
type Manifest struct {
	Version int   `json:"version"`
	Gen     int64 `json:"gen"`
	Covered int64 `json:"covered"` // global sequence covered by the snapshot
}

// SnapshotHeader is the engine part of a snapshot, produced and consumed by the
// engine; the store only indexes the buckets.
type SnapshotHeader struct {
	Version int             `json:"version"`
	Gen     int64           `json:"gen"`
	Now     int64           `json:"now"`
	NextUID uint64          `json:"next_uid"`
	NextSeq uint64          `json:"next_seq"`
	RuleOrd uint64          `json:"rule_ord"`
	Stats   json.RawMessage `json:"stats"`
	Rules   json.RawMessage `json:"rules"`
	Buckets []BucketRef     `json:"buckets"`
}

// BucketRef locates a bucket file within a generation directory.
type BucketRef struct {
	Level int    `json:"level"`
	Start int64  `json:"start"`
	Count int    `json:"count"`
	File  string `json:"file"`
}

// BucketData is one bucket to persist or restore.
type BucketData struct {
	Level int
	Start int64
	Msgs  []model.Message
}

// Dir is an open store root.
type Dir struct {
	root     string
	manifest Manifest
	log      *Log
}

// OpenDir opens or creates a store root and returns its snapshot (nil when
// none) and an open log positioned after it.
func OpenDir(root string) (*Dir, *SnapshotHeader, []BucketData, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, nil, nil, err
	}
	d := &Dir{root: root}
	mf, err := readManifest(root)
	if err != nil {
		return nil, nil, nil, err
	}
	d.manifest = mf
	var (
		hdr     *SnapshotHeader
		buckets []BucketData
	)
	if mf.Gen > 0 {
		hdr, buckets, err = readSnapshot(root, mf.Gen)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	lg, err := OpenLog(d.logPath(mf.Gen), mf.Covered)
	if err != nil {
		return nil, nil, nil, err
	}
	d.log = lg
	return d, hdr, buckets, nil
}

// Log returns the append log.
func (d *Dir) Log() *Log { return d.log }

// Gen returns the current generation.
func (d *Dir) Gen() int64 { return d.manifest.Gen }

// Covered returns the global sequence covered by the active snapshot.
func (d *Dir) Covered() int64 { return d.manifest.Covered }

// Root returns the store root.
func (d *Dir) Root() string { return d.root }

// Close closes the log.
func (d *Dir) Close() error { return d.log.Close() }

func readManifest(root string) (Manifest, error) {
	var m Manifest
	b, err := os.ReadFile(filepath.Join(root, "MANIFEST"))
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{Version: DefaultLayoutVersion}, nil
		}
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("store: bad MANIFEST: %w", err)
	}
	if m.Version != DefaultLayoutVersion {
		return m, fmt.Errorf("store: unsupported layout version %d", m.Version)
	}
	return m, nil
}

func (d *Dir) logPath(gen int64) string {
	return filepath.Join(d.root, fmt.Sprintf("log-%06d.wal", gen))
}

func (d *Dir) snapPath(gen int64) string {
	return filepath.Join(d.root, fmt.Sprintf("snap-%06d.json", gen))
}

func (d *Dir) genPath(gen int64) string {
	return filepath.Join(d.root, fmt.Sprintf("gen-%06d", gen))
}

func readSnapshot(root string, gen int64) (*SnapshotHeader, []BucketData, error) {
	p := filepath.Join(root, fmt.Sprintf("snap-%06d.json", gen))
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, nil, fmt.Errorf("store: read snapshot: %w", err)
	}
	var h SnapshotHeader
	if err := json.Unmarshal(b, &h); err != nil {
		return nil, nil, fmt.Errorf("store: decode snapshot: %w", err)
	}
	if h.Version != DefaultLayoutVersion {
		return nil, nil, fmt.Errorf("store: unsupported snapshot version %d", h.Version)
	}
	gdir := filepath.Join(root, fmt.Sprintf("gen-%06d", gen))
	out := make([]BucketData, 0, len(h.Buckets))
	for _, ref := range h.Buckets {
		msgs, err := readBucketFile(filepath.Join(gdir, ref.File), ref.Level, ref.Start)
		if err != nil {
			return nil, nil, err
		}
		if len(msgs) != ref.Count {
			return nil, nil, fmt.Errorf("store: bucket %s has %d messages, index says %d", ref.File, len(msgs), ref.Count)
		}
		out = append(out, BucketData{Level: ref.Level, Start: ref.Start, Msgs: msgs})
	}
	return &h, out, nil
}

// SaveSnapshot publishes a new generation. header must be the JSON of a
// SnapshotHeader whose Buckets field is ignored and rebuilt from buckets.
// covered is the global log sequence the snapshot reflects.
//
// Publishing order matters. The generation directory and snapshot file are
// written first, then the new log file is created (empty, with the covered
// sequence as its base), and only then does MANIFEST switch to the new
// generation. A crash before the manifest rename leaves the old manifest and
// the old log intact; a crash after it leaves the new manifest with an
// already-existing empty log. There is no instant at which the manifest points
// at a log that does not exist.
func (d *Dir) SaveSnapshot(covered int64, header []byte, buckets []BucketData) error {
	cur := d.log.Seq()
	if covered > cur {
		return fmt.Errorf("store: snapshot covers %d but log is at %d", covered, cur)
	}
	gen := d.manifest.Gen + 1
	gdir := d.genPath(gen)
	if err := os.RemoveAll(gdir); err != nil {
		return err
	}
	if err := os.MkdirAll(gdir, 0o755); err != nil {
		return err
	}
	var hdr SnapshotHeader
	if err := json.Unmarshal(header, &hdr); err != nil {
		return fmt.Errorf("store: bad snapshot header: %w", err)
	}
	hdr.Version = DefaultLayoutVersion
	hdr.Gen = gen
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].Level != buckets[j].Level {
			return buckets[i].Level < buckets[j].Level
		}
		return buckets[i].Start < buckets[j].Start
	})
	hdr.Buckets = make([]BucketRef, 0, len(buckets))
	for i, b := range buckets {
		name := fmt.Sprintf("b%05d.kwb", i)
		if err := writeBucketFile(filepath.Join(gdir, name), b.Level, b.Start, b.Msgs); err != nil {
			return err
		}
		hdr.Buckets = append(hdr.Buckets, BucketRef{Level: b.Level, Start: b.Start, Count: len(b.Msgs), File: name})
	}
	hb, err := json.Marshal(&hdr)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(d.snapPath(gen), hb); err != nil {
		return err
	}
	newLog, err := OpenLog(d.logPath(gen), covered)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(d.root, "MANIFEST"), mustJSON(Manifest{
		Version: DefaultLayoutVersion, Gen: gen, Covered: covered,
	})); err != nil {
		newLog.Close()
		return err
	}
	oldGen, oldLog := d.manifest.Gen, d.log
	d.log, d.manifest = newLog, Manifest{Version: DefaultLayoutVersion, Gen: gen, Covered: covered}
	oldLog.Close()
	// Cleanup of superseded generations is best effort: a failure only wastes
	// disk and can never affect correctness.
	_ = os.RemoveAll(d.genPath(oldGen))
	_ = os.Remove(d.snapPath(oldGen))
	_ = os.Remove(d.logPath(oldGen))
	return nil
}

func mustJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ---- bucket files ----

var bucketMagic = [4]byte{'K', 'W', 'B', '1'}

const bucketHeaderSize = 4 + 4 + 8 + 4 // magic, level, start, count

func writeBucketFile(path string, level int, start int64, msgs []model.Message) error {
	buf := make([]byte, 0, bucketHeaderSize+96*len(msgs)+4)
	buf = append(buf, bucketMagic[:]...)
	var x [8]byte
	binary.LittleEndian.PutUint32(x[:4], uint32(level))
	buf = append(buf, x[:4]...)
	binary.LittleEndian.PutUint64(x[:], uint64(start))
	buf = append(buf, x[:]...)
	binary.LittleEndian.PutUint32(x[:4], uint32(len(msgs)))
	buf = append(buf, x[:4]...)
	for i := range msgs {
		enc, err := EncodeMessage(msgs[i])
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(x[:4], uint32(len(enc)))
		buf = append(buf, x[:4]...)
		buf = append(buf, enc...)
	}
	binary.LittleEndian.PutUint32(x[:4], crc32.Checksum(buf, crcTable))
	buf = append(buf, x[:4]...)
	return writeFileAtomic(path, buf)
}

func readBucketFile(path string, level int, start int64) ([]model.Message, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("store: read bucket: %w", err)
	}
	if len(data) < bucketHeaderSize+4 {
		return nil, fmt.Errorf("store: bucket %s truncated", path)
	}
	if [4]byte(data[:4]) != bucketMagic {
		return nil, fmt.Errorf("store: bucket %s bad magic", path)
	}
	n := len(data) - 4
	if crc32.Checksum(data[:n], crcTable) != binary.LittleEndian.Uint32(data[n:]) {
		return nil, fmt.Errorf("store: bucket %s bad checksum", path)
	}
	if int(int32(binary.LittleEndian.Uint32(data[4:]))) != level ||
		int64(binary.LittleEndian.Uint64(data[8:])) != start {
		return nil, fmt.Errorf("store: bucket %s does not match its index entry", path)
	}
	count := int(binary.LittleEndian.Uint32(data[16:]))
	off := bucketHeaderSize
	out := make([]model.Message, 0, count)
	for i := 0; i < count; i++ {
		if off+4 > n {
			return nil, fmt.Errorf("store: bucket %s truncated", path)
		}
		l := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if off+l > n {
			return nil, fmt.Errorf("store: bucket %s truncated", path)
		}
		m, err := DecodeMessage(data[off : off+l])
		if err != nil {
			return nil, fmt.Errorf("store: bucket %s: %w", path, err)
		}
		off += l
		out = append(out, m)
	}
	return out, nil
}

// ErrNoSnapshot reports that a store has not been snapshotted yet.
var ErrNoSnapshot = errors.New("store: no snapshot")
