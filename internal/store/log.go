package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Record kinds.
const (
	RecEnqueue    uint8 = 1
	RecUpdate     uint8 = 2
	RecCancel     uint8 = 3
	RecRuleCreate uint8 = 4
	RecRuleDelete uint8 = 5
	RecAdvance    uint8 = 6
)

var recNames = map[uint8]string{
	RecEnqueue: "enqueue", RecUpdate: "update", RecCancel: "cancel",
	RecRuleCreate: "rule-create", RecRuleDelete: "rule-delete", RecAdvance: "advance",
}

// RecordName returns a human label for a record kind.
func RecordName(k uint8) string {
	if s, ok := recNames[k]; ok {
		return s
	}
	return fmt.Sprintf("unknown(%d)", k)
}

// Record is one entry of the engine's history.
type Record struct {
	Seq  int64
	Kind uint8
	Time int64
	Data []byte
}

const headerSize = 13 // kind(1) + virtualTime(8) + payloadLen(4)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt reports a record whose CRC does not match and which is not the
// torn tail of a crash.
var ErrCorrupt = errors.New("store: corrupt log record")

const maxPayload = 64 << 20

func encodeRecord(dst []byte, kind uint8, vt int64, payload []byte) []byte {
	var h [headerSize]byte
	h[0] = kind
	binary.LittleEndian.PutUint64(h[1:], uint64(vt))
	binary.LittleEndian.PutUint32(h[9:], uint32(len(payload)))
	dst = append(dst, h[:]...)
	dst = append(dst, payload...)
	crc := crc32.Checksum(dst, crcTable)
	var c [4]byte
	binary.LittleEndian.PutUint32(c[:], crc)
	return append(dst, c[:]...)
}

// decodeRecordHeader validates a raw encoded record and returns it.
func decodeRecord(raw []byte) (Record, bool) {
	if len(raw) < headerSize+4 {
		return Record{}, false
	}
	l := binary.LittleEndian.Uint32(raw[9:])
	if int(l)+headerSize+4 != len(raw) {
		return Record{}, false
	}
	if crc32.Checksum(raw[:headerSize+int(l)], crcTable) != binary.LittleEndian.Uint32(raw[headerSize+int(l):]) {
		return Record{}, false
	}
	return Record{
		Kind: raw[0],
		Time: int64(binary.LittleEndian.Uint64(raw[1:])),
		Data: raw[headerSize : headerSize+int(l)],
	}, true
}

// Log is an append-only record file. Sequence numbers are global and continue
// across truncation, so a snapshot that covers K records can always be
// described as "covered through sequence K".
type Log struct {
	f    *os.File
	base int64 // sequence of the record before the first byte of this file
	n    int64 // records currently in the file
	size int64
	path string
}

// OpenLog opens (creating if needed) the log. baseSeq is the sequence of the
// record preceding the first byte of this file. A torn tail is truncated.
func OpenLog(path string, baseSeq int64) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	valid, count, err := scanLog(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &Log{f: f, base: baseSeq, n: count, size: valid, path: path}, nil
}

// scanLog validates a whole log and returns its valid prefix length and record
// count. Appends are sequential, so only the final record can be torn and
// dropped; a bad record anywhere else is reported as corruption.
func scanLog(f *os.File) (valid int64, count int64, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := st.Size()
	hdr := make([]byte, headerSize)
	for valid < size {
		remaining := size - valid
		if _, err := f.ReadAt(hdr, valid); err != nil && remaining >= headerSize {
			return 0, 0, err
		}
		if remaining < headerSize {
			return valid, count, nil // torn header at the tail
		}
		l := int64(binary.LittleEndian.Uint32(hdr[9:]))
		if l > maxPayload {
			return 0, 0, fmt.Errorf("%w at offset %d: implausible payload length %d", ErrCorrupt, valid, l)
		}
		end := valid + headerSize + l + 4
		if end > size {
			return valid, count, nil // torn payload at the tail
		}
		raw := make([]byte, end-valid)
		if _, err := f.ReadAt(raw, valid); err != nil {
			return 0, 0, err
		}
		if _, ok := decodeRecord(raw); !ok {
			if end == size {
				return valid, count, nil // torn final record
			}
			return 0, 0, fmt.Errorf("%w at offset %d", ErrCorrupt, valid)
		}
		valid = end
		count++
	}
	return valid, count, nil
}

// Append writes one record, returning it with its global sequence number.
func (l *Log) Append(kind uint8, vt int64, payload []byte) (Record, error) {
	buf := encodeRecord(nil, kind, vt, payload)
	if _, err := l.f.Write(buf); err != nil {
		return Record{}, err
	}
	l.size += int64(len(buf))
	l.n++
	return Record{Seq: l.base + l.n, Kind: kind, Time: vt, Data: append([]byte(nil), payload...)}, nil
}

// Sync flushes the log.
func (l *Log) Sync() error { return l.f.Sync() }

// Seq returns the last assigned global sequence number.
func (l *Log) Seq() int64 { return l.base + l.n }

// Size returns the byte size.
func (l *Log) Size() int64 { return l.size }

// Count returns the number of records in the file.
func (l *Log) Count() int64 { return l.n }

// Path returns the file path.
func (l *Log) Path() string { return l.path }

// Close closes the file.
func (l *Log) Close() error { return l.f.Close() }

// Reader iterates a log file.
type Reader struct {
	f       *os.File
	base    int64
	cur     Record
	started bool
	err     error
}

// NewReader opens a log for reading. A missing file is an empty log.
func NewReader(path string, baseSeq int64) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Reader{base: baseSeq}, nil
		}
		return nil, err
	}
	return &Reader{f: f, base: baseSeq}, nil
}

// Close releases the file.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	return r.f.Close()
}

// Next advances to the next record. A torn tail ends iteration with a nil
// error; corruption before the tail sets Err.
func (r *Reader) Next() bool {
	if r.err != nil || r.f == nil {
		return false
	}
	hdr := make([]byte, headerSize)
	n, err := io.ReadFull(r.f, hdr)
	if err != nil {
		if err == io.EOF && n == 0 {
			return false
		}
		if err == io.ErrUnexpectedEOF {
			return false
		}
		r.err = err
		return false
	}
	l := int64(binary.LittleEndian.Uint32(hdr[9:]))
	if l > maxPayload {
		r.err = fmt.Errorf("%w at record %d", ErrCorrupt, r.cur.Seq+1)
		return false
	}
	raw := make([]byte, headerSize+l+4)
	copy(raw, hdr)
	if _, err := io.ReadFull(r.f, raw[headerSize:]); err != nil {
		return false // torn tail
	}
	rec, ok := decodeRecord(raw)
	if !ok {
		r.err = fmt.Errorf("%w at record %d", ErrCorrupt, r.cur.Seq+1)
		return false
	}
	if !r.started {
		r.cur.Seq = r.base
		r.started = true
	}
	r.cur.Seq++
	r.cur.Kind = rec.Kind
	r.cur.Time = rec.Time
	r.cur.Data = rec.Data
	return true
}

// Record returns the current record.
func (r *Reader) Record() Record { return r.cur }

// Err reports why iteration stopped other than at a clean end.
func (r *Reader) Err() error { return r.err }

// writeFileAtomic writes via a temp file and an atomic rename.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}
