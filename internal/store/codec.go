package store

import (
	"encoding/binary"
	"errors"

	"kitewheel/internal/model"
)

// Message encoding, version 3:
//
//	u8  version
//	u64 uid
//	u64 seq
//	u64 due
//	i32 priority
//	u8  class
//	u64 created
//	str id
//	str payload
//	u8  hasRule, then str ruleID, u64 ruleOrd, i64 index, str description
const messageVersion = 3

var errShort = errors.New("store: truncated payload")

func appendStr(dst []byte, s string) []byte {
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(s)))
	dst = append(dst, l[:]...)
	return append(dst, s...)
}

func readStr(b []byte, off int) (string, int, error) {
	if off+4 > len(b) {
		return "", 0, errShort
	}
	l := int(binary.LittleEndian.Uint32(b[off:]))
	off += 4
	if l < 0 || off+l > len(b) {
		return "", 0, errShort
	}
	return string(b[off : off+l]), off + l, nil
}

// EncodeMessage serializes a message.
func EncodeMessage(m model.Message) ([]byte, error) {
	if len(m.ID) > 1<<20 || len(m.Payload) > 1<<26 {
		return nil, errors.New("store: message id or payload too large")
	}
	var b [8]byte
	out := make([]byte, 0, 72+len(m.ID)+len(m.Payload))
	out = append(out, messageVersion)
	binary.LittleEndian.PutUint64(b[:], m.UID)
	out = append(out, b[:]...)
	binary.LittleEndian.PutUint64(b[:], m.Seq)
	out = append(out, b[:]...)
	binary.LittleEndian.PutUint64(b[:], uint64(m.Due))
	out = append(out, b[:]...)
	binary.LittleEndian.PutUint32(b[:4], uint32(int32(m.Priority)))
	out = append(out, b[:4]...)
	out = append(out, byte(m.Class))
	binary.LittleEndian.PutUint64(b[:], uint64(m.Created))
	out = append(out, b[:]...)
	out = appendStr(out, m.ID)
	out = appendStr(out, m.Payload)
	if m.Rule == nil {
		out = append(out, 0)
	} else {
		out = append(out, 1)
		out = appendStr(out, m.Rule.RuleID)
		binary.LittleEndian.PutUint64(b[:], m.Rule.RuleOrd)
		out = append(out, b[:]...)
		binary.LittleEndian.PutUint64(b[:], uint64(m.Rule.Index))
		out = append(out, b[:]...)
		out = appendStr(out, m.Rule.Description)
	}
	return out, nil
}

// DecodeMessage parses a message.
func DecodeMessage(b []byte) (model.Message, error) {
	var m model.Message
	if len(b) < 1 || b[0] != messageVersion {
		return m, errors.New("store: unsupported message encoding version")
	}
	off := 1
	const fixed = 8 + 8 + 8 + 4 + 1 + 8
	if off+fixed > len(b) {
		return m, errShort
	}
	m.UID = binary.LittleEndian.Uint64(b[off:])
	off += 8
	m.Seq = binary.LittleEndian.Uint64(b[off:])
	off += 8
	m.Due = int64(binary.LittleEndian.Uint64(b[off:]))
	off += 8
	m.Priority = int(int32(binary.LittleEndian.Uint32(b[off:])))
	off += 4
	m.Class = model.Class(b[off])
	off++
	m.Created = int64(binary.LittleEndian.Uint64(b[off:]))
	off += 8
	var err error
	m.ID, off, err = readStr(b, off)
	if err != nil {
		return m, err
	}
	m.Payload, off, err = readStr(b, off)
	if err != nil {
		return m, err
	}
	if off >= len(b) {
		return m, errShort
	}
	has := b[off]
	off++
	if has == 1 {
		occ := &model.Occurrence{}
		occ.RuleID, off, err = readStr(b, off)
		if err != nil {
			return m, err
		}
		if off+16 > len(b) {
			return m, errShort
		}
		occ.RuleOrd = binary.LittleEndian.Uint64(b[off:])
		off += 8
		occ.Index = int64(binary.LittleEndian.Uint64(b[off:]))
		off += 8
		occ.Description, off, err = readStr(b, off)
		if err != nil {
			return m, err
		}
		m.Rule = occ
	}
	return m, nil
}

// ---- record payloads ----

// CancelPayload encodes a cancel record.
func CancelPayload(uid uint64, id string) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uid)
	return appendStr(append([]byte(nil), b[:]...), id)
}

// DecodeCancel parses a cancel record.
func DecodeCancel(b []byte) (uid uint64, id string, err error) {
	if len(b) < 8 {
		return 0, "", errShort
	}
	uid = binary.LittleEndian.Uint64(b)
	id, _, err = readStr(b, 8)
	return uid, id, err
}

// RuleCreatePayload encodes a rule creation record.
func RuleCreatePayload(ord uint64, rule []byte) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], ord)
	out := append([]byte(nil), b[:]...)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(rule)))
	out = append(out, l[:]...)
	return append(out, rule...)
}

// DecodeRuleCreate parses a rule creation record.
func DecodeRuleCreate(b []byte) (ord uint64, rule []byte, err error) {
	if len(b) < 12 {
		return 0, nil, errShort
	}
	ord = binary.LittleEndian.Uint64(b)
	l := int(binary.LittleEndian.Uint32(b[8:]))
	if 12+l > len(b) {
		return 0, nil, errShort
	}
	return ord, b[12 : 12+l], nil
}

// AdvancePayload records a virtual clock crossing to the given time.
func AdvancePayload(now int64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(now))
	return b[:]
}

// DecodeAdvance parses an advance record.
func DecodeAdvance(b []byte) (int64, error) {
	if len(b) < 8 {
		return 0, errShort
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

// uint64Bytes encodes a single counter.
func uint64Bytes(v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return b[:]
}

// bytesToUint64 decodes a single counter.
func bytesToUint64(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, errShort
	}
	return binary.LittleEndian.Uint64(b), nil
}

// RuleDeletePayload encodes a rule deletion record.
func RuleDeletePayload(id string) []byte {
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(id)))
	return append(l[:], id...)
}

// DecodeRuleDelete parses a rule deletion record.
func DecodeRuleDelete(b []byte) (string, error) {
	if len(b) < 4 {
		return "", errShort
	}
	l := int(binary.LittleEndian.Uint32(b))
	if 4+l > len(b) {
		return "", errShort
	}
	return string(b[4 : 4+l]), nil
}
