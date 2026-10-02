package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Format constants (docs/wal-format.md, v1).
const (
	HeaderSize   = 16
	FormatVer    = 0x0001
	MaxPayload   = 16 << 20          // 16 MiB, package constant, not in header
	MaxKeySize   = 1 << 10           // 1 KiB
	MaxValueSize = MaxPayload - 4096 // 16 MiB - 4 KiB
)

// Record types. 0 is deliberately unused so zeroed disk regions are invalid.
const (
	TypePut    byte = 1
	TypeDelete byte = 2
)

var magic = [4]byte{'R', 'K', 'V', 'W'}

// Castagnoli table, built once. Fixed for the life of format version 1.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrInvalid  = errors.New("wal: invalid record")
	ErrTooLarge = errors.New("wal: key or value exceeds size limit")
	ErrEmptyKey = errors.New("wal: empty key")
)

// Record is the in-memory form of one log entry.
// For TypeDelete, Value is ignored.
type Record struct {
	Type  byte
	Key   []byte
	Value []byte
}

// encodeHeader returns the 16-byte file header.
// Layout: magic(4) | version(2) | reserved(6, zero) | crc32c(bytes 0..11)(4)
func encodeHeader() []byte {
	h := make([]byte, HeaderSize)
	copy(h[0:4], magic[:])
	binary.BigEndian.PutUint16(h[4:6], FormatVer)
	// h[6:12] stays zero (reserved)
	binary.BigEndian.PutUint32(h[12:16], crc32.Checksum(h[0:12], castagnoli))
	return h
}

// encodeRecord builds one record as a single contiguous buffer:
//
//	CRC(4) | Length(4) | Type(1) | Payload(Length)
//
// CRC covers Length + Type + Payload. Length counts the payload only.
func encodeRecord(r Record) ([]byte, error) {
	// 1. Validate and compute payload length (int math, after the caps).
	if len(r.Key) == 0 {
		return nil, ErrEmptyKey
	}
	if len(r.Key) > MaxKeySize {
		return nil, ErrTooLarge
	}

	var payloadLen int
	switch r.Type {
	case TypePut:
		if len(r.Value) > MaxValueSize {
			return nil, ErrTooLarge
		}
		payloadLen = 4 + len(r.Key) + 4 + len(r.Value)
	case TypeDelete:
		payloadLen = 4 + len(r.Key)
	default:
		return nil, ErrInvalid
	}

	// 2. One allocation: 4 (CRC) + 4 (Length) + 1 (Type) + payload.
	buf := make([]byte, 9+payloadLen)
	binary.BigEndian.PutUint32(buf[4:8], uint32(payloadLen))
	buf[8] = r.Type

	// 3. Payload.
	p := buf[9:]
	binary.BigEndian.PutUint32(p[0:4], uint32(len(r.Key)))
	n := 4 + copy(p[4:], r.Key)
	if r.Type == TypePut {
		binary.BigEndian.PutUint32(p[n:n+4], uint32(len(r.Value)))
		copy(p[n+4:], r.Value)
	}

	// 4. CRC over everything after the CRC field (Length + Type + Payload).
	binary.BigEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], castagnoli))
	return buf, nil
}
