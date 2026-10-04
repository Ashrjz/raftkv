package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Format constants (docs/wal-format.md, v1).
const (
	HeaderSize   = 16
	FormatVer    = 0x0001
	MaxPayload   = 16 << 20          // 16 MiB, package constant, not in header
	MaxKeySize   = 1 << 10           // 1 KiB
	MaxValueSize = MaxPayload - 4096 // 16 MiB - 4 KiB
	minPayload   = 5                 // smallest valid payload: a Delete with 1-byte key (4 + 1)
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
	ErrInvalid   = errors.New("wal: invalid record")
	ErrTooLarge  = errors.New("wal: key or value exceeds size limit")
	ErrEmptyKey  = errors.New("wal: empty key")
	ErrBadLength = fmt.Errorf("%w: bad length", ErrInvalid)
	ErrChecksum  = fmt.Errorf("%w: checksum mismatch", ErrInvalid)
	ErrBadType   = fmt.Errorf("%w: unknown record type", ErrInvalid)
	ErrMalformed = fmt.Errorf("%w: malformed payload", ErrInvalid)
	ErrBadHeader = errors.New("wal: invalid header")
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

func validateHeader(h []byte) error {
	if len(h) != HeaderSize {
		return fmt.Errorf("%w: short header", ErrBadHeader)
	}
	if !bytes.Equal(h[0:4], magic[:]) {
		return fmt.Errorf("%w: bad magic", ErrBadHeader)
	}
	if binary.BigEndian.Uint16(h[4:6]) != FormatVer {
		return fmt.Errorf("%w: unsupported version", ErrBadHeader)
	}
	if binary.BigEndian.Uint32(h[12:16]) != crc32.Checksum(h[0:12], castagnoli) {
		return fmt.Errorf("%w: header checksum", ErrBadHeader)
	}
	return nil
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

// ReadRecord reads exactly one record from r.
//
// Returns:
//
//	io.EOF              - clean end: zero bytes available at a record boundary
//	io.ErrUnexpectedEOF - stream ended mid-record (torn tail candidate, task 5)
//	ErrInvalid          - bad length, CRC mismatch, bad type, etc.
func ReadRecord(r io.Reader) (Record, int, error) {
	var prefix [9]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return Record{}, 0, err // io.EOF if 0 bytes, io.ErrUnexpectedEOF if 1..8
	}

	length := binary.BigEndian.Uint32(prefix[4:8])
	if length < minPayload || length > MaxPayload {
		return Record{}, len(prefix), ErrBadLength // n = bytes consumed so far
	}

	buf := make([]byte, 9+int(length))
	copy(buf, prefix[:])
	if _, err := io.ReadFull(r, buf[9:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF // we already consumed the prefix
		}
		return Record{}, 0, err
	}

	rec, _, err := DecodeRecord(buf) // CRC, type, payload structure
	return rec, len(buf), err
}

// DecodeRecord reads one record from a byte slice starting at offset 0.
// It validates the CRC, length, and type, then returns the decoded Record.
// Returns (Record, bytesConsumed, error).
func DecodeRecord(buf []byte) (Record, int, error) {
	if len(buf) < 9 {
		return Record{}, 0, ErrInvalid // not enough for CRC(4) + Length(4) + Type(1)
	}

	// 1. Parse header
	crc := binary.BigEndian.Uint32(buf[0:4])
	length := binary.BigEndian.Uint32(buf[4:8])
	typ := buf[8]

	// 2. Validate length
	if length < minPayload || length > MaxPayload {
		return Record{}, 0, ErrBadLength
	}

	// 3. Ensure we have the full record
	totalLen := 9 + int(length)
	if len(buf) < totalLen {
		return Record{}, 0, ErrInvalid // partial record, will be handled in task 5
	}

	// 4. Validate CRC over Length + Type + Payload
	payload := buf[9 : 9+length]
	expectedCRC := crc32.Checksum(buf[4:9+length], castagnoli)
	if crc != expectedCRC {
		return Record{}, 0, ErrChecksum // corrupted
	}

	// 5. Validate type
	if typ != TypePut && typ != TypeDelete {
		return Record{}, 0, ErrBadType
	}

	// 6. Decode payload
	if len(payload) < 4 {
		return Record{}, 0, ErrMalformed
	}
	keyLen := binary.BigEndian.Uint32(payload[0:4])
	if int(keyLen) > MaxKeySize || 4+int(keyLen) > len(payload) {
		return Record{}, 0, ErrMalformed
	}

	key := payload[4 : 4+keyLen]
	var value []byte

	if typ == TypePut {
		if 4+int(keyLen)+4 > len(payload) {
			return Record{}, 0, ErrMalformed
		}
		valueLen := binary.BigEndian.Uint32(payload[4+keyLen : 4+keyLen+4])
		if int(valueLen) > MaxValueSize || 4+int(keyLen)+4+int(valueLen) != len(payload) {
			return Record{}, 0, ErrMalformed
		}
		value = payload[4+keyLen+4 : 4+keyLen+4+valueLen]
	}
	// For Delete, value is ignored and stays nil

	return Record{Type: typ, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)}, totalLen, nil
}
