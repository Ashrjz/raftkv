package wal

import (
	"bytes"
	"encoding/hex"
	"hash/crc32"
	"strings"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(strings.ReplaceAll(s, " ", ""), "|", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCRC32CSanity(t *testing.T) {
	// Proves we're using Castagnoli, not IEEE.
	if got := crc32.Checksum([]byte("123456789"), castagnoli); got != 0xE3069283 {
		t.Fatalf("got %#x", got)
	}
}

func TestEncodeHeaderGolden(t *testing.T) {
	want := mustHex(t, "524B5657 | 0001 | 000000000000 | 5CDEF27B")
	if got := encodeHeader(); !bytes.Equal(got, want) {
		t.Fatalf("got %x want %x", got, want)
	}
}

func TestEncodePutGolden(t *testing.T) {
	want := mustHex(t, "B7622D95 | 0000000A | 01 | 00000001 61 00000001 62")
	got, err := encodeRecord(Record{Type: TypePut, Key: []byte("a"), Value: []byte("b")})
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("got %x, %v want %x", got, err, want)
	}
}

func TestEncodeDeleteGolden(t *testing.T) {
	want := mustHex(t, "75BD31A7 | 00000005 | 02 | 00000001 61")
	got, err := encodeRecord(Record{Type: TypeDelete, Key: []byte("a")})
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("got %x, %v want %x", got, err, want)
	}
}

func TestEncodeLimits(t *testing.T) {
	cases := []struct {
		name string
		r    Record
		want error
	}{
		{"empty key", Record{Type: TypePut, Key: nil, Value: []byte("v")}, ErrEmptyKey},
		{"key too big", Record{Type: TypePut, Key: make([]byte, MaxKeySize+1)}, ErrTooLarge},
		{"value too big", Record{Type: TypePut, Key: []byte("k"), Value: make([]byte, MaxValueSize+1)}, ErrTooLarge},
		{"unknown type", Record{Type: 0, Key: []byte("k")}, ErrInvalid},
	}
	for _, c := range cases {
		if _, err := encodeRecord(c.r); err != c.want {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
}

func TestEncodeBoundaries(t *testing.T) {
	// Max-size Put must fit exactly within MaxPayload.
	r := Record{Type: TypePut, Key: make([]byte, MaxKeySize), Value: make([]byte, MaxValueSize)}
	buf, err := encodeRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	if payload := len(buf) - 9; payload > MaxPayload {
		t.Fatalf("payload %d exceeds MaxPayload", payload)
	}
	// Empty value is legal (distinct from Delete).
	if _, err := encodeRecord(Record{Type: TypePut, Key: []byte("k"), Value: nil}); err != nil {
		t.Fatalf("empty value should be legal: %v", err)
	}
}

/* func TestDecodeHeaderGolden(t *testing.T) {
	want := mustHex(t, "524B5657 | 0001 | 000000000000 | 5CDEF27B")
	// Header validation happens at WAL.Open, not in DecodeRecord.
	// This test is for future reference; skip for now or test header detection separately.
} */

func TestDecodePutGolden(t *testing.T) {
	buf := mustHex(t, "B7622D95 | 0000000A | 01 | 00000001 61 00000001 62")
	rec, consumed, err := DecodeRecord(buf)
	if err != nil || consumed != len(buf) {
		t.Fatalf("DecodeRecord failed: %v, consumed %d", err, consumed)
	}
	if rec.Type != TypePut || string(rec.Key) != "a" || string(rec.Value) != "b" {
		t.Fatalf("got %+v", rec)
	}
}

func TestDecodeDeleteGolden(t *testing.T) {
	buf := mustHex(t, "75BD31A7 | 00000005 | 02 | 00000001 61")
	rec, consumed, err := DecodeRecord(buf)
	if err != nil || consumed != len(buf) {
		t.Fatalf("DecodeRecord failed: %v", err)
	}
	if rec.Type != TypeDelete || string(rec.Key) != "a" {
		t.Fatalf("got %+v", rec)
	}
}

func TestDecodeCorruptCRC(t *testing.T) {
	buf := mustHex(t, "FFFFFFFF | 0000000A | 01 | 00000001 61 00000001 62") // bad CRC
	_, _, err := DecodeRecord(buf)
	if err != ErrChecksum {
		t.Fatalf("expected ErrInvalid for corrupt CRC, got %v", err)
	}
}

func TestDecodePartialRecord(t *testing.T) {
	buf := mustHex(t, "B7622D95 | 0000000A | 01 | 00000001 61") // truncated before value
	_, _, err := DecodeRecord(buf)
	if err != ErrInvalid {
		t.Fatalf("expected ErrInvalid for partial record, got %v", err)
	}
}
