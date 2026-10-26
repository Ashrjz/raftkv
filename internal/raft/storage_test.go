package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func newTestStorage(t *testing.T) (*FileStorage, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

func fixCRC(b []byte) {
	binary.BigEndian.PutUint32(b[24:28], crc32.Checksum(b[:stateBodyLen], castagnoli))
}

func TestStorageMissingFileIsFreshNode(t *testing.T) {
	s, _ := newTestStorage(t)
	hs, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs != (HardState{}) {
		t.Fatalf("got %+v, want zero value", hs)
	}
}

func TestStorageRoundTripAndOverwrite(t *testing.T) {
	s, dir := newTestStorage(t)
	for _, want := range []HardState{{Term: 1, VotedFor: 2}, {Term: 5, VotedFor: 0}, {Term: 1 << 40, VotedFor: 3}} {
		if err := s.Save(want); err != nil {
			t.Fatal(err)
		}
		// A fresh FileStorage on the same dir models a restart.
		s2, err := NewFileStorage(dir)
		if err != nil {
			t.Fatal(err)
		}
		got, err := s2.Load()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
}

func TestStorageLoadRejectsBadFile(t *testing.T) {
	valid := encodeHardState(HardState{Term: 7, VotedFor: 2})
	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"empty file", func(b []byte) []byte { return b[:0] }},
		{"truncated", func(b []byte) []byte { return b[:stateSize-1] }},
		{"trailing byte", func(b []byte) []byte { return append(b, 0) }},
		{"bad magic", func(b []byte) []byte { b[0] ^= 0xFF; fixCRC(b); return b }},
		{"bad version", func(b []byte) []byte { b[5] = 2; fixCRC(b); return b }},
		{"reserved not zero", func(b []byte) []byte { b[7] = 1; fixCRC(b); return b }},
		{"term flipped, CRC stale", func(b []byte) []byte { b[15] ^= 0x01; return b }},
		{"votedFor flipped, CRC stale", func(b []byte) []byte { b[23] ^= 0x01; return b }},
		{"CRC itself corrupted", func(b []byte) []byte { b[27] ^= 0x01; return b }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, dir := newTestStorage(t)
			b := tc.mutate(append([]byte(nil), valid...))
			if err := os.WriteFile(filepath.Join(dir, StateFile), b, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := s.Load()
			if !errors.Is(err, ErrBadState) {
				t.Fatalf("got %v, want ErrBadState (no fallback to fresh state)", err)
			}
		})
	}
}

func TestStorageStaleTmpIsRemovedAndRealFileKept(t *testing.T) {
	s, dir := newTestStorage(t)
	want := HardState{Term: 3, VotedFor: 1}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash during a later Save: garbage tmp next to the real file.
	tmp := filepath.Join(dir, StateTmpFile)
	if err := os.WriteFile(tmp, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale tmp should be gone, stat err = %v", err)
	}
	got, err := s2.Load()
	if err != nil || got != want {
		t.Fatalf("got %+v, %v; want %+v", got, err, want)
	}
}

func TestStorageSaveLeavesNoTmpFile(t *testing.T) {
	s, dir := newTestStorage(t)
	if err := s.Save(HardState{Term: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, StateTmpFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp file left behind, stat err = %v", err)
	}
}

func TestEncodeHardStateLayout(t *testing.T) {
	b := encodeHardState(HardState{Term: 0x0102030405060708, VotedFor: 9})
	if len(b) != stateSize {
		t.Fatalf("len = %d, want %d", len(b), stateSize)
	}
	if string(b[0:4]) != "RKVH" || binary.BigEndian.Uint16(b[4:6]) != 1 || b[6] != 0 || b[7] != 0 {
		t.Fatalf("bad header bytes: % x", b[:8])
	}
	if binary.BigEndian.Uint64(b[8:16]) != 0x0102030405060708 || binary.BigEndian.Uint64(b[16:24]) != 9 {
		t.Fatalf("bad term/votedFor bytes: % x", b[8:24])
	}
}
