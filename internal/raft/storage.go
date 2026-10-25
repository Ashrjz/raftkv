package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
)

// Hard-state file format (docs/raft.md section 7, v1). Big-endian, CRC32C.
//
//	magic(4) | version(2) | reserved(2) | term(8) | votedFor(8) | crc(4)
const (
	StateFile    = "raft-state"
	StateTmpFile = "raft-state.tmp"

	stateVersion = 0x0001
	stateBodyLen = 24 // everything before the CRC
	stateSize    = stateBodyLen + 4
)

var (
	stateMagic = [4]byte{'R', 'K', 'V', 'H'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)

	ErrBadState = errors.New("raft: invalid hard state file")
)

// HardState is the part of Figure 2's persistent state needed for elections.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

// Storage persists the hard state. Save returns only after the data is durable.
// Load returns the zero HardState if nothing was ever saved.
type Storage interface {
	Load() (HardState, error)
	Save(HardState) error
}

// FileStorage keeps the hard state in one file in dir.
type FileStorage struct {
	dir string
}

// NewFileStorage deletes a stale raft-state.tmp (a crash during Save; the real
// file is intact) and returns a store for dir. dir must already exist.
func NewFileStorage(dir string) (*FileStorage, error) {
	err := os.Remove(filepath.Join(dir, StateTmpFile))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

// Load reads the state. A missing file is a fresh node. Any other problem
// (short, long, bad magic/version/CRC) is ErrBadState and must fail startup:
// falling back to a fresh state could let the node vote twice in one term.
func (s *FileStorage) Load() (HardState, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, StateFile))
	if errors.Is(err, fs.ErrNotExist) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, err
	}
	return decodeHardState(b)
}

// Save atomically replaces the state: write tmp, fsync, rename, fsync dir.
// On error the caller must treat the state as not saved (no vote, no reply
// that claims it).
func (s *FileStorage) Save(hs HardState) error {
	tmpPath := filepath.Join(s.dir, StateTmpFile)
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if _, err := f.Write(encodeHardState(hs)); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(s.dir, StateFile)); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(s.dir)
}

func encodeHardState(hs HardState) []byte {
	b := make([]byte, stateSize)
	copy(b[0:4], stateMagic[:])
	binary.BigEndian.PutUint16(b[4:6], stateVersion)
	// b[6:8] reserved, zero
	binary.BigEndian.PutUint64(b[8:16], hs.Term)
	binary.BigEndian.PutUint64(b[16:24], uint64(hs.VotedFor))
	binary.BigEndian.PutUint32(b[24:28], crc32.Checksum(b[:stateBodyLen], castagnoli))
	return b
}

func decodeHardState(b []byte) (HardState, error) {
	bad := func(format string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrBadState, fmt.Sprintf(format, a...))
	}
	if len(b) != stateSize {
		return HardState{}, bad("size %d, want %d", len(b), stateSize)
	}
	if !bytes.Equal(b[0:4], stateMagic[:]) {
		return HardState{}, bad("bad magic")
	}
	if binary.BigEndian.Uint16(b[4:6]) != stateVersion {
		return HardState{}, bad("unsupported version")
	}
	if b[6] != 0 || b[7] != 0 {
		return HardState{}, bad("reserved bytes not zero")
	}
	if binary.BigEndian.Uint32(b[24:28]) != crc32.Checksum(b[:stateBodyLen], castagnoli) {
		return HardState{}, bad("checksum mismatch")
	}
	return HardState{
		Term:     binary.BigEndian.Uint64(b[8:16]),
		VotedFor: NodeID(binary.BigEndian.Uint64(b[16:24])),
	}, nil
}

// syncDir fsyncs a directory so a rename inside it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
