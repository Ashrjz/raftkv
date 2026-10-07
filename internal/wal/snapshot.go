package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Snapshot format constants (docs/snapshot-format.md, v1).
const (
	SnapshotFile    = "snapshot"
	SnapshotTmpFile = "snapshot.tmp"

	snapVersion     = 0x0001
	snapHeaderSize  = 12 // magic(8) | version(2) | reserved(2)
	snapMetaSize    = 16 // walOffset(8) | entryCount(8)
	snapFooterSize  = 4  // crc32c over everything before it
	snapEntryHeader = 8  // keyLen(4) | valueLen(4)
	snapMinSize     = snapHeaderSize + snapMetaSize + snapFooterSize
)

var snapMagic = [8]byte{'R', 'K', 'V', 'S', 'N', 'A', 'P', 0}

var ErrBadSnapshot = errors.New("wal: invalid snapshot")

// WriteSnapshot atomically writes data and walOffset as the current snapshot
// in dir: write tmp, fsync, rename, fsync dir.
//
// The caller must hold the engine lock for the whole call so that data and
// walOffset (from WAL.LogicalEnd) describe the same instant, and must not
// mutate data while this runs. data is only read.
//
// If an error is returned after the rename (directory fsync failed), the new
// snapshot may or may not be durable. The caller must treat the snapshot as
// not taken, and in particular must not truncate the WAL.
func WriteSnapshot(dir string, data map[string][]byte, walOffset uint64) error {
	tmpPath := filepath.Join(dir, SnapshotTmpFile)
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	fail := func(err error) error {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := writeSnapshotBody(f, data, walOffset); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, SnapshotFile)); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(dir)
}

func writeSnapshotBody(f *os.File, data map[string][]byte, walOffset uint64) error {
	h := crc32.New(castagnoli)
	bw := bufio.NewWriterSize(io.MultiWriter(f, h), 64*1024) // everything here is checksummed

	var hdr [snapHeaderSize + snapMetaSize]byte
	copy(hdr[0:8], snapMagic[:])
	binary.BigEndian.PutUint16(hdr[8:10], snapVersion)
	// hdr[10:12] reserved, zero
	binary.BigEndian.PutUint64(hdr[12:20], walOffset)
	binary.BigEndian.PutUint64(hdr[20:28], uint64(len(data)))
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}

	var eh [snapEntryHeader]byte
	for k, v := range data {
		if len(k) == 0 {
			return ErrEmptyKey
		}
		if len(k) > MaxKeySize || len(v) > MaxValueSize {
			return ErrTooLarge
		}
		binary.BigEndian.PutUint32(eh[0:4], uint32(len(k)))
		binary.BigEndian.PutUint32(eh[4:8], uint32(len(v)))
		if _, err := bw.Write(eh[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(k); err != nil {
			return err
		}
		if _, err := bw.Write(v); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	// Footer goes to the file only, not through the hash.
	var foot [snapFooterSize]byte
	binary.BigEndian.PutUint32(foot[:], h.Sum32())
	_, err := f.Write(foot[:])
	return err
}

// LoadSnapshot reads and fully validates the snapshot in dir.
//
// Returns (nil, 0, false, nil) if no snapshot exists. Any validation failure
// returns an error wrapping ErrBadSnapshot; callers must NOT fall back to a
// full WAL replay (the WAL may already be truncated). The returned map is
// built privately and only handed back after the checksum verifies.
func LoadSnapshot(dir string) (map[string][]byte, uint64, bool, error) {
	f, err := os.Open(filepath.Join(dir, SnapshotFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, 0, false, err
	}
	size := st.Size()

	bad := func(format string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrBadSnapshot, fmt.Sprintf(format, a...))
	}
	if size < snapMinSize {
		return nil, 0, false, bad("file too short (%d bytes)", size)
	}

	// Body = everything except the footer. The tee feeds the checksum.
	h := crc32.New(castagnoli)
	body := bufio.NewReaderSize(io.LimitReader(f, size-snapFooterSize), 64*1024)
	tr := io.TeeReader(body, h)

	readFull := func(buf []byte) error {
		if _, err := io.ReadFull(tr, buf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return bad("truncated")
			}
			return err
		}
		return nil
	}

	var hdr [snapHeaderSize + snapMetaSize]byte
	if err := readFull(hdr[:]); err != nil {
		return nil, 0, false, err
	}
	if !bytes.Equal(hdr[0:8], snapMagic[:]) {
		return nil, 0, false, bad("bad magic")
	}
	if binary.BigEndian.Uint16(hdr[8:10]) != snapVersion {
		return nil, 0, false, bad("unsupported version")
	}
	if hdr[10] != 0 || hdr[11] != 0 {
		return nil, 0, false, bad("reserved bytes not zero")
	}
	walOffset := binary.BigEndian.Uint64(hdr[12:20])
	count := binary.BigEndian.Uint64(hdr[20:28])

	// Bytes of entry data not yet consumed. Used to reject impossible counts
	// and lengths BEFORE allocating anything based on them.
	remaining := size - snapFooterSize - int64(len(hdr))
	if count > uint64(remaining)/(snapEntryHeader+1) {
		return nil, 0, false, bad("entry count %d impossible for %d bytes", count, remaining)
	}

	m := make(map[string][]byte, count)
	var eh [snapEntryHeader]byte
	var keyBuf [MaxKeySize]byte
	for i := uint64(0); i < count; i++ {
		if err := readFull(eh[:]); err != nil {
			return nil, 0, false, err
		}
		kl := binary.BigEndian.Uint32(eh[0:4])
		vl := binary.BigEndian.Uint32(eh[4:8])
		if kl == 0 || kl > MaxKeySize {
			return nil, 0, false, bad("entry %d: bad key length %d", i, kl)
		}
		if vl > MaxValueSize {
			return nil, 0, false, bad("entry %d: bad value length %d", i, vl)
		}
		need := int64(snapEntryHeader) + int64(kl) + int64(vl)
		if need > remaining {
			return nil, 0, false, bad("entry %d: exceeds remaining file", i)
		}
		remaining -= need

		if err := readFull(keyBuf[:kl]); err != nil {
			return nil, 0, false, err
		}
		key := string(keyBuf[:kl])
		if _, dup := m[key]; dup {
			return nil, 0, false, bad("entry %d: duplicate key", i)
		}
		// Empty value stays nil, matching what DecodeRecord yields on WAL replay.
		var val []byte
		if vl > 0 {
			val = make([]byte, vl)
			if err := readFull(val); err != nil {
				return nil, 0, false, err
			}
		}
		m[key] = val
	}
	if remaining != 0 {
		return nil, 0, false, bad("%d unexpected bytes after last entry", remaining)
	}

	var foot [snapFooterSize]byte
	if _, err := f.ReadAt(foot[:], size-snapFooterSize); err != nil {
		return nil, 0, false, err
	}
	if binary.BigEndian.Uint32(foot[:]) != h.Sum32() {
		return nil, 0, false, bad("checksum mismatch")
	}
	return m, walOffset, true, nil
}

// RemoveStaleSnapshotTmp deletes a leftover snapshot.tmp from a crash during
// WriteSnapshot. Call it on startup before loading.
func RemoveStaleSnapshotTmp(dir string) error {
	err := os.Remove(filepath.Join(dir, SnapshotTmpFile))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
