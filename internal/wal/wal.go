package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var ErrBroken = errors.New("wal: broken after failed write/sync; restart required")

type WAL struct {
	mu     sync.Mutex
	f      *os.File
	broken error
}

// Open creates a new WAL (header + fsync + dir fsync) or opens an existing one.
// NOTE: for an existing non-empty file, run replay + torn-tail truncation
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		if _, err := f.Write(encodeHeader()); err != nil { // from your spec
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		if err := syncDir(filepath.Dir(path)); err != nil {
			// Silently ignore directory sync errors on Windows
			if runtime.GOOS != "windows" {
				f.Close()
				return nil, err
			}
		}
	}
	return &WAL{f: f}, nil
}

// Append makes the record durable before returning nil.
func (w *WAL) Append(rec Record) error {
	buf, err := encodeRecord(rec) // enforces your size limits, one contiguous buffer
	if err != nil {
		return err // nothing written; WAL still healthy
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken != nil {
		return w.broken
	}
	if _, err := w.f.Write(buf); err != nil {
		w.broken = fmt.Errorf("%w: write: %v", ErrBroken, err)
		return w.broken
	}
	if err := w.f.Sync(); err != nil {
		w.broken = fmt.Errorf("%w: sync: %v", ErrBroken, err)
		return w.broken
	}
	return nil
}

// Returns the number of records successfully decoded.
func (w *WAL) Replay(fn func(Record) error) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.broken != nil {
		return 0, w.broken
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	// 64 KiB here only batches syscalls; it does not limit record size.
	br := bufio.NewReaderSize(w.f, 64*1024)

	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return 0, fmt.Errorf("replay: read header: %w", err)
	}
	// TODO: validate magic/version/header CRC against your spec.

	count := 0
	offset := int64(HeaderSize)
	for {
		rec, n, err := ReadRecord(br)
		if err == io.EOF {
			return count, nil // clean end at a record boundary
		}
		if err != nil {
			// Torn tail or corruption. Task 5 will decide what to do;
			// until then, fail loudly with the offset instead of silently
			// dropping data.
			return count, fmt.Errorf("replay: at offset %d: %w", offset, err)
		}
		if err := fn(rec); err != nil {
			return count, fmt.Errorf("replay: apply at offset %d: %w", offset, err)
		}
		count++
		offset += int64(n)
	}
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
