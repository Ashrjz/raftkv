package wal

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrBroken    = errors.New("wal: broken after failed write/sync; restart required")
	ErrBadOffset = errors.New("wal: replay offset out of range")
)

type WAL struct {
	mu         sync.Mutex
	f          *os.File
	broken     error
	baseOffset uint64 // logical offset of the first record in the file (from header)
	logicalEnd uint64 // logical offset just past the last durable record
}

type RecoveryResult struct {
	Records        int   // records applied
	GoodOffset     int64 // file is valid up to here
	DiscardedBytes int64 // torn tail removed (0 if clean)
	DiscardReason  string
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
		if _, err := f.Write(encodeHeader(0)); err != nil { // from your spec
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		if err := syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, err
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
	w.logicalEnd += uint64(len(buf))
	return nil
}

// Recover replays records at logical offset >= from through fn and repairs a
// torn tail. Pass 0 for a full replay, or the snapshot's WALOffset.
// `from` must be a record boundary (callers only pass offsets captured at
// one). It must complete before the first Append.
func (w *WAL) Recover(from uint64, fn func(Record) error) (RecoveryResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken != nil {
		return RecoveryResult{}, w.broken
	}

	st, err := w.f.Stat()
	if err != nil {
		return RecoveryResult{}, err
	}
	size := st.Size()

	// A header that is a strict prefix of the expected one means we crashed
	// while creating the file. Nothing could have been acknowledged.
	if size < HeaderSize {
		if from != 0 { // a snapshot cannot cover records in a WAL that has none
			return RecoveryResult{}, fmt.Errorf("%w: from=%d but WAL has no records", ErrBadOffset, from)
		}
		existing := make([]byte, size)
		if _, err := w.f.ReadAt(existing, 0); err != nil && err != io.EOF {
			return RecoveryResult{}, err
		}
		if !bytes.Equal(existing, encodeHeader(0)[:size]) {
			return RecoveryResult{}, fmt.Errorf("%w: short file is not a header prefix", ErrBadHeader)
		}
		if err := w.f.Truncate(0); err != nil {
			return RecoveryResult{}, err
		}
		if _, err := w.f.Write(encodeHeader(0)); err != nil { // O_APPEND: lands at 0
			return RecoveryResult{}, err
		}
		if err := w.f.Sync(); err != nil {
			return RecoveryResult{}, err
		}
		w.baseOffset = 0
		w.setLogicalEnd(HeaderSize)
		return RecoveryResult{GoodOffset: HeaderSize, DiscardedBytes: size,
			DiscardReason: "torn header"}, nil
	}

	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(io.NewSectionReader(w.f, 0, HeaderSize), hdr[:]); err != nil {
		return RecoveryResult{}, fmt.Errorf("recover: read header: %w", err)
	}
	base, err := validateHeader(hdr[:])
	if err != nil {
		return RecoveryResult{}, err
	}
	w.baseOffset = base

	// Range check, including any torn tail still in the file. from < base
	// would underflow below, so it is checked first.
	fileEnd := base + uint64(size-HeaderSize)
	if from < base || from > fileEnd {
		return RecoveryResult{}, fmt.Errorf("%w: from=%d, WAL covers [%d, %d]",
			ErrBadOffset, from, base, fileEnd)
	}

	start := int64(HeaderSize) + int64(from-base) // safe: from-base <= size
	if _, err := w.f.Seek(start, io.SeekStart); err != nil {
		return RecoveryResult{}, err
	}
	br := bufio.NewReaderSize(w.f, 64*1024) // batches syscalls only

	res := RecoveryResult{GoodOffset: start}
	for {
		rec, n, err := ReadRecord(br)
		if err == io.EOF {
			w.setLogicalEnd(res.GoodOffset)
			return res, nil // clean end at a record boundary
		}
		if err != nil {
			torn, reason, cerr := w.isTornTail(err, res.GoodOffset, int64(n), size)
			if cerr != nil {
				return res, cerr
			}
			if !torn {
				return res, fmt.Errorf("recover: corruption at offset %d (not a torn tail): %w",
					res.GoodOffset, err)
			}
			// Discard the torn record durably BEFORE accepting any new append.
			if err := w.f.Truncate(res.GoodOffset); err != nil {
				return res, fmt.Errorf("recover: truncate: %w", err)
			}
			if err := w.f.Sync(); err != nil {
				return res, fmt.Errorf("recover: sync after truncate: %w", err)
			}
			res.DiscardedBytes = size - res.GoodOffset
			res.DiscardReason = reason
			w.setLogicalEnd(res.GoodOffset)
			return res, nil
		}
		if err := fn(rec); err != nil {
			return res, fmt.Errorf("recover: apply at offset %d: %w", res.GoodOffset, err)
		}
		res.Records++
		res.GoodOffset += int64(n)
	}
}

// isTornTail implements the table above. off is where the failed record
// starts, consumed is how many bytes ReadRecord took from the stream.
func (w *WAL) isTornTail(err error, off, consumed, size int64) (bool, string, error) {
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return true, "file ends mid-record", nil
	case errors.Is(err, ErrChecksum):
		if off+consumed == size {
			return true, "last record fails checksum", nil
		}
		return false, "", nil // more data follows: damage, not a torn write
	case errors.Is(err, ErrBadLength):
		zero, zerr := w.allZeroFrom(off, size)
		if zerr != nil {
			return false, "", zerr
		}
		if zero {
			return true, "zero-filled tail", nil
		}
		return false, "", nil
	default: // ErrBadType, ErrMalformed, read errors: never a crash artifact
		return false, "", nil
	}
}

func (w *WAL) allZeroFrom(off, size int64) (bool, error) {
	buf := make([]byte, 64*1024)
	for off < size {
		n := int64(len(buf))
		if size-off < n {
			n = size - off
		}
		m, err := w.f.ReadAt(buf[:n], off)
		if err != nil && err != io.EOF {
			return false, err
		}
		if m == 0 {
			return false, io.ErrUnexpectedEOF
		}
		for _, b := range buf[:m] {
			if b != 0 {
				return false, nil
			}
		}
		off += int64(m)
	}
	return true, nil
}

// LogicalEnd returns the logical offset just past the last durable record.
// Valid for a new WAL immediately, and for an existing one after Recover.
func (w *WAL) LogicalEnd() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.logicalEnd
}

func (w *WAL) setLogicalEnd(goodPhysical int64) {
	w.logicalEnd = w.baseOffset + uint64(goodPhysical-HeaderSize)
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
