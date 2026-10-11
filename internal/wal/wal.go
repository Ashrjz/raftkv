package wal

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	ErrBroken    = errors.New("wal: broken after failed write/sync; restart required")
	ErrBadOffset = errors.New("wal: replay offset out of range")
	ErrClosed    = errors.New("wal: closed")
)

const (
	walTmpSuffix = ".tmp"
	maxBatch     = 256 // maxBatch caps how many queued records one fsync will cover.
)

type Option func(*WAL)

// WithGroupCommit makes concurrent Appends share fsyncs: while one fsync runs,
// other callers queue up, and the next fsync makes the whole queue durable.
// Every Append still returns only after its own record is durable.
func WithGroupCommit() Option { return func(w *WAL) { w.group = true } }

type commitReq struct {
	buf  []byte
	done chan error
}

type WAL struct {
	mu         sync.Mutex
	f          *os.File
	path       string // needed to rewrite the file during truncation
	broken     error
	baseOffset uint64 // logical offset of the first record in the file (from header)
	logicalEnd uint64 // logical offset just past the last durable record

	// group commit (only used when group is true)
	group     bool
	reqs      chan commitReq // unbuffered: a successful send means the committer owns the request
	quit      chan struct{}  // w.Close() closes this channel
	stopped   chan struct{}  //
	closeOnce sync.Once

	// fsync statistics, for benchmarks and later metrics
	batches atomic.Uint64
	records atomic.Uint64
}

type RecoveryResult struct {
	Records        int   // records applied
	GoodOffset     int64 // file is valid up to here
	DiscardedBytes int64 // torn tail removed (0 if clean)
	DiscardReason  string
}

// Open creates a new WAL (header + fsync + dir fsync) or opens an existing one.
// NOTE: for an existing non-empty file, run replay + torn-tail truncation
func Open(path string, opts ...Option) (*WAL, error) {
	// A leftover tmp file comes from a truncation that crashed before its
	// rename. The real WAL is intact, so the tmp file is just garbage.
	if err := os.Remove(path + walTmpSuffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

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

	w := &WAL{f: f, path: path}
	for _, o := range opts {
		o(w)
	}
	if w.group {
		w.reqs = make(chan commitReq)
		w.quit = make(chan struct{})
		w.stopped = make(chan struct{})
		go w.committer()
	}
	return w, nil
}

// Append makes the record durable before returning nil.
func (w *WAL) Append(rec Record) error {
	buf, err := encodeRecord(rec) // enforces your size limits, one contiguous buffer
	if err != nil {
		return err // nothing written; WAL still healthy
	}
	if w.group {
		return w.appendGrouped(buf)
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
	w.batches.Add(1)
	w.records.Add(1)
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

// TruncateBefore rewrites the WAL so its first record is the one at logical
// offset upTo, dropping everything before it. Logical offsets do not change
// (BaseOffset in the new header becomes upTo), so any offset a snapshot
// stored stays valid.
//
// Preconditions: Recover has completed (or the WAL is new), upTo is a record
// boundary (callers pass a value obtained from LogicalEnd), and
// BaseOffset <= upTo <= LogicalEnd. Call it only after the snapshot covering
// upTo is fully durable (WriteSnapshot returned nil).
//
// Failure before the rename leaves the WAL untouched and usable. Failure of
// the final directory fsync marks the WAL broken: the new file is already
// in use, and acknowledging appends before the rename is durable could lose
// them on a crash.
func (w *WAL) TruncateBefore(upTo uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.broken != nil {
		return w.broken
	}
	if upTo < w.baseOffset || upTo > w.logicalEnd {
		return fmt.Errorf("%w: truncate to %d, WAL covers [%d, %d]",
			ErrBadOffset, upTo, w.baseOffset, w.logicalEnd)
	}
	if upTo == w.baseOffset {
		return nil // nothing to drop
	}

	tmpPath := w.path + walTmpSuffix
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}

	// New file = header(BaseOffset=upTo) + the records from upTo onward.
	if _, err := tmp.Write(encodeHeader(upTo)); err != nil {
		return fail(err)
	}
	start := int64(HeaderSize) + int64(upTo-w.baseOffset)
	tailLen := int64(w.logicalEnd - upTo)
	n, err := io.Copy(tmp, io.NewSectionReader(w.f, start, tailLen))
	if err != nil {
		return fail(err)
	}
	if n != tailLen {
		return fail(fmt.Errorf("wal: truncate copied %d of %d tail bytes", n, tailLen))
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}

	if err := os.Rename(tmpPath, w.path); err != nil {
		return fail(err) // nothing changed; old WAL still in place
	}

	// Point of no return: the path now names the new file. Switch handles
	// first so no append can reach the unlinked old file.
	old := w.f
	w.f = tmp
	w.baseOffset = upTo
	old.Close()

	if err := syncDir(filepath.Dir(w.path)); err != nil {
		w.broken = fmt.Errorf("%w: sync dir after truncate: %v", ErrBroken, err)
		return w.broken
	}
	return nil
}

func (w *WAL) appendGrouped(buf []byte) error {
	req := commitReq{buf: buf, done: make(chan error, 1)}
	select {
	case w.reqs <- req: // the channel is unbuffered, so the committer has it
	case <-w.quit:
		return ErrClosed
	}
	return <-req.done
}

// committer is the only goroutine that writes in group mode. It takes one
// request, then grabs whatever else is already waiting (without blocking) and
// commits all of it with a single write and a single fsync.
func (w *WAL) committer() {
	defer close(w.stopped)
	for {
		var batch []commitReq
		select {
		case r := <-w.reqs:
			batch = append(batch, r)
		case <-w.quit:
			return
		}
	drain:
		for len(batch) < maxBatch {
			select {
			case r := <-w.reqs:
				batch = append(batch, r)
			default:
				break drain
			}
		}
		err := w.commitBatch(batch)
		for _, r := range batch {
			r.done <- err
		}
	}
}

func (w *WAL) commitBatch(batch []commitReq) error {
	total := 0
	for _, r := range batch {
		total += len(r.buf)
	}
	buf := make([]byte, 0, total)
	for _, r := range batch {
		buf = append(buf, r.buf...)
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
	w.logicalEnd += uint64(total)
	w.batches.Add(1)
	w.records.Add(uint64(len(batch)))
	return nil
}

// Stats returns how many fsyncs (batches) have been issued and how many
// records they covered. records/batches is the average batch size.
func (w *WAL) Stats() (batches, records uint64) {
	return w.batches.Load(), w.records.Load()
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
	if w.group {
		w.closeOnce.Do(func() { close(w.quit) })
		<-w.stopped // no batch is in flight once this returns
	}
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
