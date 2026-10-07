package store

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/Ashrjz/raftkv/internal/wal"
)

// Engine wraps an in-memory Store with a WAL for write-ahead durability.
type Engine struct {
	mu       sync.Mutex // serializes writes (WAL append + apply) and snapshots
	mem      *MemStore
	wal      *wal.WAL
	dir      string // directory holding the WAL and snapshot
	recovery wal.RecoveryResult
}

// NewEngine recovers state as: load snapshot (if any), then replay the WAL
// from the snapshot's offset.
func NewEngine(walPath string) (*Engine, error) {
	dir := filepath.Dir(walPath)

	if err := wal.RemoveStaleSnapshotTmp(dir); err != nil {
		return nil, err
	}

	mem := NewMemStore()

	// A bad snapshot fails startup; never fall back to a full replay.
	snap, from, found, err := wal.LoadSnapshot(dir)
	if err != nil {
		return nil, err
	}
	if found {
		for k, v := range snap {
			if err := mem.Put(k, v); err != nil {
				return nil, err
			}
		}
	}

	w, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}

	res, err := w.Recover(from, func(rec wal.Record) error {
		switch rec.Type {
		case wal.TypePut:
			return mem.Put(string(rec.Key), rec.Value)
		case wal.TypeDelete:
			return mem.Delete(string(rec.Key))
		default:
			return fmt.Errorf("unknown record type %d", rec.Type)
		}
	})
	if err != nil {
		w.Close()
		return nil, err
	}

	return &Engine{mem: mem, wal: w, dir: dir, recovery: res}, nil
}

// Get reads directly from the in-memory state (fast read path).
func (e *Engine) Get(key string) ([]byte, error) {
	return e.mem.Get(key)
}

// Put writes the operation to the WAL (with fsync) before updating memory.
func (e *Engine) Put(key string, value []byte) error {
	rec := wal.Record{
		Type:  wal.TypePut,
		Key:   []byte(key),
		Value: value,
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Write-Ahead Log: append and fsync to disk FIRST
	if err := e.wal.Append(rec); err != nil {
		return fmt.Errorf("engine store put failed: %w", err)
	}

	// 2. Update in-memory map once durable on disk
	return e.mem.Put(key, value)
}

// Delete writes a delete tombstone to the WAL before updating memory.
func (e *Engine) Delete(key string) error {
	rec := wal.Record{
		Type: wal.TypeDelete,
		Key:  []byte(key),
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Append delete record to WAL
	if err := e.wal.Append(rec); err != nil {
		return fmt.Errorf("engine store delete failed: %w", err)
	}

	// 2. Delete key from in-memory map
	return e.mem.Delete(key)
}

// Snapshot writes the current state to disk. Writes are blocked for the
// duration (reads are not); the map and the WAL offset are captured together.
// The WAL is not truncated here.
func (e *Engine) Snapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.mem.withData(func(data map[string][]byte) error {
		return wal.WriteSnapshot(e.dir, data, e.wal.LogicalEnd())
	})
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.Close()
}
