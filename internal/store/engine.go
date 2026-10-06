package store

import (
	"fmt"

	"github.com/Ashrjz/raftkv/internal/wal"
)

// Engine wraps an in-memory Store with a WAL for write-ahead durability.
type Engine struct {
	mem      *MemStore
	wal      *wal.WAL
	recovery wal.RecoveryResult
}

// NewEngine initializes a Engine from an open WAL and an empty MemStore.
func NewEngine(walPath string) (*Engine, error) {
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}

	mem := NewMemStore()

	// Replay the WAL to reconstruct state
	res, err := w.Recover(0, func(rec wal.Record) error {
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

	return &Engine{mem: mem, wal: w, recovery: res}, nil
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

	// 1. Append delete record to WAL
	if err := e.wal.Append(rec); err != nil {
		return fmt.Errorf("engine store delete failed: %w", err)
	}

	// 2. Delete key from in-memory map
	return e.mem.Delete(key)
}

func (e *Engine) Close() error {
	return e.wal.Close()
}
