package store

import "errors"

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("store: key not found")

// Store is the storage contract. Later, the WAL-backed engine and the Raft
// state machine both sit behind this same shape.
type Store interface {
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Delete(key string) error
}
