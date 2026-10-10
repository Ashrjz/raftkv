package store

import "sync"

type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string][]byte)}
}

func (s *MemStore) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return clone(v), nil // copy out: don't leak internal memory
}

func (s *MemStore) Put(key string, value []byte) error {
	v := clone(value) // copy in, outside the lock (keep the critical section short)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = v
	return nil
}

func (s *MemStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key) // deleting a missing key is a no-op
	return nil
}

// clone returns a copy of the map taken under the read lock. Values are
// shared with the live map, which is safe because Put stores a private copy
// of each value and Get returns a copy: a stored slice is never mutated or
// handed out, only replaced or removed.
func (m *MemStore) clone() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		c[k] = v
	}
	return c
}

// withData runs fn with the underlying map under a read lock.
// fn must not modify or retain the map.
func (m *MemStore) withData(fn func(map[string][]byte) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fn(m.data)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
