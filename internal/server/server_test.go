package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"time"

	"github.com/Ashrjz/raftkv/internal/store"
	"github.com/Ashrjz/raftkv/internal/wal"
	kvpb "github.com/Ashrjz/raftkv/proto"
)

type fakeStore struct {
	mu    sync.Mutex
	m     map[string][]byte
	calls int   // calls that reached the store
	err   error // returned by Put/Delete when set
}

func newFake() *fakeStore { return &fakeStore{m: map[string][]byte{}} }

func (f *fakeStore) Get(k string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	v, ok := f.m[k]
	if !ok {
		return nil, store.ErrNotFound
	}
	return v, nil
}

func (f *fakeStore) Put(k string, v []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.m[k] = v
	return nil
}

func (f *fakeStore) Delete(k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	delete(f.m, k)
	return nil
}

func TestToStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"not found", fmt.Errorf("x: %w", store.ErrNotFound), codes.NotFound},
		{"empty key", fmt.Errorf("engine store put failed: %w", wal.ErrEmptyKey), codes.InvalidArgument},
		{"too large", fmt.Errorf("engine store put failed: %w", wal.ErrTooLarge), codes.InvalidArgument},
		{"closed", wal.ErrClosed, codes.Unavailable},
		{"broken", fmt.Errorf("%w: sync: boom", wal.ErrBroken), codes.Internal},
		{"invalid record", wal.ErrInvalid, codes.Internal},
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"other", errors.New("boom"), codes.Internal},
	}
	for _, tc := range tests {
		if got := status.Code(toStatus("test", tc.err)); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
	if toStatus("test", nil) != nil {
		t.Error("nil error must stay nil")
	}
}

func TestPutValidation(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  int
		want codes.Code
	}{
		{"ok", "k", 1, codes.OK},
		{"max key", strings.Repeat("k", wal.MaxKeySize), 0, codes.OK},
		{"key too long", strings.Repeat("k", wal.MaxKeySize+1), 0, codes.InvalidArgument},
		{"empty key", "", 0, codes.InvalidArgument},
		{"max value", "k", wal.MaxValueSize, codes.OK},
		{"value too large", "k", wal.MaxValueSize + 1, codes.InvalidArgument},
	}
	for _, tc := range tests {
		f := newFake()
		_, err := New(f).Put(context.Background(),
			&kvpb.PutRequest{Key: tc.key, Value: make([]byte, tc.val)})
		if got := status.Code(err); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
		if tc.want != codes.OK && f.calls != 0 {
			t.Errorf("%s: invalid request reached the store", tc.name)
		}
	}
}

func TestDeleteValidation(t *testing.T) {
	for _, key := range []string{"", strings.Repeat("k", wal.MaxKeySize+1)} {
		f := newFake()
		_, err := New(f).Delete(context.Background(), &kvpb.DeleteRequest{Key: key})
		if status.Code(err) != codes.InvalidArgument || f.calls != 0 {
			t.Errorf("key len %d: code=%v calls=%d", len(key), status.Code(err), f.calls)
		}
	}
}

func TestRoundTripAndNotFound(t *testing.T) {
	s, ctx := New(newFake()), context.Background()

	if _, err := s.Get(ctx, &kvpb.GetRequest{Key: "a"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing key: got %v", err)
	}
	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "a", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Get(ctx, &kvpb.GetRequest{Key: "a"}); err != nil || string(r.Value) != "1" {
		t.Fatalf("got %v, %v", r, err)
	}
	for i := 0; i < 2; i++ { // second delete: idempotent
		if _, err := s.Delete(ctx, &kvpb.DeleteRequest{Key: "a"}); err != nil {
			t.Fatalf("delete #%d: %v", i+1, err)
		}
	}
	if _, err := s.Get(ctx, &kvpb.GetRequest{Key: "a"}); status.Code(err) != codes.NotFound {
		t.Fatalf("after delete: got %v", err)
	}
	// Get passes any key through (engine parity): an empty key is just NotFound.
	if _, err := s.Get(ctx, &kvpb.GetRequest{}); status.Code(err) != codes.NotFound {
		t.Fatalf("empty key Get: got %v", err)
	}
}

func TestStoreErrorMapping(t *testing.T) {
	f := newFake()
	s, ctx := New(f), context.Background()

	f.err = fmt.Errorf("%w: sync: boom", wal.ErrBroken)
	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "k"}); status.Code(err) != codes.Internal {
		t.Errorf("broken WAL: got %v", err)
	}
	f.err = wal.ErrTooLarge // backstop: engine rejects what the pre-check missed
	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "k"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("engine too-large: got %v", err)
	}
}

func TestCancelledContext(t *testing.T) {
	f := newFake()
	s := New(f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: "k"}); status.Code(err) != codes.Canceled {
		t.Errorf("put: got %v", err)
	}
	if _, err := s.Get(ctx, &kvpb.GetRequest{Key: "k"}); status.Code(err) != codes.Canceled {
		t.Errorf("get: got %v", err)
	}
	if f.calls != 0 {
		t.Error("cancelled request reached the store")
	}
}

type snapFunc func() error

func (f snapFunc) Snapshot() error { return f() }

func TestAdminSnapshot(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"ok", nil, codes.OK},
		{"failed", errors.New("disk full"), codes.Internal},
		{"truncation failed", fmt.Errorf("snapshot written but WAL truncation failed: %w", wal.ErrBroken), codes.Internal},
	}
	for _, tc := range tests {
		a := NewAdmin(snapFunc(func() error { return tc.err }))
		_, err := a.Snapshot(context.Background(), &kvpb.SnapshotRequest{})
		if got := status.Code(err); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestLimitInterceptor_ShedsLoad(t *testing.T) {
	ic := LimitInterceptor(1, 50*time.Millisecond)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h := func(ctx context.Context, req any) (any, error) {
		close(entered) // only the first call gets in; the second is shed
		<-release
		return nil, nil
	}
	go func() {
		ic(context.Background(), nil, &grpc.UnaryServerInfo{}, h)
		close(done)
	}()
	<-entered // first request now holds the only slot

	if _, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{}, h); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("want ResourceExhausted, got %v", err)
	}
	close(release)
	<-done
}
