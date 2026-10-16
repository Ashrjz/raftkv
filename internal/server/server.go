package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Ashrjz/raftkv/internal/wal"
	kvpb "github.com/Ashrjz/raftkv/proto"
)

// Store is what the server needs from storage. It has the same shape as
// store.Store, so *store.Engine satisfies it directly.
type Store interface {
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Delete(key string) error
}

type Server struct {
	kvpb.UnimplementedKVServer
	store Store
}

func New(s Store) *Server { return &Server{store: s} }

// Get passes the key straight through, matching the engine: an empty or
// oversized key cannot exist, so the engine answers ErrNotFound -> NotFound.
func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, toStatus("get", err)
	}
	v, err := s.store.Get(req.Key)
	if err != nil {
		return nil, toStatus("get", err)
	}
	return &kvpb.GetResponse{Value: v}, nil
}

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := checkKey(ctx, req.Key); err != nil {
		return nil, err
	}
	if len(req.Value) > wal.MaxValueSize {
		return nil, status.Errorf(codes.InvalidArgument,
			"value is %d bytes, limit is %d", len(req.Value), wal.MaxValueSize)
	}
	if err := s.store.Put(req.Key, req.Value); err != nil {
		return nil, toStatus("put", err)
	}
	return &kvpb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := checkKey(ctx, req.Key); err != nil {
		return nil, err
	}
	if err := s.store.Delete(req.Key); err != nil {
		return nil, toStatus("delete", err)
	}
	return &kvpb.DeleteResponse{}, nil
}

// checkKey runs before a write reaches the engine. The engine would also
// reject these (wal.ErrEmptyKey / ErrTooLarge, mapped by toStatus as a
// backstop), but it only validates after taking its global write mutex, which
// is held across fsyncs. Checking here keeps invalid requests from queueing
// behind other writers just to be told no. It also skips writes whose caller
// has already given up.
func checkKey(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return toStatus("write", err)
	}
	if key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(key) > wal.MaxKeySize {
		return status.Errorf(codes.InvalidArgument,
			"key is %d bytes, limit is %d", len(key), wal.MaxKeySize)
	}
	return nil
}
