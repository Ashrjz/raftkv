package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Ashrjz/raftkv/internal/store"
	kvpb "github.com/Ashrjz/raftkv/proto"
)

// Store is what the server needs from storage; store.Store satisfies it.
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

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if err := validate(ctx, req.Key); err != nil {
		return nil, err
	}
	v, err := s.store.Get(req.Key)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// Absence is a normal outcome, reported via the flag, not a gRPC error.
		return &kvpb.GetResponse{Found: false}, nil
	case err != nil:
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &kvpb.GetResponse{Value: v, Found: true}, nil
}

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := validate(ctx, req.Key); err != nil {
		return nil, err
	}
	if err := s.store.Put(req.Key, req.Value); err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &kvpb.PutResponse{}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := validate(ctx, req.Key); err != nil {
		return nil, err
	}
	if err := s.store.Delete(req.Key); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &kvpb.DeleteResponse{}, nil
}

// validate rejects empty keys and requests whose caller already gave up,
// so we never start a write nobody is waiting for.
func validate(ctx context.Context, key string) error {
	if key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}
