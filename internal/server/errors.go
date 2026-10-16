package server

import (
	"context"
	"errors"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Ashrjz/raftkv/internal/store"
	"github.com/Ashrjz/raftkv/internal/wal"
)

// toStatus converts an error from storage (or a context) into a gRPC status.
// op names the RPC for the log. Unexpected errors are logged in full and
// returned to the client with a generic message, so internal details
// (file paths, etc.) do not leak.
func toStatus(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "key not found")
	case errors.Is(err, wal.ErrEmptyKey):
		return status.Error(codes.InvalidArgument, "key must not be empty")
	case errors.Is(err, wal.ErrTooLarge):
		return status.Error(codes.InvalidArgument, "key or value exceeds size limit")
	case errors.Is(err, wal.ErrClosed):
		return status.Error(codes.Unavailable, "storage is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, wal.ErrBroken):
		// "restart required": retrying cannot help until the process restarts.
		log.Printf("server: %s: %v", op, err)
		return status.Error(codes.Internal, "storage engine is broken; restart required")
	default:
		log.Printf("server: %s: %v", op, err)
		return status.Error(codes.Internal, "internal error")
	}
}
