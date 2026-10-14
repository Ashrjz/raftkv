package server

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LimitInterceptor allows at most maxInflight concurrent handlers. A request
// waits for a slot for at most maxWait (or until its own deadline/cancel,
// whichever is first), then is shed with ResourceExhausted.
// maxWait bounds the wait even for clients that set no deadline.
func LimitInterceptor(maxInflight int, maxWait time.Duration) grpc.UnaryServerInterceptor {
	sem := make(chan struct{}, maxInflight)

	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (any, error) {

		waitCtx, cancel := context.WithTimeout(ctx, maxWait)
		defer cancel()

		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			return handler(ctx, req)
		case <-waitCtx.Done():
			return nil, status.Error(codes.ResourceExhausted, "server overloaded, retry later")
		}
	}
}
