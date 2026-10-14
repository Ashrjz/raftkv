package server

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"
)

// Run serves until ctx is cancelled, then drains: GracefulStop rejects new
// RPCs and waits for in-flight ones; if that exceeds drainTimeout, it falls
// back to a hard Stop. It returns only after Serve has fully exited, so the
// caller can safely close storage afterwards.
func Run(ctx context.Context, lis net.Listener, srv *grpc.Server, drainTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	select {
	case err := <-serveErr:
		return err // Serve failed on its own
	case <-ctx.Done():
	}

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(drainTimeout):
		srv.Stop() // force; unblocks GracefulStop
		<-stopped
	}
	return <-serveErr
}
