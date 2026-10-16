package server

import (
	"context"

	kvpb "github.com/Ashrjz/raftkv/proto"
)

// Snapshotter is the one thing the Admin service needs; *store.Engine
// satisfies it.
type Snapshotter interface {
	Snapshot() error
}

type AdminServer struct {
	kvpb.UnimplementedAdminServer
	snap Snapshotter
}

func NewAdmin(s Snapshotter) *AdminServer { return &AdminServer{snap: s} }

// Snapshot runs synchronously. Concurrent calls are serialized by the
// engine's snapMu. The engine ignores ctx, so a client deadline stops the
// client waiting but does not abort a snapshot already running. If the
// snapshot is written but WAL truncation fails, the engine reports an error
// and the next Snapshot() retries the truncation.
func (a *AdminServer) Snapshot(ctx context.Context, _ *kvpb.SnapshotRequest) (*kvpb.SnapshotResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, toStatus("snapshot", err)
	}
	if err := a.snap.Snapshot(); err != nil {
		return nil, toStatus("snapshot", err)
	}
	return &kvpb.SnapshotResponse{}, nil
}
