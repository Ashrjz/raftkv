package client

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Ashrjz/raftkv/internal/server"
	kvpb "github.com/Ashrjz/raftkv/proto"
)

const (
	DefaultTimeout         = 2 * time.Second
	DefaultSnapshotTimeout = 60 * time.Second // a snapshot copies and writes the whole map
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("client: key not found")

type Client struct {
	conn    *grpc.ClientConn
	kv      kvpb.KVClient
	admin   kvpb.AdminClient
	timeout time.Duration
}

func Dial(addr string, timeout time.Duration, opts ...grpc.DialOption) (*Client, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	opts = append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TLS is out of scope for now
		// Large Get responses: gRPC's default 4 MiB is below the engine's value limit.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(server.MaxMessageSize)),
	}, opts...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:    conn,
		kv:      kvpb.NewKVClient(conn),
		admin:   kvpb.NewAdminClient(conn),
		timeout: timeout,
	}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// withTimeout keeps the caller's deadline if present, otherwise applies d.
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// Get returns client.ErrNotFound if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := withTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.kv.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return resp.Value, nil
}

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	ctx, cancel := withTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.kv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
	return err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	ctx, cancel := withTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.kv.Delete(ctx, &kvpb.DeleteRequest{Key: key})
	return err
}

// Snapshot asks the server to snapshot its state and truncate its WAL.
func (c *Client) Snapshot(ctx context.Context) error {
	ctx, cancel := withTimeout(ctx, DefaultSnapshotTimeout)
	defer cancel()
	_, err := c.admin.Snapshot(ctx, &kvpb.SnapshotRequest{})
	return err
}
