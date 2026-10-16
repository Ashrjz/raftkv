package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Ashrjz/raftkv/internal/server"
	"github.com/Ashrjz/raftkv/internal/store"
	kvpb "github.com/Ashrjz/raftkv/proto"
)

func main() {
	var (
		addr        = flag.String("addr", ":7000", "listen address")
		walPath     = flag.String("wal", "./data/raftkv.wal", "WAL file path (the snapshot lives in the same directory)")
		maxInflight = flag.Int("max-inflight", 256, "max concurrent requests")
		maxWait     = flag.Duration("max-wait", 100*time.Millisecond, "max time a request waits for a slot")
		drain       = flag.Duration("drain-timeout", 10*time.Second, "graceful shutdown deadline")
	)
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(*walPath), 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	eng, err := store.NewEngine(*walPath) // snapshot load + WAL replay happen here
	if err != nil {
		log.Fatalf("open engine: %v", err) // includes a bad snapshot: startup fails by design
	}
	r := eng.Recovery()
	log.Printf("recovery: replayed %d records, discarded %d torn bytes (%q)",
		r.Records, r.DiscardedBytes, r.DiscardReason)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		eng.Close()
		log.Fatalf("listen: %v", err)
	}

	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(server.MaxMessageSize),
		grpc.UnaryInterceptor(server.LimitInterceptor(*maxInflight, *maxWait)),
	)
	kvpb.RegisterKVServer(gs, server.New(eng))
	kvpb.RegisterAdminServer(gs, server.NewAdmin(eng))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("serving on %s", *addr)
	runErr := server.Run(ctx, lis, gs, *drain)

	// Server fully stopped: no in-flight handler can touch the WAL now.
	// Close also waits for any in-flight Snapshot (snapMu).
	if err := eng.Close(); err != nil {
		log.Printf("close engine: %v", err)
	}
	if runErr != nil {
		log.Printf("server: %v", runErr)
		os.Exit(1)
	}
}
