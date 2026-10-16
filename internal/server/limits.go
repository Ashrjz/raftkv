package server

import "github.com/Ashrjz/raftkv/internal/wal"

// MaxMessageSize is the gRPC message size limit for both directions' receive
// side. gRPC's default (4 MiB) is below the engine's value limit, so large
// values would be rejected before reaching validation.
//
// The worst legal Put is 16,774,152 bytes (key + value + proto framing); the
// 64 KiB headroom leaves room for fields added in later phases. A value over
// wal.MaxValueSize but under this limit gets InvalidArgument from the
// handler; larger messages are rejected by gRPC itself with ResourceExhausted.
//
// Note: gRPC buffers the whole message before the concurrency limiter runs,
// so the limiter does not bound memory (worst case ~ in-flight x 16 MiB).
const MaxMessageSize = wal.MaxPayload + 64<<10
