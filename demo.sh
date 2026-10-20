#!/usr/bin/env bash
# Phase 2 acceptance demo. Run from the repo root: ./demo.sh
set -u

PORT=7000
WORK=$(mktemp -d)
WAL=$WORK/data/raftkv.wal
SERVER_PID=
PASS=0
FAILED=0

go build -o "$WORK/kvserver" ./cmd/kvserver || exit 2
go build -o "$WORK/kvcli" ./cmd/kvcli || exit 2
KV() { "$WORK/kvcli" -addr "localhost:$PORT" "$@"; }
trap '[ -n "$SERVER_PID" ] && kill -9 "$SERVER_PID" 2>/dev/null' EXIT

ok()  { echo "PASS  $1"; PASS=$((PASS + 1)); }
bad() { echo "FAIL  $1"; FAILED=$((FAILED + 1)); }
size() { stat -c %s "$1" 2>/dev/null || echo 0; }
recovery() { grep recovery "$WORK/server.log" | tail -1; }

start_server() {
  "$WORK/kvserver" --addr ":$PORT" --wal "$WAL" >>"$WORK/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 50); do
    KV get __ready__ >/dev/null 2>&1
    [ $? -ne 2 ] && return 0 # 0 or 1 (not found) means the server answered
    sleep 0.1
  done
  echo "server did not start; see $WORK/server.log"
  exit 2
}

verify() { # verify <ack-file> <label>
  echo "   $(wc -l <"$1") acknowledged writes recorded; $(recovery)"
  if KV verify -ack-file "$1"; then ok "$2"; else bad "$2"; fi
}

echo "== 1. functional behaviour and limits (selftest)"
start_server
if KV selftest; then ok "selftest"; else bad "selftest"; fi

echo "== 2. crash recovery: kill -9 during load"
KV bench -workers 8 -duration 4s -value-size 256 -allow-unavailable \
  -ack-file "$WORK/ack-crash" >"$WORK/bench-crash.out" 2>&1 &
BENCH_PID=$!
sleep 2
kill -9 "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null
wait "$BENCH_PID"
start_server
verify "$WORK/ack-crash" "no acknowledged write lost after kill -9"

echo "== 3. snapshot truncates the WAL"
before=$(size "$WAL")
if KV snapshot; then ok "snapshot RPC"; else bad "snapshot RPC"; fi
after=$(size "$WAL")
echo "   WAL: $before -> $after bytes; snapshot file: $(size "$WORK/data/snapshot") bytes"
if [ "$after" -lt "$before" ]; then ok "WAL shrank"; else bad "WAL did not shrink"; fi
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
start_server
verify "$WORK/ack-crash" "data intact after snapshot + restart"

echo "== 4. graceful shutdown: SIGTERM during load"
KV bench -workers 16 -duration 6s -value-size 256 -allow-unavailable \
  -ack-file "$WORK/ack-term" >"$WORK/bench-term.out" 2>&1 &
BENCH_PID=$!
sleep 3
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
rc=$?
if [ $rc -eq 0 ]; then ok "server exited cleanly"; else bad "server exit status $rc"; fi
wait "$BENCH_PID"
rc=$?
sed 's/^/   /' "$WORK/bench-term.out"
if [ $rc -eq 0 ]; then ok "every request was OK or cleanly rejected (Unavailable)"; else bad "requests dropped (other status codes)"; fi
start_server
verify "$WORK/ack-term" "every write acknowledged before shutdown is present"

echo
echo "$PASS passed, $FAILED failed"
if [ $FAILED -eq 0 ]; then rm -rf "$WORK"; else echo "kept for inspection: $WORK (server.log, bench-*.out)"; fi
[ $FAILED -eq 0 ]