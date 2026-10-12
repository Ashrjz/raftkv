package store

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	k9Runs     = flag.Int("kill9.runs", 15, "number of kill -9 iterations")
	k9Seed     = flag.Int64("kill9.seed", 0, "seed for ops and kill timing (0 = random; always logged)")
	k9MaxDelay = flag.Duration("kill9.maxdelay", 300*time.Millisecond, "max time to let the child run after READY before kill -9")
)

// ---------- deterministic operation sequence ----------

type k9Operation struct {
	del       bool
	key       string
	val       []byte
	snapAfter bool
}

// k9Rand is a stateless hash (splitmix64 finalizer): same inputs, same output.
func k9Rand(seed int64, i int, salt uint64) uint64 {
	z := uint64(seed) + uint64(i)*0x9E3779B97F4A7C15 + salt*0xBF58476D1CE4E5B9
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// k9Op is a pure function of (seed, i). Values start with "op<i>:", so any
// stale, phantom or mixed-up value identifies the operation it came from.
func k9Op(seed int64, i int) k9Operation {
	op := k9Operation{
		key:       fmt.Sprintf("key-%02d", k9Rand(seed, i, 1)%40), // small keyspace: lots of overwrites
		del:       k9Rand(seed, i, 2)%100 < 25,
		snapAfter: k9Rand(seed, i, 4)%25 == 0,
	}
	n := 1 + int(k9Rand(seed, i, 3)%600)
	if k9Rand(seed, i, 5)%12 == 0 {
		n = 200 << 10 // occasional large record: a fatal signal can interrupt a big write midway
	}
	val := make([]byte, 0, n+16)
	val = append(val, fmt.Sprintf("op%d:", i)...)
	for j := 0; j < n; j++ {
		val = append(val, byte(k9Rand(seed, i, uint64(100+j/8))>>(8*uint(j%8))))
	}
	op.val = val
	return op
}

// k9Advance applies ops [from, to) to the model. A delete of an absent key
// becomes a put; the child makes the same decision from the engine's state.
func k9Advance(m map[string][]byte, seed int64, from, to int) {
	for i := from; i < to; i++ {
		op := k9Op(seed, i)
		if _, exists := m[op.key]; op.del && exists {
			delete(m, op.key)
		} else {
			m[op.key] = op.val
		}
	}
}

func k9Clone(m map[string][]byte) map[string][]byte {
	c := make(map[string][]byte, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func k9Equal(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		if bv, ok := b[k]; !ok || !bytes.Equal(av, bv) {
			return false
		}
	}
	return true
}

func k9Brief(v []byte) string {
	if len(v) > 10 {
		v = v[:10]
	}
	return fmt.Sprintf("%q", v)
}

// k9Diff describes how got differs from want, in terms of acknowledged state.
func k9Diff(want, got map[string][]byte) string {
	var lines []string
	add := func(s string) {
		if len(lines) < 8 {
			lines = append(lines, "  "+s)
		}
	}
	for k, wv := range want {
		gv, ok := got[k]
		switch {
		case !ok:
			add(fmt.Sprintf("%s: LOST (want value starting %s)", k, k9Brief(wv)))
		case !bytes.Equal(wv, gv):
			add(fmt.Sprintf("%s: WRONG VALUE (want %s, got %s)", k, k9Brief(wv), k9Brief(gv)))
		}
	}
	for k, gv := range got {
		if _, ok := want[k]; !ok {
			add(fmt.Sprintf("%s: UNEXPECTED (got %s; deleted or never written)", k, k9Brief(gv)))
		}
	}
	return strings.Join(lines, "\n")
}

// ---------- child process ----------

// TestKill9Child is the victim. It only runs when re-executed by TestKill9.
// It never returns on its own: it writes until it is killed.
func TestKill9Child(t *testing.T) {
	dir := os.Getenv("KILL9_DIR")
	if dir == "" {
		t.Skip("runs only as a child of TestKill9")
	}
	seed, _ := strconv.ParseInt(os.Getenv("KILL9_SEED"), 10, 64)
	start, _ := strconv.Atoi(os.Getenv("KILL9_START"))

	// Safety net so an orphaned child cannot spin forever.
	time.AfterFunc(60*time.Second, func() {
		fmt.Fprintln(os.Stderr, "kill9 child: self-destruct timeout")
		os.Exit(3)
	})

	die := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "kill9 child: "+format+"\n", a...)
		os.Exit(2)
	}

	e, err := NewEngine(filepath.Join(dir, "wal.log")) // includes recovery of whatever the last kill left
	if err != nil {
		die("open/recover: %v", err)
	}
	fmt.Println("READY")

	for i := start; ; i++ {
		op := k9Op(seed, i)
		fmt.Printf("BEGIN %d\n", i)

		_, gerr := e.Get(op.key)
		if op.del && gerr == nil {
			err = e.Delete(op.key)
		} else {
			err = e.Put(op.key, op.val)
		}
		if err != nil {
			die("op %d: %v", i, err)
		}
		fmt.Printf("ACK %d\n", i) // printed only after the engine acknowledged the write

		if op.snapAfter {
			if err := e.Snapshot(); err != nil {
				die("snapshot after op %d: %v", i, err)
			}
			fmt.Printf("SNAP %d\n", i)
		}
	}
}

// ---------- parent: reading the child's protocol ----------

type k9Tracker struct {
	mu       sync.Mutex
	buf      []byte
	ready    chan struct{}
	once     sync.Once
	lastAck  int // index of the last acknowledged op (start-1 if none)
	begun    int // highest op index the child started (-1 if none)
	lastSnap int // op index after which the last snapshot completed (-1 if none)
	snaps    int
}

func newK9Tracker(start int) *k9Tracker {
	return &k9Tracker{ready: make(chan struct{}), lastAck: start - 1, begun: -1, lastSnap: -1}
}

// Write receives the child's stdout (exec copies it in its own goroutine).
func (tr *k9Tracker) Write(p []byte) (int, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.buf = append(tr.buf, p...)
	for {
		i := bytes.IndexByte(tr.buf, '\n')
		if i < 0 {
			break
		}
		line := string(tr.buf[:i])
		tr.buf = tr.buf[i+1:]
		tr.handle(line)
	}
	return len(p), nil
}

func (tr *k9Tracker) handle(line string) {
	num := func(prefix string) int {
		n, err := strconv.Atoi(strings.TrimPrefix(line, prefix))
		if err != nil {
			return -1
		}
		return n
	}
	switch {
	case line == "READY":
		tr.once.Do(func() { close(tr.ready) })
	case strings.HasPrefix(line, "BEGIN "):
		tr.begun = num("BEGIN ")
	case strings.HasPrefix(line, "ACK "):
		tr.lastAck = num("ACK ")
	case strings.HasPrefix(line, "SNAP "):
		tr.lastSnap = num("SNAP ")
		tr.snaps++
	}
}

// k9RunChild starts the victim, lets it run for a random time, kills it with
// SIGKILL, and returns what it reported. early is true if the kill was aimed
// at the startup phase.
func k9RunChild(t *testing.T, exe, dir string, seed int64, start int, rng *rand.Rand) (tr *k9Tracker, early bool) {
	t.Helper()
	cmd := exec.Command(exe, "-test.run=^TestKill9Child$", "-test.count=1")
	cmd.Env = append(os.Environ(),
		"KILL9_DIR="+dir,
		"KILL9_SEED="+strconv.FormatInt(seed, 10),
		"KILL9_START="+strconv.Itoa(start))
	tr = newK9Tracker(start)
	var stderr bytes.Buffer
	cmd.Stdout = tr
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	early = rng.Intn(5) == 0 // 1 in 5: aim at process start / recovery
	var delay time.Duration
	if early {
		delay = time.Duration(rng.Int63n(int64(40 * time.Millisecond)))
	} else {
		select {
		case <-tr.ready:
		case err := <-exited:
			t.Fatalf("child died before becoming ready: %v\nstderr:\n%s", err, stderr.String())
		case <-time.After(30 * time.Second):
			cmd.Process.Kill()
			t.Fatalf("child never became ready\nstderr:\n%s", stderr.String())
		}
		delay = time.Duration(rng.Int63n(int64(*k9MaxDelay) + 1))
	}

	select {
	case <-time.After(delay):
	case err := <-exited:
		t.Fatalf("child exited on its own before being killed: %v\nstderr:\n%s", err, stderr.String())
	}
	cmd.Process.Kill() // SIGKILL: no deferred functions, no flushing
	<-exited           // after this, tr holds everything the child printed
	return tr, early
}

// ---------- verification ----------

func k9CopyDir(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.RemoveAll(dst); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		if !en.Type().IsRegular() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, en.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, en.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKill9(t *testing.T) {
	if testing.Short() {
		t.Skip("kill -9 harness skipped in -short mode")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	seed := *k9Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	t.Logf("kill9: seed=%d runs=%d maxdelay=%v (re-run with -kill9.seed=%d)", seed, *k9Runs, *k9MaxDelay, seed)

	root, err := os.MkdirTemp("", "kill9-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			t.Logf("data kept for inspection: %s", root)
		} else {
			os.RemoveAll(root)
		}
	}()
	data := filepath.Join(root, "data") // reused by every iteration: crashes compound
	verify := filepath.Join(root, "verify")
	if err := os.Mkdir(data, 0o755); err != nil {
		t.Fatal(err)
	}

	model := map[string][]byte{} // state after ops [0, start)
	start := 0
	var totalAcked, totalSnaps, inflightLanded, startupKills, midSnapshotKills, tornTails int

	for run := 1; run <= *k9Runs; run++ {
		tr, early := k9RunChild(t, exe, data, seed, start, rng)

		tr.mu.Lock()
		done := tr.lastAck + 1 // ops [start, done) were acknowledged
		inFlight := tr.begun == done
		snaps, lastSnap, lastAck := tr.snaps, tr.lastSnap, tr.lastAck
		tr.mu.Unlock()

		// Verify a COPY, so the original stays exactly as the kill left it and
		// the next child performs its own crash recovery on it.
		k9CopyDir(t, data, verify)
		e, err := NewEngine(filepath.Join(verify, "wal.log"))
		if err != nil {
			t.Fatalf("run %d (seed %d): recovery failed after kill -9: %v\n(acked through op %d)", run, seed, err, lastAck)
		}
		got := e.mem.clone()
		rec := e.recovery
		e.Close()

		want := k9Clone(model)
		k9Advance(want, seed, start, done) // every acknowledged op must be present
		chosen := -1
		switch {
		case k9Equal(got, want):
			chosen = done
		case inFlight:
			// The op that was in flight at the kill may or may not have landed.
			alt := k9Clone(want)
			k9Advance(alt, seed, done, done+1)
			if k9Equal(got, alt) {
				chosen, want = done+1, alt
				inflightLanded++
			}
		}
		if chosen < 0 {
			t.Fatalf("run %d (seed %d): recovered state is wrong after kill -9\n"+
				"  ops acknowledged: [%d, %d), op %d in flight: %v\n"+
				"  differences vs expected state (%d keys expected, %d recovered):\n%s",
				run, seed, start, done, done, inFlight, len(want), len(got), k9Diff(want, got))
		}

		// Coverage bookkeeping.
		totalAcked += done - start
		totalSnaps += snaps
		if rec.DiscardedBytes > 0 {
			tornTails++
		}
		if early && tr.begun < 0 {
			startupKills++
		}
		if lastAck >= start && k9Op(seed, lastAck).snapAfter && lastSnap != lastAck {
			midSnapshotKills++ // killed between ACK and the end of that op's snapshot
		}

		model, start = want, chosen
	}

	t.Logf("kill9 summary: %d runs, %d acknowledged ops, %d snapshots completed; "+
		"kills during startup=%d, during a snapshot=%d; in-flight op landed=%d; torn tails repaired=%d",
		*k9Runs, totalAcked, totalSnaps, startupKills, midSnapshotKills, inflightLanded, tornTails)

	// A harness that never got the child to do anything proves nothing.
	if totalAcked == 0 {
		t.Fatal("child never acknowledged a single operation; the harness is not exercising anything")
	}
	if totalSnaps == 0 && *k9Runs >= 10 {
		t.Fatal("no snapshot ever completed; snapshot/truncation crash windows were not exercised")
	}
}
