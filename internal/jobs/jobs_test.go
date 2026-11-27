package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/transport"
)

// TestMain doubles as the detached worker: proc.Detach re-execs this test binary as `_job <id>`.
func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "_job" {
		s, err := store.Open("")
		if err == nil {
			err = Run(s, os.Args[2])
			s.Close()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "test worker:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fake is a test source: it emits Batches (each one waiting for a send on gates[Gate] when Gate is
// set), then waits for cancellation if Hold, else ends cleanly.
type fake struct {
	Batches [][]string `json:"batches"`
	Gate    string     `json:"gate,omitempty"`
	Hold    bool       `json:"hold,omitempty"`
}

var (
	gatesMu sync.Mutex
	gates   = map[string]chan struct{}{}
)

func gate(name string) chan struct{} {
	gatesMu.Lock()
	defer gatesMu.Unlock()
	if gates[name] == nil {
		gates[name] = make(chan struct{})
	}
	return gates[name]
}

func init() {
	source.Register("fake", func(cfg json.RawMessage) (source.Source, error) {
		var f fake
		if err := json.Unmarshal(cfg, &f); err != nil {
			return nil, err
		}
		return f, nil
	})
}

func (f fake) Describe() string       { return "fake" }
func (f fake) RestartByDefault() bool { return false }
func (f fake) Stream(ctx context.Context, _ transport.Transport, _ bool, emit func([]string)) error {
	for _, b := range f.Batches {
		if f.Gate != "" {
			select {
			case <-gate(f.Gate):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		emit(b)
	}
	if f.Hold {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func fakeSpec(f fake) source.Spec {
	b, _ := json.Marshal(f)
	return source.Spec{Kind: "fake", Config: b}
}

func newStore(t *testing.T) *store.Store {
	t.Setenv("TAIL_MODE_HOME", t.TempDir())
	s, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func setVar[T any](t *testing.T, p *T, v T) {
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

// mkJob inserts a job row directly with the given status (no worker).
func mkJob(t *testing.T, s *store.Store, spec source.Spec, status string) string {
	b, _ := json.Marshal(spec)
	j := store.Job{ID: store.NewJobID(), Kind: spec.Kind, Transport: "local", Spec: string(b), Status: status, StartedAt: store.NowMS()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	return j.ID
}

func texts(t *testing.T, s *store.Store, id string) []string {
	ls, err := s.Read(store.ReadQuery{Job: id, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, l := range ls {
		out = append(out, l.Text)
	}
	return out
}

func status(t *testing.T, s *store.Store, id string) store.Job {
	j, err := s.GetJob(id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestRunCmdExits(t *testing.T) {
	s := newStore(t)
	id := mkJob(t, s, source.Spec{Kind: "cmd", Config: json.RawMessage(`{"cmd":"printf 'a\\nb\\n'; exit 2"}`)}, "running")
	if err := run(context.Background(), s, id); err != nil {
		t.Fatal(err)
	}
	if j := status(t, s, id); j.Status != "exited" || j.ExitCode != 2 {
		t.Fatalf("job %+v", j)
	}
	if got := texts(t, s, id); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("lines %q", got)
	}
}

func TestRunFileUntilCancel(t *testing.T) {
	s := newStore(t)
	p := filepath.Join(t.TempDir(), "log")
	os.WriteFile(p, []byte("x\n"), 0o600)
	cfg, _ := json.Marshal(source.FileConfig{Path: p})
	id := mkJob(t, s, source.Spec{Kind: "file", Config: cfg}, "running")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- run(ctx, s, id) }()
	time.Sleep(200 * time.Millisecond)
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("y\n")
	f.Close()
	for end := time.Now().Add(5 * time.Second); len(texts(t, s, id)) < 2; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("lines %q", texts(t, s, id))
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if j := status(t, s, id); j.Status != "stopped" {
		t.Fatalf("status %s", j.Status)
	}
	if got := texts(t, s, id); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("lines %q", got)
	}
}

func TestRunRestartMarker(t *testing.T) {
	s := newStore(t)
	setVar(t, &restartMin, 200*time.Millisecond)
	yes := true
	id := mkJob(t, s, source.Spec{Kind: "cmd", Config: json.RawMessage(`{"cmd":"echo hi"}`), Restart: &yes}, "running")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	run(ctx, s, id)
	got := texts(t, s, id)
	if len(got) < 3 || got[0] != "hi" || got[1] != "[tail-mode] restarted (attempt 1): source ended" || got[2] != "hi" {
		t.Fatalf("lines %q", got)
	}
	if j := status(t, s, id); j.Status != "stopped" {
		t.Fatalf("status %s", j.Status)
	}
}

func TestStartValidates(t *testing.T) {
	s := newStore(t)
	for _, o := range []StartOpts{
		{Spec: source.Spec{Kind: "nope"}},
		{Spec: fakeSpec(fake{}), Transport: "nope"},
		{Spec: source.Spec{Kind: "fake", Config: json.RawMessage(`{"batches": 1}`)}},
	} {
		if _, err := Start(s, o); err == nil {
			t.Fatalf("want error for %+v", o)
		}
	}
	if js, _ := s.ListJobs(); len(js) != 0 {
		t.Fatalf("invalid starts created rows: %+v", js)
	}
}

// End to end through a real detached worker (this test binary, see TestMain).
func TestStartStopWorker(t *testing.T) {
	s := newStore(t)
	j, err := Start(s, StartOpts{Name: "w", Spec: fakeSpec(fake{Batches: [][]string{{"one", "two"}}, Hold: true})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Stop(s, j.ID) })
	if j.Status != "running" || j.PID <= 0 || j.PID == os.Getpid() || j.Transport != "local" {
		t.Fatalf("started %+v", j)
	}
	if !proc.Locked(LockPath(s, j.ID)) || Status(s, j) != "running" {
		t.Fatal("worker does not hold its lock")
	}
	for end := time.Now().Add(3 * time.Second); len(texts(t, s, j.ID)) < 2; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("lines %q", texts(t, s, j.ID))
		}
	}
	js, _ := List(s)
	if len(js) != 1 || js[0].Status != "running" || js[0].Lines != 2 {
		t.Fatalf("list %+v", js)
	}
	j, err = Stop(s, "w")
	if err != nil || j.Status != "stopped" {
		t.Fatalf("stop: %v %+v", err, j)
	}
	if proc.Locked(LockPath(s, j.ID)) {
		t.Fatal("worker still holds its lock after Stop")
	}
	// The worker releases its lock just before exiting; Detach's goroutine then reaps it.
	for end := time.Now().Add(3 * time.Second); syscall.Kill(j.PID, 0) == nil; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("worker still alive after Stop")
		}
	}
}

// The start race: the row exists as "starting" before the worker locks and claims.
func TestStartingRace(t *testing.T) {
	s := newStore(t)
	id := mkJob(t, s, fakeSpec(fake{Hold: true}), "starting")

	// List in the window between CreateJob and claim must not mark it lost.
	js, err := List(s)
	if err != nil || js[0].Status != "starting" || status(t, s, id).Status != "starting" {
		t.Fatalf("list in start window: %v %+v", err, js)
	}
	// Stop still stops it, without signalling anything (pid is 0).
	j, err := Stop(s, id)
	if err != nil || j.Status != "stopped" {
		t.Fatalf("stop starting: %v %+v", err, j)
	}
	// The late worker then fails to claim and exits cleanly without running the source.
	if err := Run(s, id); err != nil {
		t.Fatal(err)
	}
	if j := status(t, s, id); j.Status != "stopped" || j.PID != 0 {
		t.Fatalf("after late worker: %+v", j)
	}

	// A worker that never shows up is lost after the grace period.
	setVar(t, &startingGrace, 0)
	id2 := mkJob(t, s, fakeSpec(fake{}), "starting")
	if st := Status(s, status(t, s, id2)); st != "lost" {
		t.Fatalf("abandoned starting: %s", st)
	}
	// ...but not while something holds its lock (worker between Lock and Claim).
	release, err := proc.Lock(LockPath(s, id2))
	if err != nil {
		t.Fatal(err)
	}
	if st := Status(s, status(t, s, id2)); st != "starting" {
		t.Fatalf("locked starting: %s", st)
	}
	release()
}

// Liveness is the lock, not the pid: a live pid without the lock is lost.
func TestLockLiveness(t *testing.T) {
	s := newStore(t)
	id := mkJob(t, s, fakeSpec(fake{}), "starting")
	if ok, _ := s.Claim(id, os.Getpid()); !ok {
		t.Fatal("claim")
	}
	release, _ := proc.Lock(LockPath(s, id))
	if st := Status(s, status(t, s, id)); st != "running" {
		t.Fatalf("locked: %s", st)
	}
	release()
	js, _ := List(s)
	if js[0].Status != "lost" || status(t, s, id).Status != "lost" {
		t.Fatalf("unlocked live pid not lost: %+v", js[0])
	}
	// Stop on a lost job does not signal (it would hit this test process).
	if j, err := Stop(s, id); err != nil || j.Status != "lost" {
		t.Fatalf("stop lost: %v %+v", err, j)
	}
}

func TestEmptyRef(t *testing.T) {
	s := newStore(t)
	id := mkJob(t, s, fakeSpec(fake{}), "starting") // unnamed
	if _, err := Stop(s, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stop \"\": %v", err)
	}
	if st := status(t, s, id).Status; st != "starting" {
		t.Fatalf("unnamed job touched: %s", st)
	}
}

// holdWriteLock takes the DB write lock on a second connection until the returned func is called.
func holdWriteLock(t *testing.T, s *store.Store) func() {
	db, err := sql.Open("sqlite3", "file:"+s.Path())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	unlock := func() {
		once.Do(func() {
			c.ExecContext(ctx, "COMMIT")
			c.Close()
			db.Close()
		})
	}
	t.Cleanup(unlock)
	return unlock
}

func TestAppendRetriedUnderWriteLock(t *testing.T) {
	setVar(t, &store.BusyTimeout, 100*time.Millisecond)
	setVar(t, &appendBudget, 5*time.Second)
	s := newStore(t)
	id := mkJob(t, s, fakeSpec(fake{Batches: [][]string{{"a", "b", "c"}}, Gate: t.Name()}), "running")
	done := make(chan error)
	go func() { done <- run(context.Background(), s, id) }()

	unlock := holdWriteLock(t, s)
	gate(t.Name()) <- struct{}{}
	time.Sleep(700 * time.Millisecond) // several busy failures + retries
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if j := status(t, s, id); j.Status != "exited" {
		t.Fatalf("job %+v", j)
	}
	if got := texts(t, s, id); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("lines %q", got)
	}
}

func TestAppendGivesUpFailsJob(t *testing.T) {
	setVar(t, &store.BusyTimeout, 100*time.Millisecond)
	setVar(t, &appendBudget, 400*time.Millisecond)
	setVar(t, &finishBudget, 5*time.Second)
	s := newStore(t)
	id := mkJob(t, s, fakeSpec(fake{Batches: [][]string{{"lost1", "lost2"}}, Gate: t.Name(), Hold: true}), "running")
	done := make(chan error)
	go func() { done <- run(context.Background(), s, id) }()

	unlock := holdWriteLock(t, s)
	gate(t.Name()) <- struct{}{}
	time.Sleep(time.Second) // past the append budget; the final Finish is still being retried
	unlock()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "dropped 2 line(s)") {
			t.Fatalf("run err: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("source not cancelled after append gave up")
	}
	if j := status(t, s, id); j.Status != "failed" || !strings.Contains(j.Error, "append failed") {
		t.Fatalf("job %+v", j)
	}
	if got := texts(t, s, id); len(got) != 0 {
		t.Fatalf("partial batch stored: %q", got)
	}
}
