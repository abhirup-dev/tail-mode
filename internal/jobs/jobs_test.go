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
	"testing"
	"time"

	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/testutil"
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

// fake is a test source: it emits "resumed=<bool>" if Mark, then Batches (each one waiting for a send on
// gates[Gate] when Gate is set), then if Hold waits for cancellation (closing gates[Gate+"/done"]), else
// ends cleanly.
type fake struct {
	Batches [][]string `json:"batches"`
	Gate    string     `json:"gate,omitempty"`
	Hold    bool       `json:"hold,omitempty"`
	Mark    bool       `json:"mark,omitempty"`
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
func (f fake) Stream(ctx context.Context, _ transport.Transport, resumed bool, emit func([]string)) error {
	if f.Mark {
		emit([]string{fmt.Sprint("resumed=", resumed)})
	}
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
		if f.Gate != "" {
			close(gate(f.Gate + "/done"))
		}
		return ctx.Err()
	}
	return nil
}

// mkJob inserts a job row for spec with the given status (no worker) and returns its id.
func mkJob(t *testing.T, s *store.Store, spec source.Spec, status string) string {
	b, _ := json.Marshal(spec)
	return testutil.Job(t, s, store.Job{Kind: spec.Kind, Spec: string(b), Status: status}).ID
}

func fakeJob(t *testing.T, s *store.Store, f fake, status string) string {
	b, _ := json.Marshal(f)
	return mkJob(t, s, source.Spec{Kind: "fake", Config: b}, status)
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
	j, err := s.GetJob(id, "")
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// runBG runs the worker loop for id in the background; stop cancels it and returns run's error.
func runBG(s *store.Store, id string) (stop func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- run(ctx, s, id) }()
	return func() error { cancel(); return <-done }
}

func lines(t *testing.T, s *store.Store, id string, n int) func() bool {
	return func() bool { return len(texts(t, s, id)) >= n }
}

func TestRunCmdExits(t *testing.T) {
	s := testutil.Store(t)
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
	s := testutil.Store(t)
	p := filepath.Join(t.TempDir(), "log")
	os.WriteFile(p, []byte("x\n"), 0o600)
	cfg, _ := json.Marshal(source.FileConfig{Path: p})
	id := mkJob(t, s, source.Spec{Kind: "file", Config: cfg}, "running")
	stop := runBG(s, id)
	testutil.WaitFor(t, 5*time.Second, lines(t, s, id, 1)) // the backlog is in: tail follows the file now
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("y\n")
	f.Close()
	testutil.WaitFor(t, 5*time.Second, lines(t, s, id, 2))
	if err := stop(); err != nil || status(t, s, id).Status != "stopped" {
		t.Fatalf("%v %+v", err, status(t, s, id))
	}
	if got := texts(t, s, id); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("lines %q", got)
	}
}

// A restarted source is told it resumes (a file then reads no backlog again: see source's
// TestFileConfig), after a marker line.
func TestRunRestartResumes(t *testing.T) {
	s := testutil.Store(t)
	testutil.Swap(t, &restartMin, 10*time.Millisecond)
	yes := true
	b, _ := json.Marshal(fake{Mark: true})
	id := mkJob(t, s, source.Spec{Kind: "fake", Config: b, Restart: &yes}, "running")
	stop := runBG(s, id)
	testutil.WaitFor(t, 5*time.Second, lines(t, s, id, 3))
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := texts(t, s, id); !reflect.DeepEqual(got[:3], []string{"resumed=false", "[tail-mode] restarted (attempt 1): source ended", "resumed=true"}) {
		t.Fatalf("lines %q", got)
	}
	if j := status(t, s, id); j.Status != "stopped" {
		t.Fatalf("status %s", j.Status)
	}
}

func TestStartValidates(t *testing.T) {
	s := testutil.Store(t)
	for _, o := range []StartOpts{
		{Spec: source.Spec{Kind: "nope"}},
		{Spec: source.Spec{Kind: "fake", Config: json.RawMessage(`{}`)}, Transport: "nope"},
		{Spec: source.Spec{Kind: "fake", Config: json.RawMessage(`{"batches": 1}`)}},
	} {
		if _, err := Start(s, o); err == nil {
			t.Fatalf("want error for %+v", o)
		}
	}
	if js, _ := s.ListJobs("", true); len(js) != 0 {
		t.Fatalf("invalid starts created rows: %+v", js)
	}
}

// End to end through a real detached worker (this test binary, see TestMain). Stop sends the source's
// process group SIGTERM before SIGKILL, so it can say goodbye: the line is kept and the job is stopped.
func TestStartStopWorker(t *testing.T) {
	s := testutil.Store(t)
	b, _ := json.Marshal(source.CmdConfig{Cmd: "trap 'echo bye; exit 0' TERM; echo ready; sleep 30 & wait", Dir: "/"})
	j, err := Start(s, StartOpts{Name: "w", Spec: source.Spec{Kind: "cmd", Config: b}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Stop(s, j.ID, "", false) })
	if j.Status != "running" || j.PID <= 0 || j.PID == os.Getpid() || j.Transport != "local" {
		t.Fatalf("started %+v", j)
	}
	if !proc.Locked(s.JobLock(j.ID)) || Status(s, j) != "running" {
		t.Fatal("worker does not hold its lock")
	}
	testutil.WaitFor(t, 3*time.Second, lines(t, s, j.ID, 1))
	if js, _ := List(s, "", true); len(js) != 1 || js[0].Status != "running" || js[0].Lines != 1 {
		t.Fatalf("list %+v", js)
	}
	j, err = Stop(s, "w", "", false)
	if err != nil || j.Status != "stopped" || proc.Locked(s.JobLock(j.ID)) || !reflect.DeepEqual(texts(t, s, j.ID), []string{"ready", "bye"}) {
		t.Fatalf("stop: %v %+v %q", err, j, texts(t, s, j.ID))
	}
	testutil.WaitDead(t, j.PID) // it released its lock just before exiting; Detach's goroutine reaps it
}

// The start race: the row exists as "starting" before the worker locks and claims.
func TestStartingRace(t *testing.T) {
	s := testutil.Store(t)
	id := fakeJob(t, s, fake{Hold: true}, "starting")

	// List in the window between CreateJob and claim must not mark it lost.
	js, err := List(s, "", true)
	if err != nil || js[0].Status != "starting" || status(t, s, id).Status != "starting" {
		t.Fatalf("list in start window: %v %+v", err, js)
	}
	// Stop still stops it, without signalling anything (pid is 0).
	j, err := Stop(s, id, "", false)
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

	// A worker that never shows up is lost after the grace period...
	testutil.Swap(t, &startingGrace, 0)
	id2 := fakeJob(t, s, fake{}, "starting")
	if st := Status(s, status(t, s, id2)); st != "lost" {
		t.Fatalf("abandoned starting: %s", st)
	}
	// ...but not while something holds its lock (worker between Lock and Claim).
	release, err := proc.Lock(s.JobLock(id2))
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if st := Status(s, status(t, s, id2)); st != "starting" {
		t.Fatalf("locked starting: %s", st)
	}
}

// Liveness is the lock, not the pid: a live pid without the lock is lost.
func TestLockLiveness(t *testing.T) {
	s := testutil.Store(t)
	id := fakeJob(t, s, fake{}, "starting")
	if ok, _ := s.Claim(id, os.Getpid()); !ok {
		t.Fatal("claim")
	}
	release, _ := proc.Lock(s.JobLock(id))
	if st := Status(s, status(t, s, id)); st != "running" {
		t.Fatalf("locked: %s", st)
	}
	release()
	js, _ := List(s, "", true)
	if js[0].Status != "lost" || status(t, s, id).Status != "lost" {
		t.Fatalf("unlocked live pid not lost: %+v", js[0])
	}
	// Stop on a lost job does not signal (it would hit this test process).
	if j, err := Stop(s, id, "", false); err != nil || j.Status != "lost" {
		t.Fatalf("stop lost: %v %+v", err, j)
	}
	if _, err := Stop(s, "", "", false); !errors.Is(err, store.ErrNotFound) { // "" is no name
		t.Fatalf("stop \"\": %v", err)
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

// A batch that hits a busy DB is retried; one that cannot be stored within the budget cancels the
// source and fails the job (never a clean exit after dropping lines).
func TestAppendUnderWriteLock(t *testing.T) {
	testutil.Swap(t, &store.BusyTimeout, 50*time.Millisecond)
	var mu sync.Mutex
	fails := 0
	testutil.Swap(t, &retried, func(string, error, time.Duration) { mu.Lock(); fails++; mu.Unlock() })
	failed := func() int { mu.Lock(); defer mu.Unlock(); return fails }

	s := testutil.Store(t)
	id := fakeJob(t, s, fake{Batches: [][]string{{"a", "b", "c"}}, Gate: t.Name()}, "running")
	done := make(chan error)
	go func() { done <- run(context.Background(), s, id) }()
	unlock := holdWriteLock(t, s)
	gate(t.Name()) <- struct{}{}
	testutil.WaitFor(t, 5*time.Second, func() bool { return failed() >= 2 })
	unlock()
	if err := <-done; err != nil || status(t, s, id).Status != "exited" || !reflect.DeepEqual(texts(t, s, id), []string{"a", "b", "c"}) {
		t.Fatalf("retried: %v %+v %q", err, status(t, s, id), texts(t, s, id))
	}

	testutil.Swap(t, &appendBudget, 200*time.Millisecond)
	gave := t.Name() + "/up"
	id = fakeJob(t, s, fake{Batches: [][]string{{"lost1", "lost2"}}, Gate: gave, Hold: true}, "running")
	go func() { done <- run(context.Background(), s, id) }()
	unlock = holdWriteLock(t, s)
	gate(gave) <- struct{}{}
	select {
	case <-gate(gave + "/done"): // the source was cancelled; the final Finish is being retried
	case <-time.After(10 * time.Second):
		t.Fatal("source not cancelled after append gave up")
	}
	unlock()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "dropped 2 line(s)") {
		t.Fatalf("run err: %v", err)
	}
	if j := status(t, s, id); j.Status != "failed" || !strings.Contains(j.Error, "append failed") || len(texts(t, s, id)) != 0 {
		t.Fatalf("gave up: %+v %q", j, texts(t, s, id))
	}
}

// Stop resolves names within the caller's owner and refuses another owner's job without force; jobs in
// the shared namespace are anyone's. ("starting" jobs: no worker to signal.)
func TestStopOwners(t *testing.T) {
	s := testutil.Store(t)
	a := testutil.Job(t, s, store.Job{Name: "web", Owner: "a"}).ID
	b := testutil.Job(t, s, store.Job{Name: "web", Owner: "b"}).ID
	shared := testutil.Job(t, s, store.Job{}).ID
	var noe *NotOwnerError
	if _, err := Stop(s, a, "b", false); !errors.As(err, &noe) || noe.Owner != "a" || status(t, s, a).Status != "starting" {
		t.Fatalf("cross-owner stop: %v", err)
	}
	if j, err := Stop(s, "web", "b", false); err != nil || j.ID != b || j.Status != "stopped" || status(t, s, a).Status != "starting" {
		t.Fatalf("b stops web: %+v %v", j, err)
	}
	for _, id := range []string{a, shared} {
		if j, err := Stop(s, id, "b", id == a); err != nil || j.Status != "stopped" {
			t.Fatalf("stop %s: %+v %v", id, j, err)
		}
	}
}
