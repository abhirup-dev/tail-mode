// Package testutil is what tests across packages share: temp stores and jobs, swapped package vars, and
// waiting on a condition instead of sleeping. (store's own tests cannot use it: it imports store.)
package testutil

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

// Store opens a store in a temp $TAIL_MODE_HOME (so detached children of the test binary share it).
func Store(t testing.TB) *store.Store {
	t.Helper()
	t.Setenv("TAIL_MODE_HOME", t.TempDir())
	s, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Job inserts j with lines (no worker) and returns the stored row. Zero fields default to a new id, kind
// cmd, transport local, spec {}, status starting (live during its grace period) and started now.
func Job(t testing.TB, s *store.Store, j store.Job, lines ...string) store.Job {
	t.Helper()
	def := func(p *string, v string) {
		if *p == "" {
			*p = v
		}
	}
	def(&j.ID, store.NewJobID())
	def(&j.Kind, "cmd")
	def(&j.Transport, "local")
	def(&j.Spec, "{}")
	def(&j.Status, "starting")
	if j.StartedAt == 0 {
		j.StartedAt = store.NowMS()
	}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if len(lines) > 0 {
		if err := s.Append(j.ID, lines, store.NowMS()); err != nil {
			t.Fatal(err)
		}
	}
	j, err := s.GetJob(j.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// LiveJob is Job, then holds its lock and claims it as running with this pid, as its worker would.
func LiveJob(t testing.TB, s *store.Store, j store.Job, lines ...string) store.Job {
	t.Helper()
	j.Status = "starting"
	j = Job(t, s, j, lines...)
	release, err := proc.Lock(s.JobLock(j.ID))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	if ok, err := s.Claim(j.ID, os.Getpid()); !ok || err != nil {
		t.Fatalf("claim %s: %v %v", j.ID, ok, err)
	}
	j, err = s.GetJob(j.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// Swap sets *p to v for the rest of the test.
func Swap[T any](t testing.TB, p *T, v T) {
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

// WaitFor polls cond until it holds, failing the test after timeout.
func WaitFor(t testing.TB, timeout time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(timeout); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("condition not met within %v", timeout)
		}
	}
}

// WaitDead waits up to 5 s for pid to exit (and be reaped).
func WaitDead(t testing.TB, pid int) {
	t.Helper()
	WaitFor(t, 5*time.Second, func() bool { return syscall.Kill(pid, 0) != nil })
}
