package cli

import (
	"syscall"
	"testing"
	"time"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/testutil"
)

// One notifier per (job, destination): a new one for the same destination replaces the old follower;
// different destinations coexist. Real detached followers (this test binary, see TestMain), local sinks
// only (their output goes to proc.log).
func TestNotifierReplacement(t *testing.T) {
	s := testutil.Store(t)
	j := testutil.LiveJob(t, s, store.Job{Name: "sleeper"})
	lock := func(d string) string { return follow.NotifierLock(s, j.ID, d) }
	t.Cleanup(func() { // signal only followers that still hold their lock (a dead one's pid may be reused)
		for _, d := range []string{"jsonl", "stdout"} {
			if n, ok, _ := s.GetNotifier(j.ID, d); ok {
				proc.Terminate(n.PID, lock(d), false, time.Second)
			}
		}
	})
	start := func(d string) int {
		t.Helper()
		pid, err := follow.StartNotifier(s, j.ID, follow.Options{Every: time.Minute, After: 0, Deliver: d})
		if err != nil {
			t.Fatalf("notifier %s: %v", d, err)
		}
		return pid
	}
	live := func() map[string]int {
		ns, err := s.ListNotifiers(j.ID)
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]int{}
		for _, n := range ns {
			if proc.Locked(lock(n.Deliver)) {
				m[n.Deliver] = n.PID
			}
		}
		return m
	}

	p1 := start("jsonl")
	p2 := start("jsonl") // same destination: replaces p1
	testutil.WaitDead(t, p1)
	p3 := start("stdout") // another destination: coexists with p2
	if m := live(); p1 == p2 || len(m) != 2 || m["jsonl"] != p2 || m["stdout"] != p3 {
		t.Fatalf("live notifiers %v, want jsonl=%d stdout=%d (p1 %d)", m, p2, p3, p1)
	}
	p4 := start("jsonl")
	testutil.WaitDead(t, p2)
	if m := live(); len(m) != 2 || m["jsonl"] != p4 || m["stdout"] != p3 {
		t.Fatalf("live notifiers %v, want jsonl=%d stdout=%d", m, p4, p3)
	}

	// A stopped follower removes its row.
	for _, p := range []int{p3, p4} {
		_ = syscall.Kill(p, syscall.SIGTERM) // both hold their locks (checked above)
		testutil.WaitDead(t, p)
	}
	if ns, _ := s.ListNotifiers(j.ID); len(ns) != 0 {
		t.Fatalf("rows left: %+v", ns)
	}
}
