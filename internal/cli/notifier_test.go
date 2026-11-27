package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

// TestMain doubles as the detached children (proc.Detach re-execs this test binary): `_job <id>` job
// workers and `follow …` notifiers. T3CODE_THREAD_ID is cleared so nothing here can reach a real
// T3 thread, and so --owner defaults do not depend on where the tests run.
func TestMain(m *testing.M) {
	os.Unsetenv("T3CODE_THREAD_ID")
	if len(os.Args) > 1 && (os.Args[1] == "_job" || os.Args[1] == "follow") {
		if err := Run(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "test child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// alive reports whether pid still runs (and is not a zombie awaiting reaping by this process).
func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func waitDead(t *testing.T, pid int) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); alive(pid); time.Sleep(25 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("pid %d still alive", pid)
		}
	}
}

// One notifier per (job, destination): a new one for the same destination replaces the old follower;
// different destinations coexist. Real detached followers, local sinks only (stdout/jsonl go to proc.log).
func TestNotifierReplacement(t *testing.T) {
	home := t.TempDir()
	t.Setenv("TAIL_MODE_HOME", home)
	s, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cfg, _ := json.Marshal(source.CmdConfig{Cmd: "sleep 60", Dir: home})
	j, err := jobs.Start(s, jobs.StartOpts{Name: "sleeper", Spec: source.Spec{Kind: "cmd", Config: cfg}})
	if err != nil || j.Status != "running" {
		t.Fatalf("start: %v %+v", err, j)
	}
	var pids []int
	t.Cleanup(func() {
		for _, p := range pids {
			_ = syscall.Kill(p, syscall.SIGTERM)
		}
		_, _ = jobs.Stop(s, j.ID, "", true)
	})
	start := func(deliver string) int {
		t.Helper()
		pid, err := follow.StartNotifier(s, j.ID, follow.NotifierOpts{Every: time.Minute, After: 0, Deliver: deliver})
		if err != nil {
			t.Fatalf("notifier %s: %v", deliver, err)
		}
		pids = append(pids, pid)
		return pid
	}

	p1 := start("jsonl")
	p2 := start("jsonl") // same destination: replaces p1
	if p1 == p2 {
		t.Fatal("same pid")
	}
	waitDead(t, p1)
	p3 := start("stdout") // another destination: coexists with p2

	ns, err := s.ListNotifiers(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, n := range ns {
		got[n.Deliver] = n.PID
	}
	if len(ns) != 2 || got["jsonl"] != p2 || got["stdout"] != p3 {
		t.Fatalf("notifiers %+v, want jsonl=%d stdout=%d", ns, p2, p3)
	}
	for _, d := range []string{"jsonl", "stdout"} {
		if !proc.Locked(follow.NotifierLock(s, j.ID, d)) {
			t.Errorf("%s notifier not holding its lock", d)
		}
	}
	if !alive(p2) || !alive(p3) {
		t.Fatalf("live followers: p2 %v p3 %v", alive(p2), alive(p3))
	}

	// Replacing twice more still leaves exactly one follower per destination.
	p4 := start("jsonl")
	waitDead(t, p2)
	ns, _ = s.ListNotifiers(j.ID)
	var live []string
	for _, n := range ns {
		if alive(n.PID) {
			live = append(live, fmt.Sprintf("%s=%d", n.Deliver, n.PID))
		}
	}
	sort.Strings(live)
	if want := []string{fmt.Sprintf("jsonl=%d", p4), fmt.Sprintf("stdout=%d", p3)}; fmt.Sprint(live) != fmt.Sprint(want) {
		t.Fatalf("live %v, want %v", live, want)
	}

	// Followers end with the job and remove their rows.
	if _, err := jobs.Stop(s, j.ID, "", false); err != nil {
		t.Fatal(err)
	}
	for _, p := range []int{p3, p4} {
		_ = syscall.Kill(p, syscall.SIGTERM) // do not wait for a 1 min tick
		waitDead(t, p)
	}
	if ns, _ := s.ListNotifiers(j.ID); len(ns) != 0 {
		t.Fatalf("rows left: %+v", ns)
	}
}
