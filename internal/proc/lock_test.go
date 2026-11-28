package proc

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMain doubles as a stubborn worker: `hold <lock>` takes the lock, ignores SIGTERM and sleeps.
func TestMain(m *testing.M) {
	if len(os.Args) == 3 && os.Args[1] == "hold" {
		signal.Ignore(syscall.SIGTERM)
		if _, err := Lock(os.Args[2]); err != nil {
			os.Exit(2)
		}
		time.Sleep(time.Minute)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestLockedMissingFileNotCreated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "run", "j1.lock")
	if Locked(p) {
		t.Fatal("missing lock reported held")
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("probe created the file: %v", err)
	}
}

func TestLockLifecycle(t *testing.T) {
	old := LockWait
	LockWait = 100 * time.Millisecond
	t.Cleanup(func() { LockWait = old })
	p := filepath.Join(t.TempDir(), "run", "j1.lock")
	release, err := Lock(p)
	if err != nil {
		t.Fatal(err)
	}
	// flock locks belong to the open file description, so a second open in this process conflicts.
	//lint:ignore SA4000 the second probe checks that the first did not disturb the lock
	if !Locked(p) || !Locked(p) {
		t.Fatal("held lock not seen (or probe disturbed it)")
	}
	start := time.Now()
	if _, err := Lock(p); err == nil || time.Since(start) < LockWait {
		t.Fatalf("second Lock should retry for LockWait then fail: %v after %v", err, time.Since(start))
	}
	release()
	if Locked(p) {
		t.Fatal("released lock still reported held")
	}
	r2, err := Lock(p)
	if err != nil {
		t.Fatal(err)
	}
	r2()
}

// A child process (e.g. the source command) must not inherit the worker's lock.
func TestLockNotInherited(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j1.lock")
	release, err := Lock(p)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("sleep", "5")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { child.Process.Kill(); child.Wait() }()
	release()
	if Locked(p) {
		out, _ := exec.Command("lsof", p).CombinedOutput()
		t.Fatalf("child kept the lock:\n%s", strings.TrimSpace(string(out)))
	}
}

// Terminate (jobs.Stop with group, notifier replacement without) escalates to SIGKILL when the lock
// holder ignores SIGTERM, and signals nothing once the lock is free.
func TestTerminateEscalates(t *testing.T) {
	for _, group := range []bool{false, true} {
		p := filepath.Join(t.TempDir(), "w.lock")
		child := exec.Command(os.Args[0], "hold", p)
		child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		if !waitLocked(p) {
			child.Process.Kill()
			t.Fatal("child never took the lock")
		}
		if !Terminate(child.Process.Pid, p, group, 100*time.Millisecond) {
			t.Errorf("group %v: Terminate reported failure", group)
		}
		var ee *exec.ExitError
		if err := child.Wait(); !errors.As(err, &ee) || ee.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Errorf("group %v: child ended with %v, want SIGKILL", group, err)
		}
		if !Terminate(child.Process.Pid, p, group, time.Millisecond) { // unlocked: a no-op, nothing signalled
			t.Errorf("group %v: Terminate of an unlocked pid failed", group)
		}
	}
}

func waitLocked(p string) bool {
	for end := time.Now().Add(5 * time.Second); !Locked(p); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			return false
		}
	}
	return true
}
