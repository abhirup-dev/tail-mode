package proc

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
	p := filepath.Join(t.TempDir(), "run", "j1.lock")
	release, err := Lock(p)
	if err != nil {
		t.Fatal(err)
	}
	// flock locks belong to the open file description, so a second open in this process conflicts.
	if !Locked(p) || !Locked(p) {
		t.Fatal("held lock not seen (or probe disturbed it)")
	}
	start := time.Now()
	if _, err := Lock(p); err == nil || time.Since(start) < time.Second {
		t.Fatalf("second Lock should retry ~2 s then fail: %v after %v", err, time.Since(start))
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
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("no sleep")
	}
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
