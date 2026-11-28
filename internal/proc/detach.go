// Package proc starts and stops detached tail-mode child processes (job workers, followers) and holds the
// lock files that prove a worker is alive.
package proc

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// Detach re-execs this binary with args in a new session, stdin /dev/null, stdout/stderr appended to
// <dir>/proc.log (dir: the state dir), and returns its pid without waiting. The child outlives the caller.
func Detach(dir string, args ...string) (int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, err
	}
	logf, err := os.OpenFile(filepath.Join(dir, "proc.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	defer logf.Close()
	cmd := exec.Command(exe, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	go cmd.Wait() // reap if we live long enough (e.g. inside the MCP server)
	return pid, nil
}

// Terminate ends pid, the holder of lockPath: SIGTERM, wait ≤ grace for the lock to free, then SIGKILL
// (its process group if group) and wait ≤ 1 s. It signals only while the lock is held (PID-reuse safe)
// and reports whether the lock is free.
func Terminate(pid int, lockPath string, group bool, grace time.Duration) bool {
	if pid <= 0 || !Locked(lockPath) {
		return true
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	if WaitUnlocked(lockPath, grace) {
		return true
	}
	if group {
		pid = -pid
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	return WaitUnlocked(lockPath, time.Second)
}
