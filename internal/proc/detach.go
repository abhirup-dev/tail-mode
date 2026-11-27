// Package proc starts detached tail-mode child processes (job workers, followers) and holds the
// lock files that prove a worker is alive.
package proc

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/abhirup-dev/tail-mode/internal/store"
)

// Detach re-execs this binary with args in a new session, stdin /dev/null, stdout/stderr appended to
// <store dir>/proc.log, and returns its pid without waiting. The child outlives the caller.
func Detach(args ...string) (int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, err
	}
	dir := store.Dir()
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
