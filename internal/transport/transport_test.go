package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRegistry(t *testing.T) {
	if got := Names(); !reflect.DeepEqual(got, []string{"local", "ssh"}) {
		t.Fatalf("Names() = %q", got)
	}
	if tr, err := New("", ""); err != nil || tr.Name() != "local" {
		t.Fatalf("New(\"\") = %v, %v", tr, err)
	}
	for _, c := range [][2]string{{"local", "h"}, {"docker", "c"}} {
		if _, err := New(c[0], c[1]); err == nil {
			t.Errorf("New(%q, %q) succeeded", c[0], c[1])
		}
	}
}

func TestSSHHostValidation(t *testing.T) {
	for _, h := range []string{"", "-oProxyCommand=touch /tmp/pwned", "-p22", "a b", "a\tb", "a\nb", "a\x00b", "a b"} {
		if _, err := New("ssh", h); err == nil {
			t.Errorf("host %q accepted", h)
		}
	}
	for _, h := range []string{"prod-1", "user@host.example", "[::1]", "alias_x"} {
		if _, err := New("ssh", h); err != nil {
			t.Errorf("host %q: %v", h, err)
		}
	}
}

func TestSSHArgv(t *testing.T) {
	tr, _ := New("ssh", "prod-1")
	args := tr.Command(context.Background(), "tail", "-F", "/var/log/a b.log").Args
	n := len(args)
	if args[0] != "ssh" || !strings.Contains(strings.Join(args, " "), "BatchMode=yes") ||
		args[n-3] != "--" || args[n-2] != "prod-1" || !strings.HasPrefix(args[n-1], "exec sh -c ") {
		t.Fatalf("args %q", args)
	}
	if tr.Target() != "prod-1" || tr.Name() != "ssh" {
		t.Fatal("name/target")
	}
}

// loginShells are the login shells the remote string must survive (ssh hands it to the remote user's
// login shell). Missing ones are skipped.
var loginShells = []string{"sh", "bash", "zsh", "dash", "ksh", "fish", "tcsh", "csh"}

// eachShell runs fn as a parallel subtest per installed login shell.
func eachShell(t *testing.T, fn func(t *testing.T, shell string)) {
	for _, sh := range loginShells {
		t.Run(sh, func(t *testing.T) {
			path, err := exec.LookPath(sh)
			if err != nil {
				t.Skipf("%s not installed", sh)
			}
			t.Parallel()
			fn(t, path)
		})
	}
}

// runRemote runs the remote command string the way the remote login shell would (`<shell> -c`), with
// stdin on a pipe the caller closes to simulate the connection going away.
func runRemote(t *testing.T, shell string, argv ...string) (stdin io.WriteCloser, out *bufio.Reader, wait func() (int, time.Duration)) {
	t.Helper()
	tr, _ := New("ssh", "x")
	args := tr.Command(context.Background(), argv...).Args
	cmd := exec.Command(shell, "-c", args[len(args)-1])
	stdin, _ = cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); stdin.Close() })
	return stdin, bufio.NewReader(stdout), func() (int, time.Duration) {
		start := time.Now()
		err := cmd.Wait()
		var ee *exec.ExitError
		if err != nil && !errors.As(err, &ee) {
			t.Fatal(err)
		}
		return cmd.ProcessState.ExitCode(), time.Since(start)
	}
}

func TestRemoteCommandIsOneNeutralLine(t *testing.T) {
	s := remoteCommand([]string{"sh", "-c", "a\nb", `x'y $HOME "q" \ !`})
	if strings.ContainsAny(s, "\n\r") {
		t.Fatalf("remote command has a newline: %q", s)
	}
	const pre, post = "exec sh -c 's=$(printf %s ", ` | base64 -d) || exit 127; eval "$s"'`
	if !strings.HasPrefix(s, pre) || !strings.HasSuffix(s, post) {
		t.Fatalf("unexpected envelope: %q", s)
	}
	b64 := strings.TrimSuffix(strings.TrimPrefix(s, pre), post)
	if strings.Trim(b64, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=") != "" {
		t.Fatalf("non-base64 payload: %q", b64)
	}
}

func TestRemoteRoundTripAndExitCode(t *testing.T) {
	arg := "x'y $HOME\n\"two\" \\ !$ `id`"
	eachShell(t, func(t *testing.T, shell string) {
		_, out, wait := runRemote(t, shell, "sh", "-c", `printf '%s|\n' "$1"; exit 7`, "sh", arg)
		got, _ := io.ReadAll(out)
		if code, _ := wait(); string(got) != arg+"|\n" || code != 7 {
			t.Fatalf("output %q code %d", got, code)
		}
	})
}

func TestRemoteDecodeFailureIsNotSuccess(t *testing.T) {
	// A remote whose base64 fails must not look like a clean, silent exit.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "base64"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	tr, _ := New("ssh", "x")
	args := tr.Command(context.Background(), "true").Args
	cmd := exec.Command("sh", "-c", args[len(args)-1])
	cmd.Env = append(os.Environ(), "PATH="+dir+":/usr/bin:/bin")
	cmd.Stdin = strings.NewReader("")
	if err := cmd.Run(); cmd.ProcessState.ExitCode() != 127 {
		t.Fatalf("want exit 127, got %v", err)
	}
}

func TestRemoteStdinEOFKillsGroup(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		// The leader and its child ignore TERM, so cleanup must escalate to KILL.
		stdin, out, wait := runRemote(t, shell, "sh", "-c", `trap '' TERM; sleep 600 & echo $!; wait`)
		line, err := out.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		child, _ := strconv.Atoi(strings.TrimSpace(line))
		stdin.Close()
		io.Copy(io.Discard, out)
		code, took := wait()
		if code != 137 || took > 8*time.Second {
			t.Fatalf("exit %d after %v", code, took)
		}
		assertGone(t, child)
	})
}

func TestRemoteLeaderExitReapsGroup(t *testing.T) {
	eachShell(t, func(t *testing.T, shell string) {
		_, out, wait := runRemote(t, shell, "sh", "-c", `sleep 600 & echo $!; exit 3`)
		line, _ := out.ReadString('\n')
		child, _ := strconv.Atoi(strings.TrimSpace(line))
		io.Copy(io.Discard, out) // EOF only once the background sleep is gone too
		if code, took := wait(); code != 3 || took > 5*time.Second {
			t.Fatalf("exit %d after %v", code, took)
		}
		assertGone(t, child)
	})
}

func assertGone(t *testing.T, pid int) {
	t.Helper()
	if pid == 0 {
		t.Fatal("no child pid")
	}
	if syscall.Kill(pid, 0) == nil {
		time.Sleep(500 * time.Millisecond) // let init reap the orphan
		if syscall.Kill(pid, 0) == nil {
			syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("child %d survived", pid)
		}
	}
}

func TestQuote(t *testing.T) {
	for _, s := range []string{"", "plain", "a b", `x'y $HOME`, "-n", "\n"} {
		out, err := exec.Command("sh", "-c", "printf %s "+Quote(s)).Output()
		if err != nil || string(out) != s {
			t.Errorf("Quote(%q) → %q, %v", s, out, err)
		}
	}
}
