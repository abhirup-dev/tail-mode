package source

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/abhirup-dev/tail-mode/internal/transport"
)

// Live checks against a real host: TAIL_MODE_SSH_TEST_HOST=myhost go test -run SSH ./internal/source/
func sshHost(t *testing.T) transport.Transport {
	h := os.Getenv("TAIL_MODE_SSH_TEST_HOST")
	if h == "" {
		t.Skip("TAIL_MODE_SSH_TEST_HOST not set")
	}
	tr, err := transport.New("ssh", h)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func remotePgrep(t *testing.T, tr transport.Transport, marker string) string {
	out, _ := exec.Command("ssh", "-o", "BatchMode=yes", "--", tr.Target(), "pgrep -af '"+marker+"' | grep -v pgrep").Output()
	return strings.TrimSpace(string(out))
}

func TestSSHExitCodeAndQuoting(t *testing.T) {
	tr := sshHost(t)
	var c collector
	err := cmdSource(t, `printf '%s\n' "x'y \$HOME"; exit 7`).Stream(context.Background(), tr, false, c.emit)
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 7 {
		t.Fatalf("want exit 7, got %v", err)
	}
	if got := c.get(); !reflect.DeepEqual(got, []string{`x'y $HOME`}) {
		t.Fatalf("lines %q", got)
	}
}

func TestSSHCancelLeavesNoRemoteProcess(t *testing.T) {
	tr := sshHost(t)
	n := 600000 + time.Now().UnixNano()%100000 // unique sleep lengths identify our remote processes
	marker := fmt.Sprintf("sleep %d", n)
	for _, cmd := range []string{
		fmt.Sprintf("echo up; sleep %d; echo done", n),
		fmt.Sprintf("trap '' TERM; echo up; sleep %d & sleep %d", n, n),
	} {
		ctx, cancel := context.WithCancel(context.Background())
		var c collector
		done := make(chan error, 1)
		go func() { done <- cmdSource(t, cmd).Stream(ctx, tr, false, c.emit) }()
		waitFor(t, func() bool { return len(c.get()) == 1 })
		if remotePgrep(t, tr, marker) == "" {
			t.Fatal("remote command not found while running")
		}
		cancel()
		<-done
		var left string
		for end := time.Now().Add(8 * time.Second); time.Now().Before(end); time.Sleep(500 * time.Millisecond) {
			if left = remotePgrep(t, tr, marker); left == "" {
				break
			}
		}
		if left != "" {
			t.Fatalf("%q: remote processes left:\n%s", cmd, left)
		}
	}
}
