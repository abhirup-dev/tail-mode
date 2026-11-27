package source

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/abhirupdas/tail-mode/internal/transport"
)

type collector struct {
	mu    sync.Mutex
	lines []string
}

func (c *collector) emit(b []string) { c.mu.Lock(); c.lines = append(c.lines, b...); c.mu.Unlock() }
func (c *collector) get() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func local(t *testing.T) transport.Transport {
	t.Helper()
	tr, err := transport.New("", "")
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func cmdSource(t *testing.T, cmd string) Source {
	t.Helper()
	cfg, _ := json.Marshal(CmdConfig{Cmd: cmd})
	src, err := New(Spec{Kind: "cmd", Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestCmdStreamAndExitCode(t *testing.T) {
	var c collector
	err := cmdSource(t, "printf 'a\\nb\\r\\n'; echo err >&2; exit 3").Stream(context.Background(), local(t), false, c.emit)
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 3 {
		t.Fatalf("want exit 3, got %v", err)
	}
	if got := c.get(); !reflect.DeepEqual(got, []string{"a", "b", "err"}) {
		t.Fatalf("lines %q", got)
	}
}

func TestLongLineIsCutAndIngestionContinues(t *testing.T) {
	var c collector
	err := cmdSource(t, `echo before; head -c 1100000 /dev/zero | tr '\0' x; printf '\nafter\n'`).
		Stream(context.Background(), local(t), false, c.emit)
	if err != nil {
		t.Fatal(err)
	}
	got := c.get()
	if len(got) != 3 || got[0] != "before" || got[2] != "after" || got[1] != strings.Repeat("x", maxLine) {
		lens := make([]int, len(got))
		for i, l := range got {
			lens[i] = len(l)
		}
		t.Fatalf("got %d lines with lengths %v", len(got), lens)
	}
}

func TestLeaderExitEndsStreamDespiteBackgroundChild(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "bg.pid")
	var c collector
	start := time.Now()
	err := cmdSource(t, "sleep 30 & echo $! > "+pidFile+"; echo hi; exit 23").
		Stream(context.Background(), local(t), false, c.emit)
	if took := time.Since(start); took > 3500*time.Millisecond {
		t.Fatalf("stream took %v", took)
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 23 {
		t.Fatalf("want exit 23, got %v", err)
	}
	if got := c.get(); !reflect.DeepEqual(got, []string{"hi"}) {
		t.Fatalf("lines %q", got)
	}
	b, _ := os.ReadFile(pidFile)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	waitFor(t, func() bool { return pid > 0 && syscall.Kill(pid, 0) != nil }) // group was killed
}

// rawTransport leaves Stdin nil, like ssh, so stream must hold a silent stdin open.
type rawTransport struct{}

func (rawTransport) Name() string   { return "raw" }
func (rawTransport) Target() string { return "" }
func (rawTransport) Command(ctx context.Context, argv ...string) *exec.Cmd {
	return exec.CommandContext(ctx, argv[0], argv[1:]...)
}

func TestStdin(t *testing.T) {
	// read -t returns > 128 on timeout and 1 on EOF.
	run := func(tr transport.Transport) string {
		var c collector
		if err := cmdSource(t, `read -t 1 x; echo "rc=$?"`).Stream(context.Background(), tr, false, c.emit); err != nil {
			t.Fatal(err)
		}
		got := c.get()
		if len(got) != 1 {
			t.Fatalf("lines %q", got)
		}
		return got[0]
	}
	if got := run(local(t)); got != "rc=1" {
		t.Fatalf("local: want EOF (rc=1), got %q", got)
	}
	if got := run(rawTransport{}); got == "rc=1" || got == "rc=0" {
		t.Fatalf("held-open: want timeout, got %q", got)
	}
}

func TestStreamDoesNotLeakFDs(t *testing.T) {
	count := func() int { es, _ := os.ReadDir("/dev/fd"); return len(es) }
	src := cmdSource(t, "echo x")
	src.Stream(context.Background(), local(t), false, func([]string) {}) // warm up runtime fds (poller etc.)
	before := count()
	for range 10 {
		if err := src.Stream(context.Background(), local(t), false, func([]string) {}); err != nil {
			t.Fatal(err)
		}
	}
	if after := count(); after > before {
		t.Fatalf("fds %d → %d", before, after)
	}
}

func TestFileStreamCancelKillsGroup(t *testing.T) {
	p := filepath.Join(t.TempDir(), "log")
	os.WriteFile(p, []byte("old1\nold2\nold3\n"), 0o600)
	src, err := New(Spec{Kind: "file", Config: json.RawMessage(`{"path":` + strconv.Quote(p) + `,"backlog":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var c collector
	done := make(chan error)
	go func() { done <- src.Stream(ctx, local(t), false, c.emit) }()
	waitFor(t, func() bool { return len(c.get()) == 2 })
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("new\n")
	f.Close()
	waitFor(t, func() bool { return len(c.get()) == 3 })
	if got := c.get(); !reflect.DeepEqual(got, []string{"old2", "old3", "new"}) {
		t.Fatalf("lines %q", got)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not stop on cancel")
	}
}

// argvRecorder is a transport that records argv and runs `true`.
type argvRecorder struct{ argv []string }

func (*argvRecorder) Name() string   { return "rec" }
func (*argvRecorder) Target() string { return "" }
func (r *argvRecorder) Command(ctx context.Context, argv ...string) *exec.Cmd {
	r.argv = argv
	return exec.CommandContext(ctx, "true")
}

func TestFileConfig(t *testing.T) {
	argv := func(cfg string, resumed bool) []string {
		t.Helper()
		src, err := New(Spec{Kind: "file", Config: json.RawMessage(cfg)})
		if err != nil {
			t.Fatal(err)
		}
		var r argvRecorder
		if err := src.Stream(context.Background(), &r, resumed, func([]string) {}); err != nil {
			t.Fatal(err)
		}
		return r.argv
	}
	if got := argv(`{"path":"-x.log"}`, false); !reflect.DeepEqual(got, []string{"tail", "-n", "50", "-F", "--", "-x.log"}) {
		t.Fatalf("default backlog argv %q", got)
	}
	if got := argv(`{"path":"a","backlog":0}`, false); got[2] != "0" {
		t.Fatalf("backlog 0 argv %q", got)
	}
	if got := argv(`{"path":"a","backlog":9}`, true); got[2] != "0" {
		t.Fatalf("resumed argv %q", got)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, s := range []Spec{
		{Kind: "file"},
		{Kind: "file", Config: json.RawMessage(`{"path":""}`)},
		{Kind: "file", Config: json.RawMessage(`{"path":"a","backlog":-1}`)},
		{Kind: "file", Config: json.RawMessage(`{"paht":"a"}`)},
		{Kind: "file", Config: json.RawMessage(`{"path":"a"} x`)},
		{Kind: "cmd", Config: json.RawMessage(`{}`)},
		{Kind: "cmd", Config: json.RawMessage(`"echo"`)},
		{Kind: "tmux"},
	} {
		if _, err := New(s); err == nil {
			t.Errorf("New(%s %s) succeeded", s.Kind, s.Config)
		}
	}
	if got := Kinds(); !reflect.DeepEqual(got, []string{"cmd", "file"}) {
		t.Fatalf("Kinds() = %q", got)
	}
}

type failingReader struct{ n int }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.n == 0 {
		r.n++
		return copy(p, "one\ntw"), nil
	}
	return 0, io.ErrUnexpectedEOF
}

func TestReadLinesReturnsReadErrors(t *testing.T) {
	out := make(chan string, 10)
	if err := readLines(&failingReader{}, out); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err %v", err)
	}
	close(out)
	var got []string
	for l := range out {
		got = append(got, l)
	}
	if !reflect.DeepEqual(got, []string{"one", "tw"}) {
		t.Fatalf("lines %q", got)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("timeout")
}
