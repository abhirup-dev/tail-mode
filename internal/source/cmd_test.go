package source

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/abhirup-dev/tail-mode/internal/transport"
)

func cmdWith(t *testing.T, c CmdConfig) Source {
	t.Helper()
	cfg, _ := json.Marshal(c)
	src, err := New(Spec{Kind: "cmd", Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func recordArgv(t *testing.T, src Source) []string {
	t.Helper()
	var r argvRecorder
	if err := src.Stream(context.Background(), &r, false, func([]string) {}); err != nil {
		t.Fatal(err)
	}
	return r.argv
}

func TestCmdArgv(t *testing.T) {
	got := recordArgv(t, cmdWith(t, CmdConfig{Cmd: "npm run dev"}))
	if want := []string{"bash", "-c", "export PYTHONUNBUFFERED=1\nnpm run dev"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("default argv %q", got)
	}
	got = recordArgv(t, cmdWith(t, CmdConfig{
		Cmd: "make", Dir: "/srv/a b", Shell: "/bin/zsh", Login: true,
		Env: map[string]string{"Z": "x'y $HOME", "A_1": "", "PYTHONUNBUFFERED": "0"},
	}))
	want := []string{"/bin/zsh", "-lc", "cd -- '/srv/a b' || exit 97\n" +
		"export A_1=''\nexport PYTHONUNBUFFERED=0\nexport Z='x'\"'\"'y $HOME'\nmake"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv %q\nwant %q", got, want)
	}
	for dir, cd := range map[string]string{"~": `cd -- "$HOME" || exit 97`, "~/a b": `cd -- "$HOME"/'a b' || exit 97`} {
		if got := recordArgv(t, cmdWith(t, CmdConfig{Cmd: "x", Dir: dir})); !strings.HasPrefix(got[2], cd+"\n") {
			t.Errorf("dir %q: script %q", dir, got[2])
		}
	}
}

func TestCmdConfigValidation(t *testing.T) {
	for _, c := range []string{
		`{"cmd":"x","env":{"1A":"v"}}`,
		`{"cmd":"x","env":{"A-B":"v"}}`,
		`{"cmd":"x","env":{"":"v"}}`,
		`{"cmd":"x","shell":"bash -e"}`,
		`{"cmd":"x","shell":"bin/bash"}`,
		`{"cmd":"x","shell":"-bash"}`,
		`{"cmd":"x","dir":"~root/x"}`,
		`{"cmd":"x","login":"yes"}`,
		`{"dir":"/tmp"}`,
	} {
		if _, err := New(Spec{Kind: "cmd", Config: json.RawMessage(c)}); err == nil {
			t.Errorf("config %s accepted", c)
		}
	}
	for _, c := range []string{`{"cmd":"x","shell":"zsh"}`, `{"cmd":"x","shell":"/usr/bin/env"}`, `{"cmd":"x","env":{"_a9":"v"}}`} {
		if _, err := New(Spec{Kind: "cmd", Config: json.RawMessage(c)}); err != nil {
			t.Errorf("config %s: %v", c, err)
		}
	}
}

func TestCmdDirEnvLocally(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.Mkdir(filepath.Join(home, "a b"), 0o700); err != nil {
		t.Fatal(err)
	}
	var c collector
	err := cmdWith(t, CmdConfig{
		Cmd: `pwd; printf '%s|%s\n' "$GREETING" "$PYTHONUNBUFFERED"; python3 -c 'import sys; print("py", sys.stdout.line_buffering)' 2>/dev/null || true`,
		Dir: "~/a b", Shell: "sh", Env: map[string]string{"GREETING": "x'y $HOME"},
	}).Stream(context.Background(), local(t), false, c.emit)
	if err != nil {
		t.Fatal(err)
	}
	got := c.get()
	wantDir, _ := filepath.EvalSymlinks(filepath.Join(home, "a b"))
	if len(got) < 2 || got[1] != "x'y $HOME|1" {
		t.Fatalf("lines %q", got)
	}
	if d, _ := filepath.EvalSymlinks(got[0]); d != wantDir {
		t.Fatalf("pwd %q, want %q", got[0], wantDir)
	}
}

func TestCmdMissingDirExits97(t *testing.T) {
	err := cmdWith(t, CmdConfig{Cmd: "echo ran", Dir: filepath.Join(t.TempDir(), "nope")}).
		Stream(context.Background(), local(t), false, func([]string) {})
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 97 {
		t.Fatalf("want exit 97, got %v", err)
	}
}

func TestFileTildePath(t *testing.T) {
	src, err := New(Spec{Kind: "file", Config: json.RawMessage(`{"path":"~/logs/a b.log","backlog":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	if got := recordArgv(t, src); !reflect.DeepEqual(got, []string{"sh", "-c", `exec tail -n 3 -F -- "$HOME"/'logs/a b.log'`}) {
		t.Fatalf("argv %q", got)
	}

	home := t.TempDir()
	t.Setenv("HOME", home)
	os.Mkdir(filepath.Join(home, "logs"), 0o700)
	os.WriteFile(filepath.Join(home, "logs", "a b.log"), []byte("1\n2\n3\n4\n"), 0o600)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var c collector
	done := make(chan error, 1)
	go func() { done <- src.Stream(ctx, local(t), false, c.emit) }()
	waitFor(t, func() bool { return len(c.get()) == 3 })
	cancel()
	<-done
	if got := c.get(); !reflect.DeepEqual(got, []string{"2", "3", "4"}) {
		t.Fatalf("lines %q", got)
	}
}

func TestLocalDefaults(t *testing.T) {
	cwd := "/work/proj"
	restart := true
	cases := []struct{ kind, in, want string }{
		{"cmd", `{"cmd":"x"}`, `{"cmd":"x","dir":"/work/proj"}`},
		{"cmd", `{"cmd":"x","dir":"sub"}`, `{"cmd":"x","dir":"/work/proj/sub"}`},
		{"cmd", `{"cmd":"x","dir":"/abs"}`, `{"cmd":"x","dir":"/abs"}`},
		{"cmd", `{"cmd":"x","dir":"~/a","env":{"A":"1"},"login":true}`, `{"cmd":"x","dir":"~/a","env":{"A":"1"},"login":true}`},
		{"file", `{"path":"app.log"}`, `{"path":"/work/proj/app.log"}`},
		{"file", `{"path":"../x.log","backlog":0}`, `{"path":"/work/x.log","backlog":0}`},
		{"file", `{"path":"~/x.log"}`, `{"path":"~/x.log"}`},
		{"file", `{"path":"/var/log/x"}`, `{"path":"/var/log/x"}`},
	}
	for _, c := range cases {
		got, err := LocalDefaults(Spec{Kind: c.kind, Config: json.RawMessage(c.in), Restart: &restart}, cwd)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if string(got.Config) != c.want || got.Kind != c.kind || got.Restart != &restart {
			t.Errorf("%s → %s (want %s)", c.in, got.Config, c.want)
		}
	}
	if _, err := LocalDefaults(Spec{Kind: "tmux"}, cwd); err == nil {
		t.Error("unknown kind accepted")
	}
}

func TestClean(t *testing.T) {
	for _, c := range []struct {
		in, want string
		ok       bool
	}{
		{"", "", true},
		{"\r", "", true}, // blank CRLF line
		{"plain", "plain", true},
		{"a\tb", "a\tb", true},
		{"\x1b[1ma\tb\x1b[0m", "a\tb", true}, // ansi.Strip keeps tabs
		{"crlf\r", "crlf", true},
		{"done\r\r", "done", true},
		{"10%\r50%\r100%", "100%", true},
		{"10%\r50%\r100%\r", "100%", true},
		{"\x1b[32mok\x1b[0m done", "ok done", true},
		{"\x1b]8;;http://x\x1b\\link\x1b]8;;\x1b\\", "link", true},
		{"\x1b[2K\r", "", false},
		{"50%\r\x1b[2K", "", false},
		{"\x1b[0m", "", false},
		{"héllo \x1b[1mwörld\x1b[0m", "héllo wörld", true},
	} {
		if got, ok := clean([]byte(c.in)); got != c.want || ok != c.ok {
			t.Errorf("clean(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestStreamHygiene(t *testing.T) {
	var c collector
	err := cmdSource(t, `printf 'start\n\n10%%\r50%%\r100%%\n\033[31mred\033[0m\n\033[2K\r\nend\r\n'`).
		Stream(context.Background(), local(t), false, c.emit)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.get(); !reflect.DeepEqual(got, []string{"start", "", "100%", "red", "end"}) {
		t.Fatalf("lines %q", got)
	}
}

// ---- live ssh (TAIL_MODE_SSH_TEST_HOST)

func TestSSHCmdDirEnvLogin(t *testing.T) {
	tr := sshHost(t)
	var c collector
	err := cmdWith(t, CmdConfig{
		Cmd: `pwd; printf '%s|%s\n' "$GREETING" "$PYTHONUNBUFFERED"; shopt -q login_shell && echo login`,
		Dir: "~", Env: map[string]string{"GREETING": "x'y $HOME\tz"}, Login: true,
	}).Stream(context.Background(), tr, false, c.emit)
	if err != nil {
		t.Fatal(err)
	}
	got := c.get()
	if len(got) != 3 || !strings.HasPrefix(got[0], "/") || got[1] != "x'y $HOME\tz|1" || got[2] != "login" {
		t.Fatalf("lines %q", got)
	}
	err = cmdWith(t, CmdConfig{Cmd: "echo ran", Dir: "/nonexistent-tail-mode"}).Stream(context.Background(), tr, false, func([]string) {})
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 97 {
		t.Fatalf("missing dir: want exit 97, got %v", err)
	}
}

func TestSSHFileTilde(t *testing.T) {
	tr := sshHost(t)
	name := "tail-mode-test-" + strconv.Itoa(os.Getpid()) + " x.log"
	run := func(script string) {
		t.Helper()
		if err := cmdSource(t, script).Stream(context.Background(), tr, false, func([]string) {}); err != nil {
			t.Fatal(err)
		}
	}
	run(`printf 'one\ntwo\n' > "$HOME"/` + transport.Quote(name))
	t.Cleanup(func() { run(`rm -f -- "$HOME"/` + transport.Quote(name)) })
	src, err := New(Spec{Kind: "file", Config: json.RawMessage(`{"path":` + strconv.Quote("~/"+name) + `}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var c collector
	done := make(chan error, 1)
	go func() { done <- src.Stream(ctx, tr, false, c.emit) }()
	waitFor(t, func() bool { return len(c.get()) == 2 })
	cancel()
	<-done
	if got := c.get(); !reflect.DeepEqual(got, []string{"one", "two"}) {
		t.Fatalf("lines %q", got)
	}
}
