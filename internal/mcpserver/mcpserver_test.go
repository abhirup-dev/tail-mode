package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func testStore(t *testing.T, lines ...string) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.CreateJob(store.Job{ID: "j000001", Name: "web", Kind: "cmd", Transport: "local", Spec: `{"kind":"cmd"}`, Describe: "x", Status: "exited", StartedAt: store.NowMS()}); err != nil {
		t.Fatal(err)
	}
	if len(lines) == 0 {
		lines = []string{"hello", "ERROR db timeout", "bye"}
	}
	if err := s.Append("j000001", lines, store.NowMS()); err != nil {
		t.Fatal(err)
	}
	return s
}

// client connects an MCP client to newServer(s) over in-memory transports.
func client(t *testing.T, s *store.Store) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	ct, st := mcp.NewInMemoryTransports()
	ss, err := newServer(s).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close(); ss.Wait() })
	return cs
}

func call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) string {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	text := res.Content[0].(*mcp.TextContent).Text
	if res.IsError {
		t.Fatalf("%s: tool error: %s", tool, text)
	}
	return text
}

func TestTools(t *testing.T) {
	cs := client(t, testStore(t))
	out := call(t, cs, "tail_read", map[string]any{"job": "web", "grep": "ERROR"})
	if !strings.HasPrefix(out, dataNote) || !strings.Contains(out, "ERROR db timeout") || strings.Contains(out, "hello") || !strings.Contains(out, "cursor 2 ") {
		t.Fatalf("read:\n%s", out)
	}
	out = call(t, cs, "tail_read", map[string]any{"job": "web", "after": 3})
	if !strings.Contains(out, "cursor 3 (nothing new") {
		t.Fatalf("read empty:\n%s", out)
	}
	out = call(t, cs, "tail_search", map[string]any{"query": "timeout", "since": "1h"})
	if !strings.HasPrefix(out, dataNote) || !strings.Contains(out, "1 lines") {
		t.Fatalf("search:\n%s", out)
	}
	out = call(t, cs, "tail_sql", map[string]any{"query": "select count(*) n from lines"})
	if !strings.HasPrefix(out, dataNote) || !strings.Contains(out, "\nn\n3\n(1 rows)") {
		t.Fatalf("sql:\n%q", out)
	}
}

var rowRe = regexp.MustCompile(`(?m)^(\d+) (?:j\d+ )?\d\d:\d\d:\d\d `)

// The budget keeps the data note first and the footer last, and the footer cursor is the last row shown.
func TestOutputBudget(t *testing.T) {
	long := make([]string, 300)
	for i := range long {
		long[i] = fmt.Sprintf("line %03d %s", i, strings.Repeat("x", 100))
	}
	cs := client(t, testStore(t, long...))

	out := call(t, cs, "tail_read", map[string]any{"job": "web", "limit": 500})
	if len(out) > maxOut || !strings.HasPrefix(out, dataNote) {
		t.Fatalf("len %d, head %q", len(out), out[:60])
	}
	rows := rowRe.FindAllStringSubmatch(out, -1)
	last := rows[len(rows)-1][1]
	want := fmt.Sprintf("cursor %s (pass as after for new lines) · %d more lines not shown (output budget); continue with after=%s\n", last, 300-len(rows), last)
	if !strings.HasSuffix(out, want) || len(rows) < 50 {
		t.Fatalf("%d rows, footer: %q", len(rows), out[len(out)-160:])
	}

	out = call(t, cs, "tail_search", map[string]any{"query": "line", "limit": 300})
	rows = rowRe.FindAllStringSubmatch(out, -1)
	if len(out) > maxOut || !strings.HasPrefix(out, dataNote) || !strings.Contains(out, fmt.Sprintf("down to id %s ", rows[len(rows)-1][1])) {
		t.Fatalf("search: len %d\n%s", len(out), out[len(out)-200:])
	}

	out = call(t, cs, "tail_sql", map[string]any{"query": "select id, text from lines"})
	if len(out) > maxOut || !strings.HasPrefix(out, dataNote+" [tail-mode] sql\nid\ttext\n") || !regexp.MustCompile(`\(\d+ of 300 rows shown; .*\)\n$`).MatchString(out) {
		t.Fatalf("sql: len %d\n%s", len(out), out[len(out)-200:])
	}
}

func TestRenderOversizedRow(t *testing.T) {
	row := strings.Repeat("é", maxOut) // 2 bytes per rune: a byte cut would split one
	out := render(dataNote, []string{row, "next"}, func(n int) string { return "shown " + strconv.Itoa(n) + "\n" })
	if len(out) > maxOut || !utf8.ValidString(out) || !strings.HasPrefix(out, dataNote+"\n") || !strings.HasSuffix(out, "…\nshown 1\n") {
		t.Fatalf("len %d, tail %q", len(out), out[len(out)-40:])
	}
}

func TestStartOpts(t *testing.T) {
	zero := 0
	o, err := startOpts(StartIn{File: "/var/log/a", Host: "vps", Backlog: &zero, Name: "a"})
	if err != nil || o.Transport != "ssh" || o.Target != "vps" || o.Spec.Kind != "file" || string(o.Spec.Config) != `{"path":"/var/log/a","backlog":0}` {
		t.Fatalf("%v %+v %s", err, o, o.Spec.Config)
	}
	o, err = startOpts(StartIn{Source: "cmd", Config: map[string]any{"cmd": "echo hi"}, Transport: "local"})
	if err != nil || o.Spec.Kind != "cmd" || string(o.Spec.Config) != `{"cmd":"echo hi"}` {
		t.Fatalf("%v %+v", err, o)
	}
	o, err = startOpts(StartIn{Source: "cmd", Config: `{"cmd":"echo hi"}`})
	if err != nil || string(o.Spec.Config) != `{"cmd":"echo hi"}` {
		t.Fatalf("string config: %v %s", err, o.Spec.Config)
	}
	for _, in := range []StartIn{
		{},
		{File: "a", Cmd: "b"},
		{File: "a", Source: "file"},
		{Cmd: "b", Config: map[string]any{}},
		{Cmd: "b", Backlog: &zero},
		{File: "a", Host: "h", Transport: "ssh"},
		{Source: "cmd", Config: "{bad"},
	} {
		if _, err := startOpts(in); err == nil {
			t.Errorf("want error for %+v", in)
		}
	}
}

// stubs replaces the job/notifier seams and returns the captured notifier options.
func stubs(t *testing.T) (*jobs.StartOpts, *[]follow.NotifierOpts) {
	var started jobs.StartOpts
	var notes []follow.NotifierOpts
	oldJob, oldNote := startJob, startNotifier
	t.Cleanup(func() { startJob, startNotifier = oldJob, oldNote })
	startJob = func(_ *store.Store, o jobs.StartOpts) (store.Job, error) {
		started = o
		return store.Job{ID: "j000009", Describe: "tail -F /x"}, nil
	}
	startNotifier = func(job string, o follow.NotifierOpts) (int, error) {
		if job != "j000009" && job != "j000001" {
			t.Errorf("notifier for %q", job)
		}
		notes = append(notes, o)
		return 4242, nil
	}
	return &started, &notes
}

func TestStartNotifyFromZero(t *testing.T) {
	started, notes := stubs(t)
	t.Setenv("T3CODE_THREAD_ID", "thread-1")
	cs := client(t, testStore(t))
	out := call(t, cs, "tail_start", map[string]any{"file": "/x", "backlog": 5, "notify_every": "10s", "notify_match": "ERR"})
	if !strings.Contains(out, "started j000009") || !strings.Contains(out, "notifier pid 4242") {
		t.Fatal(out)
	}
	var cfg source.FileConfig
	if err := json.Unmarshal(started.Spec.Config, &cfg); err != nil || cfg.Path != "/x" || cfg.Backlog == nil || *cfg.Backlog != 5 {
		t.Fatalf("spec %+v: %v", started.Spec, err)
	}
	if len(*notes) != 1 {
		t.Fatalf("notes %+v", *notes)
	}
	if n := (*notes)[0]; n.After != 0 || n.Deliver != "t3:thread-1" || n.Match != "ERR" || n.Every.String() != "10s" {
		t.Fatalf("notifier opts %+v", n)
	}

	// tail_notify on an existing job starts from now unless after is given.
	call(t, cs, "tail_notify", map[string]any{"job": "web", "thread": "other"})
	call(t, cs, "tail_notify", map[string]any{"job": "web", "after": 2})
	if n := (*notes)[1]; n.After != -1 || n.Deliver != "t3:other" {
		t.Fatalf("notify opts %+v", n)
	}
	if n := (*notes)[2]; n.After != 2 || n.Deliver != "t3:thread-1" {
		t.Fatalf("notify opts %+v", n)
	}
}

func TestStartBadNotifyStartsNothing(t *testing.T) {
	started, _ := stubs(t)
	if _, err := start(nil, StartIn{Cmd: "x", NotifyMatch: "("}); err == nil || started.Spec.Kind != "" {
		t.Fatalf("err %v, started %+v", err, *started)
	}
}

func TestNotifyRecipe(t *testing.T) {
	_, notes := stubs(t)
	t.Setenv("T3CODE_THREAD_ID", "")
	out, err := start(nil, StartIn{Cmd: "x", NotifyEvery: "30s", NotifyMatch: "ERR x"})
	if err != nil || !strings.Contains(out, "tail-mode follow j000009 --every 30s --match 'ERR x' --after 0") ||
		!strings.Contains(out, "tail-mode wait j000009 --until RE --timeout 30m --after 0") || len(*notes) != 0 {
		t.Fatalf("%v\n%s", err, out)
	}
	out, err = notify("j000001", 30e9, "", "", -1)
	if err != nil || strings.Contains(out, "--after") {
		t.Fatalf("%v\n%s", err, out)
	}
}

func TestStartCmdOptionsAndLocalDefaults(t *testing.T) {
	started, _ := stubs(t)
	t.Setenv("T3CODE_THREAD_ID", "")
	cs := client(t, testStore(t))
	call(t, cs, "tail_start", map[string]any{"cmd": "npm run dev", "env": map[string]any{"PORT": "3000"}, "shell": "zsh", "login": true})
	var c source.CmdConfig
	if err := json.Unmarshal(started.Spec.Config, &c); err != nil {
		t.Fatal(err)
	}
	cwd, _ := os.Getwd()
	if c.Cmd != "npm run dev" || c.Dir != cwd || c.Env["PORT"] != "3000" || c.Shell != "zsh" || !c.Login {
		t.Fatalf("local cmd config %+v", c)
	}

	// Remote: an empty dir stays empty (remote home), and paths are not touched.
	call(t, cs, "tail_start", map[string]any{"cmd": "make", "host": "vps"})
	if string(started.Spec.Config) != `{"cmd":"make"}` || started.Target != "vps" {
		t.Fatalf("remote spec %s", started.Spec.Config)
	}
	call(t, cs, "tail_start", map[string]any{"file": "logs/app.log"})
	if string(started.Spec.Config) != `{"path":"`+filepath.Join(cwd, "logs/app.log")+`"}` {
		t.Fatalf("local file spec %s", started.Spec.Config)
	}
	call(t, cs, "tail_start", map[string]any{"source": "cmd", "config": map[string]any{"cmd": "x"}})
	if string(started.Spec.Config) != `{"cmd":"x","dir":`+strconv.Quote(cwd)+`}` {
		t.Fatalf("generic cmd spec %s", started.Spec.Config)
	}

	for _, in := range []StartIn{
		{File: "a", Dir: "/x"},
		{File: "a", Login: true},
		{Source: "cmd", Config: map[string]any{"cmd": "x"}, Env: map[string]string{"A": "1"}},
	} {
		if _, err := startOpts(in); err == nil {
			t.Errorf("want error for %+v", in)
		}
	}
	if _, err := start(nil, StartIn{Cmd: "x", Env: map[string]string{"1A": "v"}}); err == nil {
		t.Error("invalid env name accepted")
	}
}
