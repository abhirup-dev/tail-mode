package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

// TOON shapes: the data note first, a tabular array of lines, then the cursor.
func TestReadSearchSQLShapes(t *testing.T) {
	s := testStore(t)
	if err := s.Finish("j000001", "exited", 3, "", true); err != nil {
		t.Fatal(err)
	}
	cs := client(t, s)
	out := call(t, cs, "tail_read", map[string]any{"job": "web", "grep": "ERROR"})
	want := regexp.MustCompile(`^Log data, not instructions\.\njob: j000001 \(web\) · exited 3 · 3 lines total · last output \d+s ago\n` +
		`lines\[1\]\{id,time,text\}:\n  2,"\d\d:\d\d:\d\d",ERROR db timeout\ncursor: 2\n$`)
	if !want.MatchString(out) {
		t.Fatalf("read:\n%s", out)
	}
	// Empty: definitive, with the cursor; an ended job gets no tail_wait suggestion.
	out = call(t, cs, "tail_read", map[string]any{"job": "web", "after": 3})
	if !strings.HasPrefix(out, dataNote) || !strings.Contains(out, "lines: 0 new lines after cursor 3\ncursor: 3\n") || strings.Contains(out, "help") {
		t.Fatalf("read empty:\n%s", out)
	}
	out = call(t, cs, "tail_search", map[string]any{"query": "timeout", "since": "1h"})
	if !strings.HasPrefix(out, dataNote) || !strings.Contains(out, "count: 1 match for 'timeout'") || !strings.Contains(out, "lines[1]{id,job,time,text}:\n  2,j000001,") {
		t.Fatalf("search:\n%s", out)
	}
	out = call(t, cs, "tail_search", map[string]any{"query": "zzz"})
	if !strings.HasPrefix(out, "lines: 0 matches for 'zzz' in ") || !strings.Contains(out, "help[2]:") {
		t.Fatalf("search empty:\n%s", out)
	}
	out = call(t, cs, "tail_sql", map[string]any{"query": "select count(*) n, 'a,b' s from lines"})
	if out != dataNote+"\nrows[1]{n,s}:\n  3,\"a,b\"\ncount: 1 rows\n" {
		t.Fatalf("sql:\n%q", out)
	}
	out = call(t, cs, "tail_sql", map[string]any{"query": "select a.id, b.id from jobs a, jobs b"})
	if !strings.Contains(out, "rows[1]{id,id_2}:") {
		t.Fatalf("sql duplicate columns:\n%s", out)
	}
	out = call(t, cs, "tail_sql", map[string]any{"query": "select id from jobs where 0"})
	if !strings.Contains(out, "rows: 0 rows\ncolumns[1]: id\n") {
		t.Fatalf("sql empty:\n%s", out)
	}
}

var rowRe = regexp.MustCompile(`(?m)^  (\d+),(?:j\d+,)?"\d\d:\d\d:\d\d",`)

// The budget keeps the data note first and the footer last, the header counts the rows shown, and the
// footer cursor is the last row shown.
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
	want := fmt.Sprintf("cursor: %s\nmore: %d lines not shown (output budget); continue with after=%s\n", last, 300-len(rows), last)
	if !strings.HasSuffix(out, want) || len(rows) < 50 || !strings.Contains(out, fmt.Sprintf("\nlines[%d]{id,time,text}:\n", len(rows))) {
		t.Fatalf("%d rows, footer: %q", len(rows), out[len(out)-160:])
	}

	out = call(t, cs, "tail_search", map[string]any{"query": "line", "limit": 300})
	rows = rowRe.FindAllStringSubmatch(out, -1)
	if len(out) > maxOut || !strings.HasPrefix(out, dataNote) || !strings.Contains(out, fmt.Sprintf("down to id %s ", rows[len(rows)-1][1])) ||
		!strings.Contains(out, fmt.Sprintf("\nlines[%d]{", len(rows))) {
		t.Fatalf("search: len %d\n%s", len(out), out[len(out)-200:])
	}

	out = call(t, cs, "tail_sql", map[string]any{"query": "select id, text from lines"})
	if len(out) > maxOut || !strings.HasPrefix(out, dataNote+"\nrows[") || !regexp.MustCompile(`count: "?\d+ of 300 rows shown \(output budget\); narrow the query"?\n$`).MatchString(out) {
		t.Fatalf("sql: len %d\n%s", len(out), out[len(out)-200:])
	}
}

func TestRenderOversizedRow(t *testing.T) {
	row := strings.Repeat("é", maxOut) // 2 bytes per rune: a byte cut would split one
	tb := table{key: "lines", head: "{text}:", rows: []string{"  " + row, "  next"}}
	out := render(dataNote+"\n", tb, func(n int) string { return "shown " + strconv.Itoa(n) + "\n" })
	if len(out) > maxOut || !utf8.ValidString(out) || !strings.HasPrefix(out, dataNote+"\nlines[1]{text}:\n") || !strings.HasSuffix(out, "…\nshown 1\n") {
		t.Fatalf("len %d, tail %q", len(out), out[len(out)-40:])
	}
}

func TestStartOpts(t *testing.T) {
	zero := 0
	o, err := startOpts(StartIn{File: "/var/log/a", Host: "vps", Backlog: &zero, Name: "a"})
	if err != nil || o.Transport != "ssh" || o.Target != "vps" || o.Spec.Kind != "file" || string(o.Spec.Config) != `{"path":"/var/log/a","backlog":0}` {
		t.Fatalf("%v %+v %s", err, o, o.Spec.Config)
	}
	o, err = startOpts(StartIn{Source: "cmd", Config: map[string]any{"cmd": "echo hi"}})
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
		{Source: "cmd", Config: "{bad"},
	} {
		_, err := startOpts(in)
		if err == nil {
			t.Errorf("want error for %+v", in)
		} else if s := err.Error(); !strings.HasPrefix(s, "error: ") || !strings.Contains(s, "\nhelp: ") {
			t.Errorf("unstructured error for %+v: %s", in, s)
		}
	}
}

// Identity comes from the server's env only: no tool takes thread, the env thread is the owner and
// the destination, and owner is only a fallback label.
func TestIdentityFromEnv(t *testing.T) {
	last, notes := fakes(t)

	t.Setenv("T3CODE_THREAD_ID", "thread-1")
	cs := client(t, testStore(t))
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		b, _ := json.Marshal(tool.InputSchema)
		if strings.Contains(string(b), `"thread"`) || !strings.Contains(string(b), `"owner"`) {
			t.Errorf("%s schema: want owner, no thread: %s", tool.Name, b)
		}
	}
	if text := callErr(t, cs, "tail_jobs", map[string]any{"thread": "t9"}); !strings.Contains(text, "thread") {
		t.Fatalf("thread param accepted: %s", text)
	}
	if out := call(t, cs, "tail_jobs", map[string]any{}); out != "jobs: 0 jobs for this thread\n"+helpBlock(`tail_start cmd="<command>" dir="<absolute dir>" runs one`, "all=true lists every owner's jobs") {
		t.Fatalf("jobs empty:\n%s", out)
	}
	out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "owner": "ignored"})
	if cwd, _ := os.Getwd(); last().Owner != "thread-1" || last().Cwd != cwd || !strings.Contains(out, "dir: "+cwd+"\n") || !strings.Contains(out, "owner: this T3 thread\n") ||
		!strings.Contains(out, "notifications: off; tail_notify sends digests → this T3 thread thread-1 (detected from your session; you do not need to pass it)") {
		t.Fatalf("owner %q:\n%s", last().Owner, out)
	}
	out = call(t, cs, "tail_notify", map[string]any{"job": "j000001"})
	if n := notes()[0]; n.Deliver != "t3:thread-1" || !strings.Contains(out, "notifications: on → this T3 thread thread-1 (detected from your session") {
		t.Fatalf("notify %+v:\n%s", n, out)
	}

	// No thread: owner is the label, notify returns the wake recipe (no error), results say so.
	t.Setenv("T3CODE_THREAD_ID", "")
	cs = client(t, testStore(t))
	out = call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs", "owner": "me", "notify_match": "ERR x"})
	if last().Owner != "me" || !strings.Contains(out, "no T3 thread detected") || !strings.Contains(out, " --every 30s --match 'ERR x' --after 0") ||
		!strings.Contains(out, "tail-mode wait j") || strings.Contains(out, "warning") || len(notes()) != 1 {
		t.Fatalf("owner %q:\n%s", last().Owner, out)
	}
	out = call(t, cs, "tail_notify", map[string]any{"job": "web"})
	if !strings.Contains(out, "no T3 thread detected so nothing was started") || !strings.Contains(out, "tail_wait job=j000001") || len(notes()) != 1 {
		t.Fatalf("notify without thread:\n%s", out)
	}
	out = call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs"})
	if last().Owner != "" || !strings.Contains(out, "owner: none (shared namespace)") || !strings.Contains(out, "warning: no T3 thread detected and no owner given") {
		t.Fatalf("shared namespace:\n%s", out)
	}
}

func TestStartNotifyFromZero(t *testing.T) {
	last, notes := fakes(t)
	t.Setenv("T3CODE_THREAD_ID", "thread-1")
	cs := client(t, testStore(t))
	out := call(t, cs, "tail_start", map[string]any{"file": "/x", "backlog": 5, "notify_every": "10s", "notify_match": "ERR"})
	if !strings.Contains(out, "started:\n  job: j000009\n") || !strings.Contains(out, "notifications: on → this T3 thread thread-1") ||
		!strings.Contains(out, "from the job's first line") || !strings.Contains(out, "help[2]:\n  Notifications on. End your turn now") ||
		strings.Contains(out, "tail_wait") {
		t.Fatal(out)
	}
	var cfg source.FileConfig
	if err := json.Unmarshal(last().Spec.Config, &cfg); err != nil || cfg.Path != "/x" || cfg.Backlog == nil || *cfg.Backlog != 5 {
		t.Fatalf("spec %+v: %v", last().Spec, err)
	}
	if n := notes()[0]; len(notes()) != 1 || n.After != 0 || n.Deliver != "t3:thread-1" || n.Match != "ERR" || n.Every.String() != "10s" {
		t.Fatalf("notifier opts %+v", notes())
	}
	// tail_notify on an existing job starts from now unless after is given.
	call(t, cs, "tail_notify", map[string]any{"job": "j000001"})
	call(t, cs, "tail_notify", map[string]any{"job": "j000001", "after": 2})
	if n := notes()[1]; n.After != -1 || n.Deliver != "t3:thread-1" {
		t.Fatalf("notify opts %+v", n)
	}
	if n := notes()[2]; n.After != 2 {
		t.Fatalf("notify opts %+v", n)
	}
}

func TestStartBadNotifyStartsNothing(t *testing.T) {
	last, _ := fakes(t)
	if _, err := (&handler{}).start(context.Background(), StartIn{Cmd: "x", Dir: "/x", NotifyMatch: "("}); err == nil || last().Spec.Kind != "" {
		t.Fatalf("err %v, started %+v", err, last())
	}
}

// Exit codes surface in tail_jobs and tail_read headers; multi-line commands stay on one row.
func TestJobsShowsExitCodeAndOneLineRows(t *testing.T) {
	s := testStore(t)
	if err := s.Finish("j000001", "exited", 3, "", true); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateJob(store.Job{ID: "j000002", Name: "multi", Kind: "cmd", Transport: "local", Spec: `{}`, Describe: "echo a\necho b", Status: "starting", StartedAt: store.NowMS()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish("j000002", "failed", 0, "boom\nmore", true); err != nil {
		t.Fatal(err)
	}
	out := call(t, client(t, s), "tail_jobs", map[string]any{})
	want := regexp.MustCompile(`^count: 2 jobs for the shared namespace \(no T3 thread detected; no owner given\) · newest first\n` +
		`jobs\[2\]\{id,name,status,lines,last_output,source\}:\n` +
		`  j000002,multi,"failed: boom ⏎ more",0,never,echo a ⏎ echo b\n` +
		`  j000001,web,exited 3,3,\d+s ago,x\n` +
		`help\[2\]:\n  tail_read job=<id> after=<cursor> reads output\n  tail_stop job=<id> stops a job\n$`)
	if !want.MatchString(out) {
		t.Fatalf("jobs:\n%s", out)
	}
	if out := call(t, client(t, s), "tail_jobs", map[string]any{"all": true}); !strings.Contains(out, "{id,name,status,lines,last_output,source,owner}") {
		t.Fatalf("jobs all:\n%s", out)
	}
}

// Stopping an ended job is a no-op, not an error.
func TestStopIdempotent(t *testing.T) {
	s := testStore(t)
	if err := s.Finish("j000001", "exited", 3, "", true); err != nil {
		t.Fatal(err)
	}
	cs := client(t, s)
	if out := call(t, cs, "tail_stop", map[string]any{"job": "web"}); out != "job: j000001 (web) already exited 3 (no-op)\n" {
		t.Fatalf("stop ended: %q", out)
	}
	// A starting job (no worker) is stopped; stopping it again is a no-op.
	if err := s.CreateJob(store.Job{ID: "j000003", Kind: "cmd", Transport: "local", Spec: `{}`, Status: "starting", StartedAt: store.NowMS()}); err != nil {
		t.Fatal(err)
	}
	if out := call(t, cs, "tail_stop", map[string]any{"job": "j000003"}); out != "job: j000003 stopped\n" {
		t.Fatalf("stop: %q", out)
	}
	if out := call(t, cs, "tail_stop", map[string]any{"job": "j000003"}); out != "job: j000003 already stopped (no-op)\n" {
		t.Fatalf("stop again: %q", out)
	}
	if text := callErr(t, cs, "tail_stop", map[string]any{"job": "nope"}); !strings.HasPrefix(text, "error: no job \"nope\"") || !strings.Contains(text, "\nhelp: tail_jobs") {
		t.Fatalf("not found: %s", text)
	}
}

// tail_wait: a match, a timeout on a live job, and a job that ended without a match.
func TestWait(t *testing.T) {
	s := testStore(t)
	if err := s.Finish("j000001", "exited", 3, "", true); err != nil {
		t.Fatal(err)
	}
	// A "starting" job inside its grace period counts as live without a worker.
	if err := s.CreateJob(store.Job{ID: "j000004", Name: "srv", Kind: "cmd", Transport: "local", Spec: `{}`, Status: "starting", StartedAt: store.NowMS()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("j000004", []string{"booting", "listening on :3000"}, store.NowMS()); err != nil {
		t.Fatal(err)
	}
	cs := client(t, s)

	out := call(t, cs, "tail_wait", map[string]any{"job": "srv", "until": "listening"})
	if !strings.HasPrefix(out, dataNote+"\nwait: matched /listening/ in job j000004 (srv)\nlines[1]{id,time,text}:\n  5,") || !strings.Contains(out, "cursor: 5\n") {
		t.Fatalf("match:\n%s", out)
	}
	out = call(t, cs, "tail_wait", map[string]any{"job": "srv", "until": "ready", "timeout": "1s"})
	if !strings.Contains(out, "wait: timed out after 1s; no line matched /ready/") || !strings.Contains(out, "last_lines[2]{id,time,text}:") ||
		!strings.Contains(out, "cursor: 5\n") || !strings.Contains(out, `tail_wait job=j000004 until="ready" after=5 keeps waiting`) ||
		!strings.Contains(out, "tail_notify job=j000004") {
		t.Fatalf("timeout:\n%s", out)
	}
	out = call(t, cs, "tail_wait", map[string]any{"job": "web", "until": "never"})
	if !strings.HasPrefix(out, dataNote+"\nwait: job ended without a line matching /never/\njob: j000001 (web) · exited 3 · 3 lines total") ||
		!strings.Contains(out, "last_lines[3]") || !strings.HasSuffix(out, "cursor: 3\n") {
		t.Fatalf("ended:\n%s", out)
	}
	// after skips earlier matches.
	out = call(t, cs, "tail_wait", map[string]any{"job": "web", "until": "ERROR", "after": 2})
	if !strings.Contains(out, "job ended without a line matching") {
		t.Fatalf("after:\n%s", out)
	}
	for args, want := range map[string]map[string]any{
		"over the 5m maximum":        {"job": "web", "until": "x", "timeout": "10m"},
		"is not a positive duration": {"job": "web", "until": "x", "timeout": "soon"},
		"non-empty regex":            {"job": "web", "until": "("},
	} {
		if text := callErr(t, cs, "tail_wait", want); !strings.Contains(text, args) || !strings.Contains(text, "\nhelp: ") {
			t.Errorf("%v: %s", want, text)
		}
	}
}

var cliMention = regexp.MustCompile("`?tail-mode (run|start|stop|jobs|read|search|sql|follow|wait|notify|mcp)\\b")

// Descriptions carry all guidance (clients drop server instructions) and name only tools, not CLI commands.
func TestDescriptions(t *testing.T) {
	cs := sharedClient(t, testStore(t))
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tool := range res.Tools {
		names[tool.Name] = true
		b, _ := json.Marshal(tool)
		if m := cliMention.FindString(string(b)); m != "" {
			t.Errorf("%s mentions the CLI (%q): %s", tool.Name, m, tool.Description)
		}
		if strings.Contains(string(b), "T3CODE_THREAD_ID") {
			t.Errorf("%s asks for the thread id", tool.Name)
		}
	}
	for _, n := range []string{"tail_start", "tail_wait", "tail_read", "tail_search", "tail_jobs", "tail_stop", "tail_notify", "tail_sql"} {
		if !names[n] {
			t.Errorf("missing tool %s", n)
		}
	}
	var start, wait string
	for _, tool := range res.Tools {
		switch tool.Name {
		case "tail_start":
			b, _ := json.Marshal(tool)
			start = string(b)
		case "tail_wait":
			wait = tool.Description
		}
	}
	for _, w := range []string{"Required: exactly one of cmd, file", "dir is required, an absolute path", "exported in the job's shell on the target host", "automatically"} {
		if !strings.Contains(start, w) {
			t.Errorf("tail_start lacks %q", w)
		}
	}
	if !strings.Contains(wait, "Required: job, until") || !strings.Contains(wait, "prefer tail_notify") {
		t.Errorf("tail_wait description: %s", wait)
	}
}

// tail_start takes host only (no transport/target, on any tool); host alone runs the job over ssh.
func TestStartHostOnly(t *testing.T) {
	last, _ := fakes(t)
	cs := client(t, testStore(t))
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		var schema struct {
			Properties map[string]any `json:"properties"`
		}
		b, _ := json.Marshal(tool.InputSchema)
		if err := json.Unmarshal(b, &schema); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{"transport", "target"} {
			if _, ok := schema.Properties[p]; ok {
				t.Errorf("%s has param %s", tool.Name, p)
			}
		}
		if tool.Name == "tail_start" {
			if _, ok := schema.Properties["host"]; !ok {
				t.Errorf("tail_start lacks host: %s", b)
			}
		}
	}
	out := call(t, cs, "tail_start", map[string]any{"cmd": "./build.sh", "host": "vps", "dir": "/srv/app", "owner": "me"})
	if o := last(); o.Transport != "ssh" || o.Target != "vps" || !strings.Contains(out, "on: ssh vps\n  dir: /srv/app\n") {
		t.Fatalf("%+v\n%s", o, out)
	}
	// Remote: an empty dir stays empty (the remote home).
	if out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "host": "vps"}); !strings.Contains(out, "dir: ~ (remote home)") {
		t.Fatalf("remote home:\n%s", out)
	}
	if text := callErr(t, cs, "tail_start", map[string]any{"cmd": "x", "host": "vps", "transport": "ssh"}); !strings.Contains(text, "transport") {
		t.Fatalf("transport accepted: %s", text)
	}
}

// With a T3 thread, a notify-on result tells the agent to end its turn and never suggests tail_wait.
func TestNotifyOnSaysEndTurn(t *testing.T) {
	fakes(t)
	t.Setenv("T3CODE_THREAD_ID", "thread-1")
	cs := client(t, testStore(t))
	for _, c := range []struct {
		tool string
		args map[string]any
	}{
		{"tail_start", map[string]any{"cmd": "make", "dir": "/abs", "notify_match": "ERROR"}},
		{"tail_start", map[string]any{"cmd": "make", "dir": "/abs", "notify_every": "30s"}},
		{"tail_notify", map[string]any{"job": "j000001"}},
	} {
		out := call(t, cs, c.tool, c.args)
		if !strings.Contains(out, "notifications: on") || !strings.Contains(out, "End your turn") || strings.Contains(out, "tail_wait") {
			t.Errorf("%s %v:\n%s", c.tool, c.args, out)
		}
	}
	// Notifications off: tail_wait stays a suggestion.
	if out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs"}); strings.Contains(out, "End your turn") || !strings.Contains(out, "tail_wait") {
		t.Errorf("notify off:\n%s", out)
	}
}

// MCP errors name the MCP param, not the CLI flag.
func TestStopForceWording(t *testing.T) {
	s := testStore(t)
	if err := s.CreateJob(store.Job{ID: "j0000ab", Owner: "a", Kind: "cmd", Transport: "local", Spec: `{}`, Status: "starting", StartedAt: store.NowMS()}); err != nil {
		t.Fatal(err)
	}
	cs := client(t, s)
	text := callErr(t, cs, "tail_stop", map[string]any{"job": "j0000ab", "owner": "b"})
	if !strings.Contains(text, "force: true") || strings.Contains(text, "--force") || strings.Contains(text, "CLI") {
		t.Fatalf("stop without force: %s", text)
	}
}

// `tail-mode tool` and MCP are one implementation: List is what MCP advertises, and Exec returns the
// MCP text (ids and ages normalized).
func TestListAndExecMatchMCP(t *testing.T) {
	fakes(t)
	s := testStore(t)
	cs := client(t, s)
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	canon := func(v any) string { // through any, so key order does not matter
		var x any
		b, _ := json.Marshal(v)
		_ = json.Unmarshal(b, &x)
		b, _ = json.Marshal(x)
		return string(b)
	}
	if got, want := canon(List()), canon(res.Tools); got != want {
		t.Fatalf("list:\n%s\nmcp:\n%s", got, want)
	}
	norm := regexp.MustCompile(`j[0-9a-f]{6}|\d+s ago`)
	h := &handler{s: testStore(t)} // its own store, so each side starts and stops its own job
	for _, c := range []struct {
		tool string
		args map[string]any
	}{
		{"tail_start", map[string]any{"cmd": "make", "dir": "/abs", "name": "b"}},
		{"tail_read", map[string]any{"job": "web"}},
		{"tail_jobs", map[string]any{}},
		{"tail_stop", map[string]any{"job": "b"}},
		{"tail_read", map[string]any{"job": "nope"}},
	} {
		mcpText, isErr, err := callResult(cs, c.tool, c.args)
		args, _ := json.Marshal(c.args)
		r, xerr := h.exec(context.Background(), c.tool, args)
		cliText := r.Text
		if xerr != nil {
			cliText = xerr.Error()
		}
		if err != nil || isErr != (xerr != nil) || norm.ReplaceAllString(mcpText, "X") != norm.ReplaceAllString(cliText, "X") {
			t.Errorf("%s %v: mcp (error %v):\n%s\nexec (error %v):\n%s", c.tool, c.args, isErr, mcpText, xerr, cliText)
		}
	}
}

// A client that delivers digests itself (Pi) gets the follower to run, not a T3 notifier or the recipe.
func TestPiNotify(t *testing.T) {
	_, notes := fakes(t)
	h := &handler{s: testStore(t), pi: true}
	r, err := h.exec(context.Background(), "tail_notify", []byte(`{"job":"web","match":"ERR"}`))
	want := []string{"follow", "--deliver=jsonl", "--after=-1", "--every=30s", "--max-lines=10", "--match=ERR", "--", "j000001"}
	if err != nil || r.Follow == nil || r.Follow.Job != "j000001" || !slices.Equal(r.Follow.Argv, want) || len(notes()) != 0 ||
		!strings.Contains(r.Text, "notifications: on → this Pi session") || !strings.Contains(r.Text, "End your turn") {
		t.Fatalf("%v %+v\n%s", err, r.Follow, r.Text)
	}
}
