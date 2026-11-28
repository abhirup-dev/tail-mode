package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/testutil"
)

// has fails the test unless out contains every want.
func has(t *testing.T, what, out string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("%s: %q missing from:\n%s", what, w, out)
		}
	}
}

// TOON shapes: the data note first, a counted tabular array, the cursor last; exit codes in headers and
// job rows; multi-line text kept on one row; empty results say so.
func TestShapes(t *testing.T) {
	s := testStore(t)
	j := testutil.Job(t, s, store.Job{Name: "multi", Describe: "echo a\necho b"})
	if err := s.Finish(j.ID, "failed", 0, "boom\nmore", true); err != nil {
		t.Fatal(err)
	}
	cs := client(t, s)
	for _, c := range []struct {
		tool string
		args map[string]any
		want []string
	}{
		{"tail_read", map[string]any{"job": "web", "grep": "ERROR"}, []string{dataNote + "\njob: j000001 (web) · exited 3 · 3 lines total · last output ",
			"\nlines[1]{id,time,text}:\n  2,", ",ERROR db timeout\ncursor: 2\n"}},
		{"tail_read", map[string]any{"job": "web", "after": 3}, []string{"lines: 0 new lines after cursor 3\ncursor: 3\n"}},
		{"tail_search", map[string]any{"query": "timeout", "since": "1h"}, []string{dataNote, "count: 1 match for 'timeout'", "lines[1]{id,job,time,text}:\n  2,j000001,"}},
		{"tail_search", map[string]any{"query": "zzz"}, []string{"lines: 0 matches for 'zzz' in ", "help[2]:"}},
		{"tail_sql", map[string]any{"query": "select count(*) n, 'a,b' s from lines"}, []string{dataNote + "\nrows[1]{n,s}:\n  3,\"a,b\"\ncount: 1 rows\n"}},
		{"tail_sql", map[string]any{"query": "select a.id, b.id from jobs a, jobs b where a.id=b.id"}, []string{"rows[2]{id,id_2}:"}},
		{"tail_sql", map[string]any{"query": "select id from jobs where 0"}, []string{"rows: 0 rows\ncolumns[1]: id\n"}},
		{"tail_jobs", map[string]any{}, []string{"count: 2 jobs for the shared namespace", "jobs[2]{id,name,status,lines,last_output,source}:\n",
			`,multi,"failed: boom ⏎ more",0,never,echo a ⏎ echo b`, "  j000001,web,exited 3,3,"}},
		{"tail_jobs", map[string]any{"all": true}, []string{"{id,name,status,lines,last_output,source,owner}"}},
	} {
		out := call(t, cs, c.tool, c.args)
		has(t, fmt.Sprint(c.tool, c.args), out, c.want...)
		if strings.Contains(out, "help") && c.args["after"] != nil { // an ended job gets no tail_wait suggestion
			t.Errorf("read empty on an ended job suggests waiting:\n%s", out)
		}
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
	if len(out) > maxOut || !strings.HasPrefix(out, dataNote+"\nrows[") || !strings.Contains(out, " of 300 rows shown (output budget); narrow the query") {
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

var cliMention = regexp.MustCompile("`?tail-mode (run|start|stop|jobs|read|search|sql|follow|wait|notify|mcp)\\b")

// The schemas: identity never comes from params (no thread), MCP takes only host (no transport/target),
// every tool takes owner, unknown params are refused; descriptions carry the guidance and name only
// tools. `tail-mode tool --list` (List) is exactly what MCP advertises.
func TestSchemas(t *testing.T) {
	t.Setenv("TAIL_MODE_SHARED", "1") // for List
	cs := connect(t, testStore(t), true)
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
		var schema struct{ Properties map[string]any }
		b, _ := json.Marshal(tool.InputSchema)
		_ = json.Unmarshal(b, &schema)
		_, owner := schema.Properties["owner"]
		_, host := schema.Properties["host"]
		if !owner || host != (tool.Name == "tail_start") || schema.Properties["thread"] != nil || schema.Properties["transport"] != nil || schema.Properties["target"] != nil {
			t.Errorf("%s params: %v", tool.Name, slices.Sorted(maps.Keys(schema.Properties)))
		}
		d, _ := json.Marshal(tool)
		if m := cliMention.FindString(string(d)); m != "" || strings.Contains(string(d), "T3CODE_THREAD_ID") {
			t.Errorf("%s mentions the CLI or the thread env (%q): %s", tool.Name, m, tool.Description)
		}
		switch tool.Name {
		case "tail_start":
			has(t, "tail_start", string(d), "Required: exactly one of cmd, file", "dir is required, an absolute path", "exported in the job's shell on the target host")
		case "tail_wait":
			has(t, "tail_wait", tool.Description, "Required: job, until", "prefer tail_notify")
		}
	}
	if want := []string{"tail_jobs", "tail_notify", "tail_read", "tail_search", "tail_sql", "tail_start", "tail_stop", "tail_wait"}; !slices.Equal(names, want) {
		t.Errorf("tools %v, want %v", names, want)
	}
	canon := func(v any) string { // through any, so key order does not matter
		var x any
		b, _ := json.Marshal(v)
		_ = json.Unmarshal(b, &x)
		b, _ = json.Marshal(x)
		return string(b)
	}
	if got, want := canon(List()), canon(res.Tools); got != want {
		t.Errorf("List:\n%s\nMCP:\n%s", got, want)
	}
	has(t, "unknown param", callErr(t, cs, "tail_jobs", map[string]any{"thread": "t9"}), "thread")
}

// Bad input is refused with "error: … / help: …" before anything starts.
func TestBadInput(t *testing.T) {
	last, notes := fakes(t)
	cs := client(t, testStore(t))
	for _, c := range []struct {
		tool string
		args map[string]any
		want string
	}{
		{"tail_start", map[string]any{}, "exactly one"},
		{"tail_start", map[string]any{"file": "/a", "cmd": "b"}, "exactly one"},
		{"tail_start", map[string]any{"file": "/a", "source": "file"}, "exactly one"},
		{"tail_start", map[string]any{"cmd": "b", "config": map[string]any{}}, "config needs source"},
		{"tail_start", map[string]any{"cmd": "b", "backlog": 0}, "backlog applies to file"},
		{"tail_start", map[string]any{"source": "cmd", "config": "{bad"}, "config is not valid JSON"},
		{"tail_start", map[string]any{"cmd": "x", "dir": "/x", "notify_match": "("}, "missing closing )"},
		{"tail_start", map[string]any{"cmd": "x", "dir": "/x", "notify_every": "1s"}, "5s"},
		{"tail_wait", map[string]any{"job": "web", "until": "x", "timeout": "10m"}, "over the 5m0s maximum"},
		{"tail_wait", map[string]any{"job": "web", "until": "x", "timeout": "soon"}, "is not a positive duration"},
		{"tail_wait", map[string]any{"job": "web", "until": "("}, "non-empty regex"},
		{"tail_read", map[string]any{"job": "web", "grep": "("}, "grep"},
		{"tail_search", map[string]any{"query": "x", "since": "soon"}, "since"},
	} {
		text := callErr(t, cs, c.tool, c.args)
		if !strings.HasPrefix(text, "error: ") || !strings.Contains(text, c.want) || !strings.Contains(text, "\nhelp: ") {
			t.Errorf("%s %v:\n%s", c.tool, c.args, text)
		}
	}
	if last().Spec.Kind != "" || len(notes()) != 0 {
		t.Errorf("bad input started %+v, notifiers %+v", last(), notes())
	}
}

// Identity comes from the server's env only: the env thread is the owner and the destination, owner is a
// fallback label, neither means the shared namespace. Without a thread nothing can be pushed, so
// notifications return the wake recipe instead.
func TestIdentityFromEnv(t *testing.T) {
	last, notes := fakes(t)
	t.Setenv("T3CODE_THREAD_ID", "thread-1")
	cs := client(t, testStore(t))
	out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "owner": "ignored"})
	if cwd, _ := os.Getwd(); last().Owner != "thread-1" || last().Cwd != cwd {
		t.Errorf("start %+v", last())
	}
	has(t, "start in T3", out, "owner: this T3 thread\n", "notifications: off; tail_notify sends digests → this T3 thread thread-1 (detected from your session")
	has(t, "jobs in T3", call(t, cs, "tail_jobs", map[string]any{}), "count: 1 job for this thread")

	t.Setenv("T3CODE_THREAD_ID", "")
	cs = client(t, testStore(t))
	out = call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs", "owner": "me", "notify_match": "ERR x"})
	has(t, "start with owner", out, "owner: me\n", "no T3 thread detected", " --every 30s --match 'ERR x' --after 0", "tail-mode wait j")
	if last().Owner != "me" || strings.Contains(out, "warning") {
		t.Errorf("owner %q:\n%s", last().Owner, out)
	}
	has(t, "notify without thread", call(t, cs, "tail_notify", map[string]any{"job": "web"}),
		"no T3 thread detected so nothing was started", "tail_wait job=j000001", "tail-mode follow j000001 --every 30s\n")
	has(t, "shared namespace", call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs"}),
		"owner: none (shared namespace)", "warning: no T3 thread detected and no owner given")
	if last().Owner != "" || len(notes()) != 0 {
		t.Errorf("owner %q, notifiers %+v", last().Owner, notes())
	}
}

// With push (a T3 thread), notifying starts one notifier for the thread (tail_start's from the job's
// first line, tail_notify's from now unless after) and tells the agent to end its turn, never to wait.
func TestNotifyOn(t *testing.T) {
	_, notes := fakes(t)
	t.Setenv("T3CODE_THREAD_ID", "thread-1")
	cs := client(t, testStore(t))
	for i, c := range []struct {
		tool  string
		args  map[string]any
		after int64
		every time.Duration
		match string
	}{
		{"tail_start", map[string]any{"file": "/x", "notify_every": "10s", "notify_match": "ERR"}, 0, 10 * time.Second, "ERR"},
		{"tail_start", map[string]any{"cmd": "make", "dir": "/abs", "notify_every": "30s"}, 0, 30 * time.Second, ""},
		{"tail_notify", map[string]any{"job": "j000001"}, -1, 30 * time.Second, ""},
		{"tail_notify", map[string]any{"job": "j000001", "after": 2, "match": "E"}, 2, 30 * time.Second, "E"},
	} {
		out := call(t, cs, c.tool, c.args)
		has(t, fmt.Sprint(c.tool, c.args), out, "notifications: on → this T3 thread thread-1", "End your turn now")
		if strings.Contains(out, "tail_wait") {
			t.Errorf("%s %v suggests waiting:\n%s", c.tool, c.args, out)
		}
		if ns := notes(); len(ns) != i+1 || ns[i].Deliver != "t3:thread-1" || ns[i].After != c.after || ns[i].Every != c.every || ns[i].Match != c.match {
			t.Errorf("%s %v: notifier %+v", c.tool, c.args, ns)
		}
	}
	if out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs"}); strings.Contains(out, "End your turn") || !strings.Contains(out, "tail_wait") {
		t.Errorf("notify off:\n%s", out)
	}
}

// A client that delivers digests itself (Pi) gets the follower to run, not a T3 notifier or the recipe.
func TestPiNotify(t *testing.T) {
	_, notes := fakes(t)
	h := &handler{s: testStore(t), c: Pi}
	r, err := h.exec(context.Background(), "tail_notify", []byte(`{"job":"web","match":"ERR"}`))
	want := []string{"follow", "--deliver=jsonl", "--after=-1", "--every=30s", "--max-lines=10", "--match=ERR", "--", "j000001"}
	if err != nil || r.Follow == nil || r.Follow.Job != "j000001" || !slices.Equal(r.Follow.Argv, want) || len(notes()) != 0 {
		t.Fatalf("%v %+v\n%s", err, r.Follow, r.Text)
	}
	has(t, "pi notify", r.Text, "notifications: on → this Pi session", "End your turn")
}

// tail_stop: an ended job is a no-op; a starting job is stopped; another owner's job needs force (the
// error names the MCP param); an unknown job says how to find yours.
func TestStop(t *testing.T) {
	s := testStore(t)
	testutil.Job(t, s, store.Job{ID: "j000003"})
	testutil.Job(t, s, store.Job{ID: "j0000ab", Owner: "a"})
	cs := client(t, s)
	has(t, "ended", call(t, cs, "tail_stop", map[string]any{"job": "web"}), "job: j000001 (web) already exited 3 (no-op)")
	has(t, "starting", call(t, cs, "tail_stop", map[string]any{"job": "j000003"}), "job: j000003 stopped")
	has(t, "again", call(t, cs, "tail_stop", map[string]any{"job": "j000003"}), "already stopped (no-op)")
	text := callErr(t, cs, "tail_stop", map[string]any{"job": "j0000ab", "owner": "b"})
	has(t, "cross-owner", text, "belongs to owner 'a'", "force: true")
	if strings.Contains(text, "--force") {
		t.Errorf("names the CLI flag: %s", text)
	}
	has(t, "forced", call(t, cs, "tail_stop", map[string]any{"job": "j0000ab", "owner": "b", "force": true}), "job: j0000ab stopped")
	has(t, "unknown", callErr(t, cs, "tail_stop", map[string]any{"job": "nope"}), `error: no job "nope"`, "\nhelp: tail_jobs")
}

// tail_wait: a match, a timeout on a live job, a job that ended without a match; after skips earlier lines.
func TestWait(t *testing.T) {
	s := testStore(t)
	testutil.Job(t, s, store.Job{ID: "j000004", Name: "srv"}, "booting", "listening on :3000") // starting: live in its grace period
	cs := client(t, s)
	has(t, "match", call(t, cs, "tail_wait", map[string]any{"job": "srv", "until": "listening"}),
		dataNote+"\nwait: matched /listening/ in job j000004 (srv)\nlines[1]{id,time,text}:\n  5,", "cursor: 5\n")
	has(t, "timeout", call(t, cs, "tail_wait", map[string]any{"job": "srv", "until": "ready", "timeout": "100ms"}),
		"wait: timed out after 100ms; no line matched /ready/", "last_lines[2]{id,time,text}:", "cursor: 5\n",
		`tail_wait job=j000004 until="ready" after=5 keeps waiting`, "tail_notify job=j000004")
	has(t, "ended", call(t, cs, "tail_wait", map[string]any{"job": "web", "until": "never"}),
		"wait: job ended without a line matching /never/\njob: j000001 (web) · exited 3 · 3 lines total", "last_lines[3]", "cursor: 3\n")
	has(t, "after", call(t, cs, "tail_wait", map[string]any{"job": "web", "until": "ERROR", "after": 2}), "job ended without a line matching")
}

// host alone runs the job over ssh; an empty remote dir is the remote home.
func TestStartHost(t *testing.T) {
	last, _ := fakes(t)
	cs := client(t, testStore(t))
	out := call(t, cs, "tail_start", map[string]any{"cmd": "./build.sh", "host": "vps", "dir": "/srv/app", "owner": "me"})
	if o := last(); o.Transport != "ssh" || o.Target != "vps" || !strings.Contains(out, "on: ssh vps\n  dir: /srv/app\n") {
		t.Fatalf("%+v\n%s", o, out)
	}
	has(t, "remote home", call(t, cs, "tail_start", map[string]any{"cmd": "make", "host": "vps"}), "dir: ~ (remote home)")
}

// Exec (`tail-mode tool`, the CLI) runs the same handlers as MCP: the same text. Only the Client
// differs: the CLI's owner param wins over the env thread, shared mode does not apply to its cwd, and its
// waits have no cap and report the exit code.
func TestExec(t *testing.T) {
	fakes(t)
	cs := client(t, testStore(t))
	norm := regexp.MustCompile(`j[0-9a-f]{6}|\d+s ago`)
	h := &handler{s: testStore(t), c: MCP} // its own store, so each side starts and stops its own job
	for _, c := range []struct {
		tool string
		args map[string]any
	}{
		{"tail_start", map[string]any{"cmd": "make", "dir": "/abs", "name": "b"}},
		{"tail_read", map[string]any{"job": "web"}},
		{"tail_stop", map[string]any{"job": "b"}},
		{"tail_read", map[string]any{"job": "nope"}},
	} {
		mcpText, isErr, err := callResult(cs, c.tool, c.args)
		args, _ := json.Marshal(c.args)
		r, xerr := h.exec(context.Background(), c.tool, args)
		text := r.Text
		if xerr != nil {
			text = xerr.Error()
		}
		if err != nil || isErr != (xerr != nil) || norm.ReplaceAllString(mcpText, "X") != norm.ReplaceAllString(text, "X") {
			t.Errorf("%s %v: mcp (error %v):\n%s\nexec (error %v):\n%s", c.tool, c.args, isErr, mcpText, xerr, text)
		}
	}

	t.Setenv("T3CODE_THREAD_ID", "thread-1")
	t.Setenv("TAIL_MODE_SHARED", "1")
	h = newHandler(testStore(t), CLI)
	r, err := h.exec(context.Background(), "tail_start", []byte(`{"cmd":"make","owner":"me"}`)) // relative dir: allowed
	if err != nil || r.Job == "" || !strings.Contains(r.Text, "job: "+r.Job+"\n") || !strings.Contains(r.Text, "owner: me\n") {
		t.Errorf("cli start %v %+v", err, r)
	}
	r, err = h.exec(context.Background(), "tail_wait", []byte(`{"job":"web","until":"x","timeout":"1h"}`))
	if err != nil || r.Code != follow.WaitEnded {
		t.Errorf("cli wait %v %+v", err, r)
	}
}
