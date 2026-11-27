package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestMain clears T3CODE_THREAD_ID so no test can reach a real T3 thread, and so owner defaults do
// not depend on where the tests run.
func TestMain(m *testing.M) {
	os.Unsetenv("T3CODE_THREAD_ID")
	os.Exit(m.Run())
}

// sharedClient connects an MCP client to a shared-mode server over in-memory transports.
func sharedClient(t *testing.T, s *store.Store) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	ct, st := mcp.NewInMemoryTransports()
	ss, err := newServer(s, true).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "mcporter", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close(); ss.Wait() })
	return cs
}

// callResult returns a tool's text and whether it is a tool error; transport errors are returned.
func callResult(cs *mcp.ClientSession, tool string, args map[string]any) (string, bool, error) {
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return "", false, err
	}
	return res.Content[0].(*mcp.TextContent).Text, res.IsError, nil
}

func callErr(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) string {
	t.Helper()
	text, isErr, err := callResult(cs, tool, args)
	if err != nil {
		t.Fatal(err)
	}
	if !isErr {
		t.Fatalf("%s %v: want a tool error, got:\n%s", tool, args, text)
	}
	return text
}

// rowStubs makes startJob insert a real "starting" row (no process; Stop aborts it) with one line,
// and records notifier starts. Both are safe for concurrent use.
func rowStubs(t *testing.T) (notes func() []follow.NotifierOpts) {
	var mu sync.Mutex
	var ns []follow.NotifierOpts
	oldJob, oldNote := startJob, startNotifier
	t.Cleanup(func() { startJob, startNotifier = oldJob, oldNote })
	startJob = func(s *store.Store, o jobs.StartOpts) (store.Job, error) {
		j := store.Job{ID: store.NewJobID(), Name: o.Name, Owner: o.Owner, Kind: o.Spec.Kind, Transport: "local",
			Spec: string(o.Spec.Config), Describe: "stub", Status: "starting", StartedAt: store.NowMS()}
		if err := s.CreateJob(j); err != nil {
			return j, err
		}
		if err := s.Append(j.ID, []string{"marker " + o.Owner + " " + j.ID}, store.NowMS()); err != nil {
			return j, err
		}
		return s.GetJob(j.ID, "")
	}
	startNotifier = func(_ *store.Store, _ string, o follow.NotifierOpts) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		ns = append(ns, o)
		return 4242, nil
	}
	return func() []follow.NotifierOpts {
		mu.Lock()
		defer mu.Unlock()
		return append([]follow.NotifierOpts(nil), ns...)
	}
}

var startedRe = regexp.MustCompile(`^started (j[0-9a-f]+): `)

// Many agents over ONE MCP session (as an MCP daemon multiplexes them): interleaved start, read,
// search, jobs and stop for 4 owners with the same job name, concurrently. No errors, no cross-talk.
func TestSharedSessionConcurrentOwners(t *testing.T) {
	rowStubs(t)
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cs := sharedClient(t, s)

	owners := []string{"thread-a", "thread-b", "thread-c", "label-d"}
	const rounds = 6
	var wg sync.WaitGroup
	errs := make(chan error, 1000)
	fail := func(format string, a ...any) { errs <- fmt.Errorf(format, a...) }
	for _, o := range owners {
		who := map[string]any{"thread": o}
		if strings.HasPrefix(o, "label-") { // a non-T3 client identifies with owner
			who = map[string]any{"owner": o}
		}
		with := func(m map[string]any) map[string]any {
			for k, v := range who {
				m[k] = v
			}
			return m
		}
		for r := range rounds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				name := fmt.Sprintf("web%d", r) // every owner uses the same names
				do := func(tool string, args map[string]any) string {
					text, isErr, err := callResult(cs, tool, with(args))
					if err != nil || isErr {
						fail("%s %s %v: %v %s", o, tool, args, err, text)
					}
					return text
				}
				out := do("tail_start", map[string]any{"cmd": "npm run dev", "dir": "/srv/" + o, "name": name})
				m := startedRe.FindStringSubmatch(out)
				if m == nil || !strings.Contains(out, "owner "+o) || !strings.Contains(out, "dir /srv/"+o) || strings.Contains(out, "warning") {
					fail("%s start:\n%s", o, out)
					return
				}
				id := m[1]
				if out := do("tail_read", map[string]any{"job": name}); !strings.Contains(out, "[tail-mode] "+id+" ") || !strings.Contains(out, "marker "+o+" "+id) {
					fail("%s read %s:\n%s", o, name, out)
				}
				if out := do("tail_search", map[string]any{"query": "marker"}); strings.Count(out, "marker ") != strings.Count(out, "marker "+o+" ") {
					fail("%s search saw other owners:\n%s", o, out)
				}
				if out := do("tail_jobs", map[string]any{}); !strings.Contains(out, id+" "+name+" ") || strings.Count(out, "\n") > rounds+1 {
					fail("%s jobs:\n%s", o, out)
				}
				if out := do("tail_stop", map[string]any{"job": name}); out != "stopped "+id+" (stopped)" {
					fail("%s stop %s: %s (want %s)", o, name, out, id)
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every owner ended up with its own rounds jobs, all stopped; nothing leaked across owners.
	all, err := s.ListJobs("", true)
	if err != nil || len(all) != len(owners)*rounds {
		t.Fatalf("%d jobs, %v", len(all), err)
	}
	for _, j := range all {
		if j.Status != "stopped" || !strings.Contains(j.Spec, `/srv/`+j.Owner+`"`) {
			t.Errorf("job %+v", j)
		}
	}
	// Cross-owner stop by id is refused without force, even within one session.
	id := all[0].ID
	other := "thread-z"
	if text := callErr(t, cs, "tail_stop", map[string]any{"job": id, "thread": other}); !strings.Contains(text, fmt.Sprintf("%q", all[0].Owner)) {
		t.Fatalf("cross-owner stop: %s", text)
	}
	if text, isErr, err := callResult(cs, "tail_stop", map[string]any{"job": id, "thread": other, "force": true}); err != nil || isErr {
		t.Fatalf("forced stop: %v %s", err, text)
	}
}

// Shared mode: no fallback to the server's cwd or env; errors say exactly what to pass.
func TestSharedModeValidation(t *testing.T) {
	notes := rowStubs(t)
	t.Setenv("T3CODE_THREAD_ID", "daemon-env-thread") // the daemon's env: a shared server must ignore it
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cs := sharedClient(t, s)

	for _, c := range []struct {
		args map[string]any
		want []string
	}{
		{map[string]any{"cmd": "make", "thread": "t1"}, []string{"cmd has no dir", "absolute path", `dir: "/`, "pwd"}},
		{map[string]any{"cmd": "make", "dir": "app", "thread": "t1"}, []string{`dir "app" is relative`, "absolute path"}},
		{map[string]any{"source": "cmd", "config": map[string]any{"cmd": "make"}, "thread": "t1"}, []string{"cmd has no dir", "put \"dir\" in config"}},
		{map[string]any{"file": "logs/app.log", "thread": "t1"}, []string{`file path "logs/app.log" is relative`, "absolute path"}},
		{map[string]any{"cmd": "make", "dir": "/abs", "notify_every": "30s"}, []string{"nothing started", "notify needs thread", "echo $T3CODE_THREAD_ID"}},
	} {
		text := callErr(t, cs, "tail_start", c.args)
		for _, w := range c.want {
			if !strings.Contains(text, w) {
				t.Errorf("%v: %q missing from:\n%s", c.args, w, text)
			}
		}
	}
	if js, _ := s.ListJobs("", true); len(js) != 0 {
		t.Fatalf("a refused start left jobs: %+v", js)
	}

	// Accepted forms; the result echoes owner, dir and where.
	call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "~/app", "thread": "t1"})
	call(t, cs, "tail_start", map[string]any{"file": "~/x.log", "thread": "t1"})
	if out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "host": "vps", "thread": "t1"}); !strings.Contains(out, "on ssh vps") || !strings.Contains(out, "dir ~ (remote home)") {
		t.Fatalf("remote:\n%s", out)
	}
	out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs", "name": "w"})
	if !strings.Contains(out, "owner '' (shared)") || !strings.Contains(out, "dir /abs") || !strings.Contains(out, "other agents can see and stop it") ||
		!strings.Contains(out, "echo $T3CODE_THREAD_ID") || strings.Contains(out, "daemon-env-thread") {
		t.Fatalf("shared namespace start:\n%s", out)
	}
	if out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs", "thread": "t1", "owner": "ignored"}); !strings.Contains(out, "owner t1 ") || strings.Contains(out, "warning") {
		t.Fatalf("thread wins over owner:\n%s", out)
	}

	// Forgetting thread on a later call: the not-found error says to pass it.
	call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs", "name": "mine", "thread": "t9"})
	for _, tool := range []string{"tail_read", "tail_stop", "tail_notify"} {
		if text := callErr(t, cs, tool, map[string]any{"job": "mine"}); !strings.Contains(text, "pass the same thread") || !strings.Contains(text, "echo $T3CODE_THREAD_ID") {
			t.Fatalf("%s without thread: %s", tool, text)
		}
	}
	if out := call(t, cs, "tail_read", map[string]any{"job": "mine", "thread": "t9"}); !strings.Contains(out, "marker t9") {
		t.Fatalf("read with thread:\n%s", out)
	}

	// Notify: never the daemon's env; with thread it goes to that thread.
	if text := callErr(t, cs, "tail_notify", map[string]any{"job": "w"}); !strings.Contains(text, "notify needs thread") || !strings.Contains(text, "tail-mode follow j") {
		t.Fatalf("notify without thread: %s", text)
	}
	out = call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs", "thread": "t2", "notify_match": "ERR"})
	if !strings.Contains(out, "notifier pid 4242") || !strings.Contains(out, "thread t2") {
		t.Fatalf("notify with thread:\n%s", out)
	}
	if ns := notes(); len(ns) != 1 || ns[0].Deliver != "t3:t2" || ns[0].After != 0 {
		t.Fatalf("notifiers %+v", ns)
	}
	if out := call(t, cs, "tail_jobs", map[string]any{"thread": "t1"}); !strings.Contains(out, `4 jobs of owner "t1"`) {
		t.Fatalf("t1 jobs:\n%s", out)
	}
	if out := call(t, cs, "tail_jobs", map[string]any{"all": true}); !strings.Contains(out, "7 jobs (all owners)") || !strings.Contains(out, `owner ""`) {
		t.Fatalf("all jobs:\n%s", out)
	}
}

// Every tool accepts thread and owner, so an agent told to always pass thread never hits the strict schema.
func TestEveryToolTakesCaller(t *testing.T) {
	cs := sharedClient(t, testStore(t))
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		b, _ := json.Marshal(tool.InputSchema)
		if !strings.Contains(string(b), `"thread"`) || !strings.Contains(string(b), `"owner"`) {
			t.Errorf("%s schema lacks thread/owner: %s", tool.Name, b)
		}
	}
	if out := call(t, cs, "tail_sql", map[string]any{"query": "select owner from jobs", "thread": "t1"}); !strings.Contains(out, "(1 rows)") {
		t.Fatalf("sql with thread:\n%s", out)
	}
}
