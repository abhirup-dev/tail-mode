package mcpserver

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/abhirup-dev/tail-mode/internal/store"
)

var startedRe = regexp.MustCompile(`^started:\n  job: (j[0-9a-f]+)\n`)

// Many non-T3 agents over ONE MCP session, each identified by its owner label: interleaved start,
// read, search, jobs and stop with the same job name, concurrently. No errors, no cross-talk.
func TestSharedSessionConcurrentOwners(t *testing.T) {
	rowStubs(t)
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cs := sharedClient(t, s)

	owners := []string{"label-a", "label-b", "label-c", "label-d"}
	const rounds = 6
	var wg sync.WaitGroup
	errs := make(chan error, 1000)
	fail := func(format string, a ...any) { errs <- fmt.Errorf(format, a...) }
	for _, o := range owners {
		for r := range rounds {
			wg.Add(1)
			go func() {
				defer wg.Done()
				name := fmt.Sprintf("web%d", r) // every owner uses the same names
				do := func(tool string, args map[string]any) string {
					args["owner"] = o
					text, isErr, err := callResult(cs, tool, args)
					if err != nil || isErr {
						fail("%s %s %v: %v %s", o, tool, args, err, text)
					}
					return text
				}
				out := do("tail_start", map[string]any{"cmd": "npm run dev", "dir": "/srv/" + o, "name": name})
				m := startedRe.FindStringSubmatch(out)
				if m == nil || !strings.Contains(out, "owner: "+o+"\n") || !strings.Contains(out, "dir: /srv/"+o+"\n") || strings.Contains(out, "warning") {
					fail("%s start:\n%s", o, out)
					return
				}
				id := m[1]
				if out := do("tail_read", map[string]any{"job": name}); !strings.Contains(out, "job: "+id+" ("+name+")") || !strings.Contains(out, "marker "+o+" "+id) {
					fail("%s read %s:\n%s", o, name, out)
				}
				if out := do("tail_search", map[string]any{"query": "marker"}); strings.Count(out, "marker ") != strings.Count(out, "marker "+o+" ") {
					fail("%s search saw other owners:\n%s", o, out)
				}
				if out := do("tail_jobs", map[string]any{}); !strings.Contains(out, "  "+id+","+name+",") || strings.Contains(out, "owner") && !strings.Contains(out, "owner '"+o+"'") {
					fail("%s jobs:\n%s", o, out)
				}
				if out := do("tail_stop", map[string]any{"job": name}); out != "job: "+id+" ("+name+") stopped\n" {
					fail("%s stop %s: %q (want %s)", o, name, out, id)
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
	// Cross-owner stop by id is refused without force (on a live job), even within one session.
	j := store.Job{ID: "j0000aa", Owner: "label-a", Kind: "cmd", Transport: "local", Spec: `{}`, Status: "starting", StartedAt: store.NowMS()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if text := callErr(t, cs, "tail_stop", map[string]any{"job": j.ID, "owner": "label-z"}); !strings.Contains(text, "belongs to owner 'label-a'") || !strings.Contains(text, "help: ") {
		t.Fatalf("cross-owner stop: %s", text)
	}
	if out := call(t, cs, "tail_stop", map[string]any{"job": j.ID, "owner": "label-z", "force": true}); out != "job: j0000aa stopped\n" {
		t.Fatalf("forced stop: %q", out)
	}
}

// Shared mode: no fallback to the server's cwd; errors say exactly what to pass. The env thread is
// still the caller's (one server process per agent session).
func TestSharedModeValidation(t *testing.T) {
	notes := rowStubs(t)
	t.Setenv("T3CODE_THREAD_ID", "env-thread")
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
		{map[string]any{"cmd": "make"}, []string{"cmd has no dir", "absolute path", `dir="/`, "pwd"}},
		{map[string]any{"cmd": "make", "dir": "app"}, []string{`dir "app" is relative`, "absolute path"}},
		{map[string]any{"source": "cmd", "config": map[string]any{"cmd": "make"}}, []string{"cmd has no dir", `put "dir" in config`}},
		{map[string]any{"file": "logs/app.log"}, []string{`file path "logs/app.log" is relative`, "absolute path"}},
	} {
		text := callErr(t, cs, "tail_start", c.args)
		for _, w := range append(c.want, "error: ", "\nhelp: ") {
			if !strings.Contains(text, w) {
				t.Errorf("%v: %q missing from:\n%s", c.args, w, text)
			}
		}
	}
	if js, _ := s.ListJobs("", true); len(js) != 0 {
		t.Fatalf("a refused start left jobs: %+v", js)
	}

	// Accepted forms; the result echoes owner, dir and where.
	call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "~/app"})
	call(t, cs, "tail_start", map[string]any{"file": "~/x.log"})
	out := call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs", "notify_match": "ERR"})
	if !strings.Contains(out, "owner: this T3 thread\n") || !strings.Contains(out, "dir: /abs\n") || !strings.Contains(out, "notifications: on → this T3 thread env-thread") {
		t.Fatalf("start:\n%s", out)
	}
	if ns := notes(); len(ns) != 1 || ns[0].Deliver != "t3:env-thread" || ns[0].After != 0 {
		t.Fatalf("notifiers %+v", ns)
	}
	if out := call(t, cs, "tail_jobs", map[string]any{}); !strings.HasPrefix(out, "count: 3 jobs for this thread") {
		t.Fatalf("jobs:\n%s", out)
	}
}
