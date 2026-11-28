package tools

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/abhirup-dev/tail-mode/internal/testutil"
)

var startedRe = regexp.MustCompile(`^started:\n  job: (j[0-9a-f]+)\n`)

// Many non-T3 agents over ONE MCP session, each identified by its owner label: interleaved start,
// read, search, jobs and stop with the same job name, concurrently. No errors, no cross-talk.
func TestSharedSessionConcurrentOwners(t *testing.T) {
	fakes(t)
	s := testutil.Store(t)
	cs := connect(t, s, true)

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
				do := func(tool string, args map[string]any, want ...string) string {
					args["owner"] = o
					text, isErr, err := callResult(cs, tool, args)
					for _, w := range want {
						if err != nil || isErr || !strings.Contains(text, w) {
							fail("%s %s %v: %v; %q missing from:\n%s", o, tool, args, err, w, text)
						}
					}
					return text
				}
				text := do("tail_start", map[string]any{"cmd": "npm run dev", "dir": "/srv/" + o, "name": name})
				m := startedRe.FindStringSubmatch(text)
				if m == nil || !strings.Contains(text, "owner: "+o+"\n") || !strings.Contains(text, "dir: /srv/"+o+"\n") || strings.Contains(text, "warning") {
					fail("%s start:\n%s", o, text)
					return
				}
				id := m[1]
				do("tail_read", map[string]any{"job": name}, "job: "+id+" ("+name+")", "marker "+o+" "+id)
				if out := do("tail_search", map[string]any{"query": "marker"}); strings.Count(out, "marker ") != strings.Count(out, "marker "+o+" ") {
					fail("%s search saw other owners:\n%s", o, out)
				}
				do("tail_jobs", map[string]any{}, "  "+id+","+name+",", "owner '"+o+"'")
				do("tail_stop", map[string]any{"job": name}, "job: "+id+" ("+name+") stopped")
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every owner ended up with its own rounds jobs, all stopped.
	all, err := s.ListJobs("", true)
	if err != nil || len(all) != len(owners)*rounds {
		t.Fatalf("%d jobs, %v", len(all), err)
	}
	for _, j := range all {
		if j.Status != "stopped" || !strings.Contains(j.Spec, `/srv/`+j.Owner+`"`) {
			t.Errorf("job %+v", j)
		}
	}
}

// Shared mode: no fallback to the server's cwd; errors say exactly what to pass. The env thread is
// still the caller's (one server process per agent session).
func TestSharedModeValidation(t *testing.T) {
	fakes(t)
	t.Setenv("T3CODE_THREAD_ID", "env-thread")
	s := testutil.Store(t)
	cs := connect(t, s, true)
	for _, c := range []struct {
		args map[string]any
		want []string
	}{
		{map[string]any{"cmd": "make"}, []string{"cmd has no dir", "absolute path", `dir="/`, "pwd"}},
		{map[string]any{"cmd": "make", "dir": "app"}, []string{`dir "app" is relative`, "absolute path"}},
		{map[string]any{"source": "cmd", "config": map[string]any{"cmd": "make"}}, []string{"cmd has no dir", `put "dir" in config`}},
		{map[string]any{"file": "logs/app.log"}, []string{`file path "logs/app.log" is relative`, "absolute path"}},
	} {
		has(t, fmt.Sprint(c.args), callErr(t, cs, "tail_start", c.args), append(c.want, "error: ", "\nhelp: ")...)
	}
	if js, _ := s.ListJobs("", true); len(js) != 0 {
		t.Fatalf("a refused start left jobs: %+v", js)
	}
	// Accepted forms; the result echoes owner and dir.
	call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "~/app"})
	call(t, cs, "tail_start", map[string]any{"file": "~/x.log"})
	has(t, "start", call(t, cs, "tail_start", map[string]any{"cmd": "make", "dir": "/abs"}), "owner: this T3 thread\n", "dir: /abs\n")
	has(t, "jobs", call(t, cs, "tail_jobs", map[string]any{}), "count: 3 jobs for this thread")
}
