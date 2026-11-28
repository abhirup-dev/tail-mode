package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/testutil"
)

// TestMain doubles as the detached children (proc.Detach re-execs this test binary): `_job <id>` job
// workers and `follow …` notifiers. The env identity is cleared so nothing here can reach a real
// T3 thread, and so --owner defaults do not depend on where the tests run.
func TestMain(m *testing.M) {
	os.Unsetenv("T3CODE_THREAD_ID")
	os.Unsetenv("TAIL_MODE_OWNER")
	if len(os.Args) > 1 && (os.Args[1] == "_job" || os.Args[1] == "follow") {
		if err := Run(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "test child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// Each job command is one tool call: its flags (interleaved with positionals, hyphenated values allowed)
// map onto exactly these tool args. Defaults are the tool's, except the CLI's own (wait: 30m, from now).
func TestFlagsToToolArgs(t *testing.T) {
	t.Setenv("T3CODE_THREAD_ID", " T1\n") // the --owner default, trimmed
	for _, c := range []struct {
		args       []string
		tool, json string
	}{
		{[]string{"start", "--file", "/x", "--backlog", "0", "--host", "vps", "--no-restart", "--name", "w"}, "tail_start",
			`{"owner":"T1","file":"/x","host":"vps","name":"w","backlog":0,"restart":false}`},
		{[]string{"start", "--source", "cmd", "--config", `{"cmd":"ls"}`, "--owner="}, "tail_start", `{"source":"cmd","config":"{\"cmd\":\"ls\"}"}`},
		{[]string{"run", "--name", "web", "--dir", "/a", "--env", "A=b=c", "--env", "B=1;2", "--shell", "zsh", "--login",
			"--every", "10s", "--match", "ERR", "--", "npm", "run", "a b"}, "tail_start",
			`{"owner":"T1","cmd":"npm run 'a b'","dir":"/a","env":{"A":"b=c","B":"1;2"},"shell":"zsh","login":true,"name":"web","notify_every":"10s","notify_match":"ERR"}`},
		{[]string{"run", "--host", "vps", "--", "make 2>&1 | tee log"}, "tail_start", `{"owner":"T1","cmd":"make 2\u003e\u00261 | tee log","host":"vps"}`},
		{[]string{"stop", "web", "--force", "--owner", "x"}, "tail_stop", `{"owner":"x","job":"web","force":true}`},
		{[]string{"jobs", "--all"}, "tail_jobs", `{"owner":"T1","all":true}`},
		{[]string{"read", "--tail", "2", "web", "--grep", "-x", "--after=-1"}, "tail_read", `{"owner":"T1","job":"web","after":-1,"tail":2,"grep":"-x"}`},
		{[]string{"search", "--since", "1h", "--", "-foo", "bar"}, "tail_search", `{"owner":"T1","query":"-foo bar","since":"1h"}`},
		{[]string{"sql", "select", "1", "--limit", "5"}, "tail_sql", `{"query":"select 1","limit":5}`},
		{[]string{"wait", "web", "--until", "ready"}, "tail_wait", `{"owner":"T1","job":"web","until":"ready","timeout":"30m0s","after":-1}`},
		{[]string{"notify", "web"}, "tail_notify", `{"owner":"T1","job":"web"}`},
		{[]string{"notify", "web", "--every", "10s", "--match", "E", "--after", "0"}, "tail_notify", `{"owner":"T1","job":"web","every":"10s","match":"E","after":0}`},
	} {
		ctx, err := parse(&root{}, c.args)
		if err != nil {
			t.Errorf("%q: %v", c.args, err)
			continue
		}
		tool, in := ctx.Selected().Target.Addr().Interface().(toolCommand).tool()
		if b, _ := json.Marshal(in); tool != c.tool || string(b) != c.json {
			t.Errorf("%q:\n got %s %s\nwant %s %s", c.args, tool, b, c.tool, c.json)
		}
	}
	// run --until then waits on the new job from its first line.
	var r root
	if _, err := parse(&r, []string{"run", "--until", "ready", "--timeout", "1m", "--", "x"}); err != nil {
		t.Fatal(err)
	}
	if b, _ := json.Marshal(r.RunCmd.wait("j1")); string(b) != `{"owner":"T1","job":"j1","until":"ready","timeout":"1m0s","after":0}` {
		t.Errorf("run --until: %s", b)
	}
	// follow (no tool) parses the argv notifiers and Pi followers are started with.
	o := follow.Options{Deliver: "jsonl", After: -1, Every: 30 * time.Second, MaxLines: 10, Match: "-x"}
	if _, err := parse(&r, o.FollowArgs("web")); err != nil || r.Follow != (followCmd{Job: "web", Every: o.Every, Match: "-x", MaxLines: 10, After: -1,
		Deliver: "jsonl", ownerOpt: ownerOpt{"T1"}}) {
		t.Errorf("follow %v: %+v", err, r.Follow)
	}
	for _, bad := range [][]string{{"start"}, {"start", "--file", "/x", "--cmd", "ls"}, {"start", "--file", "/x", "--transport", "ssh"},
		{"wait", "web"}, {"search", "-foo"}, {"jobs", "--json"}} {
		if _, err := parse(&root{}, bad); err == nil {
			t.Errorf("%q: want a parse error", bad)
		}
	}
}

// Exit status: 0 ok; wait's follow.WaitTimeout and follow.WaitEnded; 1 for a tool error (stderr, or stdout
// for the hidden tool command that Pi reads). run with a bad --until or --timeout, or no command, starts nothing.
func TestExitCodes(t *testing.T) {
	s := testutil.Store(t)
	testutil.LiveJob(t, s, store.Job{Name: "live"}, "booting", "ready")
	testutil.Job(t, s, store.Job{Name: "ended", Status: "exited"}, "bye")
	var out bytes.Buffer
	testutil.Swap[io.Writer](t, &stdout, &out)
	for _, c := range []struct {
		args []string
		code int
		out  string
	}{
		{[]string{"wait", "live", "--until", "ready", "--after", "0"}, 0, "wait: matched /ready/"},
		{[]string{"wait", "live", "--until", "nope", "--timeout", "50ms"}, follow.WaitTimeout, "wait: timed out after 50ms"},
		{[]string{"wait", "ended", "--until", "nope"}, follow.WaitEnded, "wait: job ended without a line matching /nope/"},
		{[]string{"read", "nosuch"}, 1, ""},
		{[]string{"run", "--until", "(", "--", "true"}, -1, ""},
		{[]string{"run", "--until", "x", "--timeout", "0s", "--", "true"}, -1, ""},
		{[]string{"run", "--", " "}, -1, ""},
		{[]string{"tool", "tail_read", `{"job":"nosuch"}`}, 1, `error: no job "nosuch"`},
		{[]string{"tool", "tail_jobs", "--client", "pi"}, 0, `{"text":"count: 2 jobs`},
	} {
		out.Reset()
		err := Run(c.args)
		code := 0
		if ee := (ExitError{}); errors.As(err, &ee) {
			code = ee.Code
		} else if err != nil {
			code = -1
		}
		if code != c.code || !strings.Contains(out.String(), c.out) {
			t.Errorf("%q: exit %d (%v), want %d; output:\n%s", c.args, code, err, c.code, out.String())
		}
	}
	if js, _ := s.ListJobs("", true); len(js) != 2 {
		t.Errorf("a refused run started a job: %+v", js)
	}
}

// Pi runs `follow --deliver jsonl` and relies on the last line being the final digest.
func TestFollowJSONLEndsFinal(t *testing.T) {
	s := testutil.Store(t)
	j := testutil.Job(t, s, store.Job{Status: "exited"}, "done")
	out, err := exec.Command(os.Args[0], "follow", j.ID, "--deliver", "jsonl").Output()
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var d struct {
		Final    bool `json:"final"`
		ExitCode *int `json:"exit_code"`
	}
	if err != nil || json.Unmarshal([]byte(lines[len(lines)-1]), &d) != nil || !d.Final || d.ExitCode == nil || *d.ExitCode != 0 {
		t.Fatalf("%v %+v:\n%s", err, d, out)
	}
}
