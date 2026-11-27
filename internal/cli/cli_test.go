package cli

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/abhirup-dev/tail-mode/internal/store"
)

func mustParse(t *testing.T, args ...string) *root {
	t.Helper()
	var r root
	if _, err := parse(&r, args); err != nil {
		t.Fatalf("%q: %v", args, err)
	}
	return &r
}

func parseErr(t *testing.T, args ...string) {
	t.Helper()
	if _, err := parse(&root{}, args); err == nil {
		t.Fatalf("%q: want parse error", args)
	}
}

func TestParseInterleavedAndDashDash(t *testing.T) {
	for _, args := range [][]string{
		{"read", "web", "--tail", "2", "--json"},
		{"read", "--tail", "2", "web", "--json"},
		{"read", "--json", "--tail=2", "web"},
	} {
		r := mustParse(t, args...)
		if r.Read.Job != "web" || r.Read.Tail != 2 || !r.Read.JSON || r.Read.Limit != 200 {
			t.Fatalf("%q: %+v", args, r.Read)
		}
	}
	r := mustParse(t, "search", "--limit", "5", "--", "-foo", "--json")
	if strings.Join(r.Search.Query, " ") != "-foo --json" || r.Search.JSON || r.Search.Limit != 5 {
		t.Fatalf("search: %+v", r.Search)
	}
	r = mustParse(t, "sql", "select 1", "--json")
	if r.SQL.Query[0] != "select 1" || !r.SQL.JSON || r.SQL.Limit != 500 || r.SQL.Timeout.String() != "5s" {
		t.Fatalf("sql: %+v", r.SQL)
	}
	for _, args := range [][]string{{"follow", "web", "--after", "-1"}, {"follow", "web", "--after=-1"}, {"follow", "web"}} {
		if r := mustParse(t, args...); r.Follow.After != -1 {
			t.Fatalf("%q: after %d", args, r.Follow.After)
		}
	}
	if r := mustParse(t, "read", "web", "--grep", "-x"); r.Read.Grep != "-x" {
		t.Fatalf("hyphen value: %+v", r.Read)
	}
	parseErr(t, "search", "-foo") // a hyphenated positional needs --
	if r := mustParse(t, "follow", "--after=0", "--", "web"); r.Follow.After != 0 || r.Follow.Job != "web" {
		t.Fatalf("notifier argv: %+v", r.Follow)
	}
	parseErr(t, "jobs", "--nope")
	parseErr(t, "wait", "web") // --until required
}

func TestStartOpts(t *testing.T) {
	opts := func(args ...string) (string, string, string, *bool, error) {
		t.Helper()
		r := mustParse(t, append([]string{"start"}, args...)...)
		o, err := r.Start.opts()
		return o.Spec.Kind + " " + string(o.Spec.Config), o.Transport, o.Target, o.Spec.Restart, err
	}
	if spec, tr, tg, rs, err := opts("--file", "/x", "--name", "w"); err != nil || spec != `file {"path":"/x"}` || tr != "" || tg != "" || rs != nil {
		t.Fatalf("file: %s %s %s %v %v", spec, tr, tg, rs, err)
	}
	if spec, _, _, _, _ := opts("--file", "/x", "--backlog", "0"); spec != `file {"path":"/x","backlog":0}` {
		t.Fatalf("backlog 0: %s", spec)
	}
	if spec, tr, tg, rs, _ := opts("--cmd", "make dev", "--host", "vps", "--no-restart"); spec != `cmd {"cmd":"make dev"}` || tr != "ssh" || tg != "vps" || rs == nil || *rs {
		t.Fatalf("cmd: %s %s %s %v", spec, tr, tg, rs)
	}
	if _, tr, tg, rs, _ := opts("--source", "cmd", "--config", `{"cmd":"ls"}`, "--transport", "local", "--target", "t", "--restart"); tr != "local" || tg != "t" || rs == nil || !*rs {
		t.Fatalf("source: %s %s %v", tr, tg, rs)
	}
	for _, bad := range [][]string{{"--cmd", "ls", "--backlog", "3"}, {"--file", "/x", "--config", "{}"}, {"--source", "cmd", "--config", "{nope"}} {
		if _, _, _, _, err := opts(bad...); err == nil {
			t.Fatalf("%q: want error", bad)
		}
	}
	parseErr(t, "start")                                // no source
	parseErr(t, "start", "--file", "/x", "--cmd", "ls") // two sources
	parseErr(t, "start", "--file", "/x", "--host", "h", "--transport", "ssh")
	parseErr(t, "start", "--file", "/x", "--host", "h", "--target", "h2")
}

func TestNotifyDeliverResolution(t *testing.T) {
	t.Setenv("TAIL_MODE_HOME", t.TempDir())
	t.Setenv("T3CODE_THREAD_ID", "")
	if err := Notify([]string{"web"}); err == nil || !strings.Contains(err.Error(), "nowhere to deliver") {
		t.Fatalf("want resolution error before touching the job, got %v", err)
	}
	spec := func(args ...string) string {
		t.Helper()
		r := mustParse(t, append([]string{"notify", "web"}, args...)...)
		s, err := r.Notify.deliverSpec()
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if s := spec("--thread", "T1"); s != "t3:T1" {
		t.Fatal(s)
	}
	if s := spec("--deliver", "jsonl"); s != "jsonl" {
		t.Fatal(s)
	}
	t.Setenv("T3CODE_THREAD_ID", "ENV")
	if s := spec(); s != "t3:ENV" {
		t.Fatal(s)
	}
	if s := spec("--thread", "T1"); s != "t3:T1" {
		t.Fatal(s)
	}
	parseErr(t, "notify", "web", "--thread", "T1", "--deliver", "jsonl")
}

func TestReadAndSQL(t *testing.T) {
	t.Setenv("TAIL_MODE_HOME", t.TempDir())
	s, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	j := store.Job{ID: store.NewJobID(), Name: "web", Kind: "cmd", Transport: "local", Spec: "{}", Status: "starting", StartedAt: store.NowMS()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(j.ID, []string{"a", "b", "c"}, store.NowMS()); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	stdout = &buf
	t.Cleanup(func() { stdout = os.Stdout })
	if err := Read([]string{"web", "--tail", "2"}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); strings.Count(got, "\n") != 2 || !strings.HasSuffix(got, " c\n") {
		t.Fatalf("read %q", got)
	}
	buf.Reset()
	if err := SQL([]string{"select count(*) n from lines", "--json"}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != `{"columns":["n"],"rows":[[3]]}`+"\n" {
		t.Fatalf("sql %q", got)
	}
	if err := SQL([]string{"delete from lines"}); err == nil {
		t.Fatal("sql must be read-only")
	}
}

// --owner defaults to $T3CODE_THREAD_ID (parse only: nothing runs), and --owner= selects the shared namespace.
func TestOwnerFlag(t *testing.T) {
	t.Setenv("T3CODE_THREAD_ID", "t-env")
	if r := mustParse(t, "read", "web"); r.Read.Owner != "t-env" {
		t.Fatalf("default owner %q", r.Read.Owner)
	}
	if r := mustParse(t, "stop", "web", "--owner", "x", "--force"); r.Stop.Owner != "x" || !r.Stop.Force {
		t.Fatalf("stop %+v", r.Stop)
	}
	if r := mustParse(t, "jobs", "--owner=", "--all"); r.Jobs.Owner != "" || !r.Jobs.All {
		t.Fatalf("jobs %+v", r.Jobs)
	}
	if r := mustParse(t, "search", "q", "--all"); !r.Search.All || r.Search.Owner != "t-env" {
		t.Fatalf("search %+v", r.Search)
	}
}

// The jobs table shows exit codes and keeps a multi-line command on one row.
func TestJobsTable(t *testing.T) {
	t.Setenv("TAIL_MODE_HOME", t.TempDir())
	s, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, j := range []store.Job{
		{ID: "j000001", Name: "a", Describe: "make test", StartedAt: 1},
		{ID: "j000002", Name: "b", Describe: "echo one\necho two", StartedAt: 2},
	} {
		j.Kind, j.Transport, j.Spec, j.Status = "cmd", "local", "{}", "starting"
		if err := s.CreateJob(j); err != nil {
			t.Fatal(err)
		}
		if err := s.Finish(j.ID, []string{"exited", "failed"}[i], 3, "boom", true); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	stdout = &buf
	t.Cleanup(func() { stdout = os.Stdout })
	if err := Jobs([]string{"--owner="}); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if strings.Count(got, "\n") != 3 || !strings.Contains(got, "exited 3") || !strings.Contains(got, "failed: boom") || !strings.Contains(got, "echo one ⏎ echo two") {
		t.Fatalf("jobs:\n%s", got)
	}
}
