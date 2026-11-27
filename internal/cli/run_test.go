package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

func TestStartCmdOptions(t *testing.T) {
	cfg := func(args ...string) (source.CmdConfig, error) {
		t.Helper()
		r := mustParse(t, append([]string{"start"}, args...)...)
		o, err := r.Start.opts()
		var c source.CmdConfig
		if err == nil {
			json.Unmarshal(o.Spec.Config, &c)
		}
		return c, err
	}
	// Repeated --env keeps commas and semicolons in values (no kong list splitting); split at the first "=".
	c, err := cfg("--cmd", "make", "--env", "A=x,y", "--env", "B=p;q=r", "--env", "E=", "--dir", "~/src", "--shell", "zsh", "--login")
	want := source.CmdConfig{Cmd: "make", Dir: "~/src", Shell: "zsh", Login: true, Env: map[string]string{"A": "x,y", "B": "p;q=r", "E": ""}}
	if err != nil || !reflect.DeepEqual(c, want) {
		t.Fatalf("got %+v, %v", c, err)
	}
	for _, bad := range [][]string{
		{"--file", "/x", "--dir", "/d"},
		{"--file", "/x", "--login"},
		{"--source", "cmd", "--config", `{"cmd":"x"}`, "--env", "A=1"},
		{"--cmd", "x", "--env", "NOEQ"},
	} {
		if _, err := cfg(bad...); err == nil {
			t.Errorf("%q: want error", bad)
		}
	}
}

func TestRunCommandJoin(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"run", "--", "npm run dev | tee x"}, "npm run dev | tee x"},
		{[]string{"run", "--", "ls", "-la", "a b", "x'y"}, `ls -la 'a b' 'x'"'"'y'`},
		{[]string{"run", "--name", "w", "--", "python3", "-m", "http.server"}, "python3 -m http.server"},
		{[]string{"run", "--every", "10s", "--", "make", "--keep-going"}, "make --keep-going"},
	} {
		r := mustParse(t, c.args...)
		argv := r.RunCmd.Argv
		if len(argv) > 0 && argv[0] == "--" {
			argv = argv[1:]
		}
		if got := command(argv); got != c.want {
			t.Errorf("%q → %q, want %q", c.args, got, c.want)
		}
	}
}

// fakeRun replaces the job/notifier seams: the fake job is created in the store with lines, and finished
// when ended is true. It returns the captured StartOpts and notifier calls.
func fakeRun(t *testing.T, lines []string, ended bool) (*jobs.StartOpts, *[]follow.NotifierOpts, *bytes.Buffer) {
	t.Helper()
	t.Setenv("TAIL_MODE_HOME", t.TempDir())
	t.Setenv("T3CODE_THREAD_ID", "")
	var got jobs.StartOpts
	var notifiers []follow.NotifierOpts
	oldJob, oldNotifier := startJob, startNotifier
	startJob = func(s *store.Store, o jobs.StartOpts) (store.Job, error) {
		got = o
		j := store.Job{ID: store.NewJobID(), Name: o.Name, Kind: o.Spec.Kind, Transport: "local", Spec: string(o.Spec.Config),
			Describe: "fake", Status: "starting", StartedAt: store.NowMS()}
		if err := s.CreateJob(j); err != nil {
			return j, err
		}
		if len(lines) > 0 {
			if err := s.Append(j.ID, lines, store.NowMS()); err != nil {
				return j, err
			}
		}
		if ended {
			if err := s.Finish(j.ID, "exited", 0, "", true); err != nil {
				return j, err
			}
		}
		return s.GetJob(j.ID)
	}
	startNotifier = func(job string, o follow.NotifierOpts) (int, error) {
		notifiers = append(notifiers, o)
		return 4242, nil
	}
	var buf bytes.Buffer
	stdout = &buf
	t.Cleanup(func() { startJob, startNotifier, stdout = oldJob, oldNotifier, os.Stdout })
	return &got, &notifiers, &buf
}

func TestRunLocalDefaultsAndRecipe(t *testing.T) {
	o, notifiers, out := fakeRun(t, nil, false)
	if err := Run([]string{"run", "--name", "dev", "--match", "ERROR", "--env", "A=1", "--", "npm", "run", "dev"}); err != nil {
		t.Fatal(err)
	}
	var c source.CmdConfig
	json.Unmarshal(o.Spec.Config, &c)
	cwd, _ := os.Getwd()
	if o.Spec.Kind != "cmd" || o.Transport != "" || c.Cmd != "npm run dev" || c.Dir != cwd || c.Env["A"] != "1" || o.Name != "dev" {
		t.Fatalf("opts %+v config %+v", o, c)
	}
	lines := strings.Split(out.String(), "\n")
	if !strings.Contains(lines[0], "(dev) started") || !strings.Contains(out.String(), "Arm the Monitor tool") || len(*notifiers) != 0 {
		t.Fatalf("output %q notifiers %v", out, *notifiers)
	}
	if !strings.Contains(out.String(), "--every 30s --match ERROR --after 0") {
		t.Fatalf("recipe %q", out)
	}
}

func TestRunRemoteKeepsDirAndNotifies(t *testing.T) {
	o, notifiers, out := fakeRun(t, nil, false)
	if err := Run([]string{"run", "--host", "vps", "--every", "1m", "--thread", "T1", "--", "tail -f x"}); err != nil {
		t.Fatal(err)
	}
	if o.Transport != "ssh" || o.Target != "vps" || string(o.Spec.Config) != `{"cmd":"tail -f x"}` {
		t.Fatalf("opts %+v %s", o, o.Spec.Config)
	}
	if len(*notifiers) != 1 {
		t.Fatalf("notifiers %v", *notifiers)
	}
	if n := (*notifiers)[0]; n.After != 0 || n.Deliver != "t3:T1" || n.Every.String() != "1m0s" || n.MaxLines != 10 {
		t.Fatalf("notifier %+v", n)
	}
	if !strings.Contains(out.String(), "notifier pid 4242") {
		t.Fatalf("output %q", out)
	}
}

func TestRunUntil(t *testing.T) {
	_, _, out := fakeRun(t, []string{"booting", "listening on :3000", "x"}, false)
	if err := Run([]string{"run", "--until", "listening", "--", "npm start"}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "started") || !strings.HasSuffix(lines[1], "listening on :3000") {
		t.Fatalf("output %q", out)
	}

	_, _, _ = fakeRun(t, []string{"booting", "crashed"}, true)
	var ee store.ExitError
	if err := Run([]string{"run", "--until", "listening", "--", "npm start"}); !errors.As(err, &ee) || ee.Code != 4 {
		t.Fatalf("ended job: want exit 4, got %v", err)
	}
}

func TestRunErrors(t *testing.T) {
	fakeRun(t, nil, false)
	for _, args := range [][]string{
		{"run"},
		{"run", "--"},
		{"run", "--match", "(", "--", "x"},
		{"run", "--until", "(", "--", "x"},
		{"run", "--thread", "T1", "--", "x"}, // a destination without --every/--match
		{"run", "--env", "1A=x", "--", "x"},
	} {
		if err := Run(args); err == nil {
			t.Errorf("%q: want error", args)
		}
	}
	parseErr(t, "run", "--deliver", "jsonl", "--thread", "T1", "--", "x")
}
