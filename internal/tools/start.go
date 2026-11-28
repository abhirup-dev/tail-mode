package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	toon "github.com/toon-format/toon-go"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

type StartIn struct {
	Caller
	File        string            `json:"file,omitempty" jsonschema:"log file to tail -F (shorthand for source=file); for host jobs a path on that host"`
	Cmd         string            `json:"cmd,omitempty" jsonschema:"long-running shell command to run and capture, e.g. a dev server, build, test watcher or remote script (shorthand for source=cmd)"`
	Dir         string            `json:"dir,omitempty" jsonschema:"cmd only: working dir; for host jobs on that host (default: its home)"`
	Env         map[string]string `json:"env,omitempty" jsonschema:"cmd only: variables exported in the job's shell on the target host (the remote host for host jobs), before the command; PYTHONUNBUFFERED=1 unless set"`
	Shell       string            `json:"shell,omitempty" jsonschema:"cmd only: POSIX shell to run it with, name or absolute path (default ${shell})"`
	Login       bool              `json:"login,omitempty" jsonschema:"cmd only: run in a login shell (-lc) so profile files set PATH (use when a tool is not found)"`
	Source      string            `json:"source,omitempty" jsonschema:"source adapter kind; use with config"`
	Config      any               `json:"config,omitempty" jsonschema:"adapter config object, e.g. file: {path, backlog}; cmd: {cmd, dir, env, shell, login}"`
	Host        string            `json:"host,omitempty" jsonschema:"ssh host to run on (as in your ssh config, e.g. vps or user@box); omit to run locally"`
	Name        string            `json:"name,omitempty" jsonschema:"job name to refer to it by; names are scoped to your own jobs"`
	Backlog     *int              `json:"backlog,omitempty" jsonschema:"file only: lines of history on start (default ${backlog}, 0 = none)"`
	Restart     *bool             `json:"restart,omitempty" jsonschema:"restart when the source ends (default: file yes, cmd no)"`
	NotifyEvery string            `json:"notify_every,omitempty" jsonschema:"${everyDoc}"`
	NotifyMatch string            `json:"notify_match,omitempty" jsonschema:"regex: only digest matching lines"`
}

const startUsage = `give exactly one of cmd="<command>" (+ dir="<absolute dir>"), file="<absolute path>", or source="<kind>" + config={...}`

// startOpts maps the tool params onto jobs.StartOpts (local defaults not yet applied); adapters
// validate their own config in jobs.Start.
func startOpts(in StartIn) (jobs.StartOpts, error) {
	o := jobs.StartOpts{Name: in.Name, Spec: source.Spec{Restart: in.Restart}}
	if in.Host != "" {
		o.Transport, o.Target = "ssh", in.Host
	}
	n := 0
	for _, set := range []bool{in.File != "", in.Cmd != "", in.Source != ""} {
		if set {
			n++
		}
	}
	var cfg any
	switch {
	case n != 1:
		return o, fail(startUsage, "tail_start needs exactly one of cmd, file, or source (got %d)", n)
	case in.Config != nil && in.Source == "":
		return o, fail(startUsage, "config needs source")
	case in.Backlog != nil && in.File == "":
		return o, fail("with source, put backlog in config", "backlog applies to file")
	case (in.Dir != "" || len(in.Env) > 0 || in.Shell != "" || in.Login) && in.Cmd == "":
		return o, fail("with source, put them in config", "dir, env, shell and login apply to cmd")
	case in.File != "":
		o.Spec.Kind, cfg = "file", source.FileConfig{Path: in.File, Backlog: in.Backlog}
	case in.Cmd != "":
		o.Spec.Kind, cfg = "cmd", source.CmdConfig{Cmd: in.Cmd, Dir: in.Dir, Env: in.Env, Shell: in.Shell, Login: in.Login}
	default:
		o.Spec.Kind, cfg = in.Source, in.Config
		if s, ok := in.Config.(string); ok { // some clients send the config object as a JSON string
			if !json.Valid([]byte(s)) {
				return o, fail(`pass config as an object, e.g. config={"cmd": "make"}`, "config is not valid JSON")
			}
			cfg = json.RawMessage(s)
		}
	}
	var err error
	if cfg != nil {
		o.Spec.Config, err = json.Marshal(cfg)
	}
	return o, err
}

func (h *handler) start(ctx context.Context, in StartIn) (string, error) {
	o, err := startOpts(in)
	if err != nil {
		return "", err
	}
	o.Owner = h.owner(in.Caller)
	if !h.shared { // shared: the cwd is not the caller's, so relative local paths are refused (ErrNeedAbsolute)
		if o.Cwd, err = os.Getwd(); err != nil {
			return "", err
		}
	}
	wantNotify := in.NotifyEvery != "" || in.NotifyMatch != ""
	var n follow.Options
	if wantNotify { // validate before starting, so a bad regex does not leave an unwatched job
		// After 0: the job is new, so the notifier sees all of its lines, including backlog and fast output.
		if n, err = notifyOpts("notify_every", in.NotifyEvery, in.NotifyMatch, 0); err != nil {
			return "", err
		}
	}
	j, err := startJob(h.s, o)
	if errors.Is(err, source.ErrNeedAbsolute) {
		why := fmt.Sprintf("%v; this server cannot see your current dir", err)
		if o.Spec.Kind == "file" {
			return "", fail(`pass the file as an absolute path, e.g. file="/home/me/app/logs/app.log", or as ~/x (run pwd in your shell to get your dir)`, "%s", why)
		}
		return "", fail(`pass dir as an absolute path, e.g. dir="/home/me/app" (with source+config, put "dir" in config), or as ~/x (run pwd in your shell to get your dir)`, "%s", why)
	}
	var nr *jobs.NameRunningError
	if errors.As(err, &nr) {
		return "", fail(fmt.Sprintf("it is already yours: tail_read job=%s reads it, tail_notify job=%s watches it; or tail_stop it first, or pick another name", nr.ID, nr.ID), "%v", err)
	}
	if err != nil {
		return "", err
	}
	result(ctx).Job = j.ID
	fields := []toon.Field{f("job", j.ID)}
	if j.Name != "" {
		fields = append(fields, f("name", j.Name))
	}
	fields = append(fields, h.resolved(j)...)
	fields = append(fields, f("source", store.Clip(j.Describe, 200)), f("status", j.StatusText()))
	out := kv(f("started", toon.NewObject(fields...)))
	if j.Owner == "" {
		out += kv(f("warning", sharedWarning))
	}
	help := []string{
		fmt.Sprintf("tail_wait job=%s until=<regex> blocks until a line matches (e.g. ready|listening)", j.ID),
		fmt.Sprintf("tail_read job=%s reads its output; tail_stop job=%s stops it", j.ID, j.ID),
	}
	switch {
	case !wantNotify && h.pushes():
		out += kv(f("notifications", "off; tail_notify sends digests "+h.destination()))
	case !wantNotify:
		out += kv(f("notifications", "off; "+h.destination()))
	case !h.pushes():
		out += kv(f("notifications", h.destination()))
		help = append(help[:1], recipeLines(j.ID, n)...)
	default:
		if err := h.notify(ctx, j.ID, n); err != nil {
			out += kv(f("notifications", "failed: "+err.Error()+" (the job is running)"))
			help = append(help[:1], fmt.Sprintf("tail_notify job=%s retries the notifier", j.ID))
		} else {
			out += kv(f("notifications", "on "+h.destination()), f("notify", notifyDesc(n, "the job's first line")))
			help = endTurn(j.ID)
		}
	}
	return out + helpBlock(help...), nil
}

// resolved echoes what a start resolved to (jobs.Start applied the local defaults), so the agent sees as
// whom and where its job runs.
func (h *handler) resolved(j store.Job) []toon.Field {
	owner := j.Owner
	switch {
	case owner == "":
		owner = "none (shared namespace)"
	case owner == h.self:
		owner = h.selfName()
	}
	where := "local"
	if j.Target != "" {
		where = j.Transport + " " + j.Target
	}
	out := []toon.Field{f("owner", owner), f("on", where)}
	var sp struct {
		Config struct{ Dir, Path string }
	}
	_ = json.Unmarshal([]byte(j.Spec), &sp)
	switch {
	case sp.Config.Dir != "":
		out = append(out, f("dir", sp.Config.Dir))
	case j.Kind == "cmd":
		out = append(out, f("dir", "~ (remote home)"))
	}
	if sp.Config.Path != "" {
		out = append(out, f("path", sp.Config.Path))
	}
	return out
}

type StopIn struct {
	Caller
	Job   string `json:"job" jsonschema:"job id, or the name of one of your jobs (your newest job with it)"`
	Force bool   `json:"force,omitempty" jsonschema:"stop it even if it is another owner's job (do not stop other agents' jobs)"`
}

func (h *handler) stop(_ context.Context, in StopIn) (string, error) {
	owner := h.owner(in.Caller)
	j, err := h.job(in.Job, owner)
	if err != nil {
		return "", err
	}
	if st := jobs.Status(h.s, j); !store.Live(st) { // idempotent: before the owner check, like a no-op
		j.Status = st
		return kv(f("job", j.Label()+" already "+j.StatusText()+" (no-op)")), nil
	}
	j, err = jobs.Stop(h.s, j.ID, owner, in.Force)
	if noe := (*jobs.NotOwnerError)(nil); errors.As(err, &noe) {
		return "", fail("do not stop other agents' jobs; pass force: true only if you are sure",
			"job %s belongs to owner '%s', not to %s", noe.Job, noe.Owner, h.scope(owner))
	}
	if err != nil {
		return "", err
	}
	return kv(f("job", j.Label()+" "+j.StatusText())), nil
}

type JobsIn struct {
	Caller
	All bool `json:"all,omitempty" jsonschema:"list every owner's jobs, not just yours"`
}

func (h *handler) jobsList(_ context.Context, in JobsIn) (string, error) {
	owner := h.owner(in.Caller)
	js, err := jobs.List(h.s, owner, in.All)
	if err != nil {
		return "", err
	}
	scope := "for " + h.scope(owner)
	if in.All {
		scope = "of all owners"
	}
	if len(js) == 0 {
		help := []string{`tail_start cmd="<command>" dir="<absolute dir>" runs one`}
		if !in.All {
			help = append(help, "all=true lists every owner's jobs")
		}
		return kv(f("jobs", "0 jobs "+scope)) + helpBlock(help...), nil
	}
	rows := make([]toon.Object, len(js))
	for i, j := range js {
		fs := []toon.Field{f("id", j.ID), f("name", store.OneLine(j.Name)), f("status", store.Clip(j.StatusText(), 80)), f("lines", j.Lines),
			f("last_output", store.Ago(j.LastTS, "never")), f("source", store.Clip(j.Describe, 80))}
		if in.All {
			fs = append(fs, f("owner", j.Owner))
		}
		rows[i] = toon.NewObject(fs...)
	}
	t := newTable("jobs", rows, len(rows))
	return render(kv(f("count", plural(len(js), "job", "jobs")+" "+scope+" · newest first")), t, func(shown int) string {
		help := []string{"tail_read job=<id> after=<cursor> reads output", "tail_stop job=<id> stops a job"}
		if shown < len(js) {
			help = append(help, fmt.Sprintf("%d of %d jobs shown (output budget); tail_sql over jobs lists the rest", shown, len(js)))
		}
		return helpBlock(help...)
	}), nil
}
