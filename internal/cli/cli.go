// Package cli is layer 5's command line: every tail-mode command, parsed by one kong grammar.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"al.essio.dev/pkg/shellescape"
	"github.com/alecthomas/kong"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/tools"
)

// stdout is where command output goes (a test knob).
var stdout io.Writer = os.Stdout

// ExitError makes main exit with Code without printing anything (wait/run --until: 3 timeout, 4 ended).
type ExitError struct{ Code int }

func (e ExitError) Error() string { return fmt.Sprintf("exit %d", e.Code) }

type root struct {
	Start  startCmd  `cmd:"" help:"Start a tail job."`
	RunCmd runCmd    `cmd:"" name:"run" help:"Run a long-running command as a job; optionally notify, or block until a line matches."`
	Stop   stopCmd   `cmd:"" help:"Stop a job."`
	Jobs   jobsCmd   `cmd:"" help:"List jobs."`
	Read   readCmd   `cmd:"" help:"Read a job's lines."`
	Search searchCmd `cmd:"" help:"Full-text search (FTS5), newest first."`
	SQL    sqlCmd    `cmd:"" name:"sql" help:"Read-only SQL over the jobs and lines tables."`
	Follow followCmd `cmd:"" help:"Stream digests of a job's new lines until it ends."`
	Wait   waitCmd   `cmd:"" help:"Wait for a matching line (exit 0; 3 on timeout; 4 if the job ended)."`
	Notify notifyCmd `cmd:"" help:"Start a detached follower that pushes digests to an agent."`
	MCP    mcpCmd    `cmd:"" name:"mcp" help:"Run the stdio MCP server."`
	Tool   toolCmd   `cmd:"" hidden:"" help:"Run one MCP tool (tail-mode tool --list | tail-mode tool NAME [JSON|-] [--client pi])."`
	Job    jobCmd    `cmd:"" name:"_job" hidden:"" help:"Internal: run a job's worker."`
}

func parse(r *root, args []string) (*kong.Context, error) {
	p, err := kong.New(r, kong.Name("tail-mode"),
		kong.Description("Tail local/remote logs and commands for coding agents."),
		kong.Writers(stdout, os.Stderr),
		kong.Vars{"every": follow.DefaultEvery.String(), "maxLines": strconv.Itoa(follow.DefaultMaxLines), "owner": tools.EnvThread()},
		kong.WithHyphenPrefixedParameters(true)) // flag values may start with "-": --after -1, --grep -x
	if err != nil {
		return nil, err
	}
	return p.Parse(args)
}

// Run parses args (without the program name) and runs the selected command.
func Run(args []string) error {
	if len(args) == 0 {
		args = []string{"--help"}
	}
	ctx, err := parse(&root{}, args)
	if err != nil {
		return err
	}
	return ctx.Run()
}

// ---- start / run / stop / jobs / _job

// Seams for tests: real jobs and notifiers detach processes (and a notifier can wake a T3 thread).
var (
	startJob      = jobs.Start
	startNotifier = follow.StartNotifier
)

// ownerOpt scopes a command to one owner: job names resolve among that owner's jobs, and new jobs
// get it. In T3 it defaults to the thread id, so each agent thread has its own namespace.
type ownerOpt struct {
	Owner string `default:"${owner}" placeholder:"O" help:"Job owner: names resolve among its jobs, new jobs get it (default $T3CODE_THREAD_ID; '' = shared namespace)."`
}

// cmdOpts are the cmd source's options, shared by start and run.
type cmdOpts struct {
	Dir   string   `placeholder:"D" help:"With a command: working dir (default: the current dir; remote: home). Quote '~/x' so it is the target's home."`
	Env   []string `placeholder:"K=V" sep:"none" help:"With a command: export K=V before it (repeatable). PYTHONUNBUFFERED=1 is set unless given."`
	Shell string   `placeholder:"S" help:"With a command: POSIX shell to run it with, name or absolute path (default bash)."`
	Login bool     `help:"With a command: run it in a login shell (-lc), so profile files set PATH."`
}

// spec adds the cmd options to in (--env split at the first "=") and builds the source spec, with the
// builder's errors reworded in flag names.
func (o cmdOpts) spec(in source.SpecInput) (source.Spec, error) {
	in.Dir, in.Shell, in.Login = o.Dir, o.Shell, o.Login
	for _, kv := range o.Env {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return source.Spec{}, fmt.Errorf("--env %q: want K=V", kv)
		}
		if in.Env == nil {
			in.Env = map[string]string{}
		}
		in.Env[k] = v
	}
	sp, err := in.Spec()
	switch {
	case errors.Is(err, source.ErrBacklogNeedsFile):
		err = errors.New("--backlog needs --file")
	case errors.Is(err, source.ErrConfigNeedsSource):
		err = errors.New("--config needs --source")
	case errors.Is(err, source.ErrCmdOptionsNeedCmd):
		err = errors.New("--dir, --env, --shell and --login need --cmd (with --source, put them in --config)")
	case errors.Is(err, source.ErrConfigInvalidJSON):
		err = errors.New("--config is not valid JSON")
	}
	return sp, err
}

// startOpts are StartOpts for a job on host ("" = local) started from this process's cwd.
func startOpts(name, owner, host string, sp source.Spec) jobs.StartOpts {
	o := jobs.StartOpts{Name: name, Owner: owner, Spec: sp}
	if host != "" {
		o.Transport, o.Target = "ssh", host
	}
	o.Cwd, _ = os.Getwd() // "" if unavailable: relative local paths are then refused
	return o
}

// start starts the job and prints it.
func start(s *store.Store, o jobs.StartOpts, asJSON bool) (store.Job, error) {
	j, err := startJob(s, o)
	if err != nil {
		return j, err
	}
	if asJSON {
		return j, printJSON(j)
	}
	fmt.Fprintf(stdout, "%s started (pid %d): %s\n", j.Label(), j.PID, store.OneLine(j.Describe))
	return j, nil
}

type startCmd struct {
	Name      string `placeholder:"N" help:"Job name (reference it instead of the id)."`
	Host      string `placeholder:"H" help:"SSH host; sugar for --transport ssh --target H." xor:"host-transport,host-target"`
	Transport string `placeholder:"T" help:"Transport (default local)." xor:"host-transport"`
	Target    string `placeholder:"X" help:"Transport target (e.g. an ssh host)." xor:"host-target"`
	Restart   *bool  `negatable:"" help:"Restart when the source ends (default depends on the source)."`
	File      string `placeholder:"PATH" help:"File to tail; sugar for --source file. Quote '~/x' so it is the target's home." xor:"source" required:""`
	Backlog   *int   `placeholder:"N" help:"With --file: lines of history to read first (default 50)."`
	Cmd       string `placeholder:"CMD" help:"Command to run; sugar for --source cmd." xor:"source" required:""`
	Source    string `placeholder:"KIND" help:"Source kind." xor:"source" required:""`
	Config    string `placeholder:"JSON" help:"With --source: adapter config as JSON."`
	cmdOpts   `embed:""`
	ownerOpt  `embed:""`
	JSON      bool `name:"json" help:"JSON output."`
}

// opts turns the flags (and their sugar) into jobs.StartOpts.
func (c *startCmd) opts() (jobs.StartOpts, error) {
	in := source.SpecInput{CmdConfig: source.CmdConfig{Cmd: c.Cmd}, File: c.File, Backlog: c.Backlog, Source: c.Source, Restart: c.Restart}
	if c.Config != "" {
		in.Config = json.RawMessage(c.Config)
	}
	sp, err := c.cmdOpts.spec(in)
	if err != nil {
		return jobs.StartOpts{}, err
	}
	o := startOpts(c.Name, c.Owner, c.Host, sp)
	if c.Host == "" {
		o.Transport, o.Target = c.Transport, c.Target
	}
	return o, nil
}

func (c *startCmd) Run() error {
	o, err := c.opts()
	if err != nil {
		return err
	}
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	_, err = start(s, o, c.JSON)
	return err
}

type runCmd struct {
	Name     string `placeholder:"N" help:"Job name (reference it instead of the id)."`
	Host     string `placeholder:"H" help:"Run over ssh on this host (default local)."`
	cmdOpts  `embed:""`
	ownerOpt `embed:""`
	Every    time.Duration `placeholder:"${every}" help:"Notify with digests this often (starts a notifier; min 5s)."`
	Match    string        `placeholder:"RE" help:"Notify only on lines matching this regex (starts a notifier; default every ${every})."`
	MaxLines int           `default:"${maxLines}" help:"Lines per digest."`
	Deliver  string        `xor:"dest" help:"Notifier sink, e.g. t3:THREAD (default t3:$T3CODE_THREAD_ID)." placeholder:"SPEC"`
	Thread   string        `xor:"dest" help:"T3 thread id for the notifier (default $T3CODE_THREAD_ID)." placeholder:"ID"`
	Until    string        `placeholder:"RE" help:"Block until a line matches (exit 0; 3 on timeout; 4 if the job ended). The job keeps running."`
	Timeout  time.Duration `default:"30m" help:"With --until: give up after this long (exit 3)."`
	JSON     bool          `name:"json" help:"JSON output."`
	Argv     []string      `arg:"" passthrough:"" placeholder:"CMD" help:"The command, after --. One argument is used verbatim as a shell command; several are shell-quoted and joined."`
}

// command is the shell command for argv: a single argument verbatim (so `run -- 'a | b'` works),
// several shell-quoted and joined.
func command(argv []string) string {
	if len(argv) == 1 {
		return argv[0]
	}
	return shellescape.QuoteCommand(argv)
}

func (c *runCmd) Run() error {
	argv := c.Argv
	if len(argv) > 0 && argv[0] == "--" {
		argv = argv[1:]
	}
	if len(argv) == 0 {
		return errors.New("run: no command (tail-mode run [flags] -- CMD...)")
	}
	until, err := compile(c.Until)
	if err != nil {
		return err
	}
	wantNotify := c.Every != 0 || c.Match != ""
	if !wantNotify && (c.Deliver != "" || c.Thread != "") {
		return errors.New("--deliver/--thread need --every or --match")
	}
	// After 0: the job is new, so the notifier sees all of its lines, including fast early output.
	n := follow.Options{Every: c.Every, Match: c.Match, MaxLines: c.MaxLines, After: 0}
	if err := n.Validate(); wantNotify && err != nil { // before starting, so a bad flag leaves no job
		return err
	}
	sp, err := c.cmdOpts.spec(source.SpecInput{CmdConfig: source.CmdConfig{Cmd: command(argv)}})
	if err != nil {
		return err
	}
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	j, err := start(s, startOpts(c.Name, c.Owner, c.Host, sp), c.JSON)
	if err != nil {
		return err
	}
	if wantNotify {
		if err := c.notify(s, j.ID, n); err != nil {
			return err
		}
	}
	if until == nil {
		return nil
	}
	// After 0, not -1: jobs.Start returns after the worker claims the job, so a fast "ready" line may
	// already be stored.
	return wait(s, j, until, c.Timeout, 0, c.JSON)
}

// notify starts run's notifier for job (deliver resolved like notify), or prints the recipe when
// there is no destination.
func (c *runCmd) notify(s *store.Store, job string, n follow.Options) error {
	var msg string
	if n.Deliver = resolveDeliver(c.Deliver, c.Thread); n.Deliver == "" {
		msg = "No T3 thread; to get woken:\n- " + strings.Join(tools.RecipeLines(job, n), "\n- ")
	} else {
		pid, err := startNotifier(s, job, n)
		if err != nil {
			return fmt.Errorf("job %s started, but its notifier did not: %w", job, err)
		}
		msg = fmt.Sprintf("notifier pid %d: digests every %s to %s", pid, n.Every, n.Deliver)
	}
	if c.JSON {
		return printJSON(map[string]string{"notify": msg})
	}
	fmt.Fprintln(stdout, msg)
	return nil
}

type stopCmd struct {
	Job      string `arg:"" help:"Job id or name."`
	Force    bool   `help:"Stop it even if it is another owner's job."`
	ownerOpt `embed:""`
	JSON     bool `name:"json" help:"JSON output."`
}

func (c *stopCmd) Run() error {
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	j, err := jobs.Stop(s, c.Job, c.Owner, c.Force)
	if noe := (*jobs.NotOwnerError)(nil); errors.As(err, &noe) {
		return fmt.Errorf("%w; pass --force to stop it anyway", err)
	}
	if err != nil {
		return err
	}
	if c.JSON {
		return printJSON(j)
	}
	fmt.Fprintf(stdout, "%s %s\n", j.Label(), j.StatusText())
	return nil
}

type jobsCmd struct {
	All      bool `help:"Every owner's jobs, not just --owner's."`
	ownerOpt `embed:""`
	JSON     bool `name:"json" help:"JSON output."`
}

func (c *jobsCmd) Run() error {
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	js, err := jobs.List(s, c.Owner, c.All)
	if err != nil {
		return err
	}
	if c.JSON {
		if js == nil {
			js = []store.Job{}
		}
		return printJSON(js)
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprint(tw, "ID\tNAME\tSTATUS\tLINES\tLAST\tSOURCE")
	if c.All {
		fmt.Fprint(tw, "\tOWNER")
	}
	fmt.Fprintln(tw)
	for _, j := range js {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s", j.ID, store.OneLine(j.Name), store.Clip(j.StatusText(), 60), j.Lines,
			store.Ago(j.LastTS, "-"), store.OneLine(j.Describe))
		if c.All {
			fmt.Fprintf(tw, "\t%s", j.Owner)
		}
		fmt.Fprintln(tw)
	}
	return tw.Flush()
}

type jobCmd struct {
	ID string `arg:""`
}

func (c *jobCmd) Run() error {
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	return jobs.Run(s, c.ID)
}

// ---- read / search / sql

type readCmd struct {
	Job      string `arg:"" help:"Job id or name."`
	After    int64  `help:"Only lines with id > ID (exclusive cursor)." placeholder:"ID"`
	Tail     int    `help:"Newest N lines." placeholder:"N"`
	Grep     string `help:"Only lines matching this regex." placeholder:"RE"`
	Limit    int    `default:"200" help:"Max lines."`
	ownerOpt `embed:""`
	JSON     bool `name:"json" help:"JSON output."`
}

func (c *readCmd) Run() error {
	re, err := compile(c.Grep)
	if err != nil {
		return err
	}
	s, j, err := openJob(c.Job, c.Owner)
	if err != nil {
		return err
	}
	defer s.Close()
	lines, err := s.Read(store.ReadQuery{Job: j.ID, After: c.After, Tail: c.Tail, Grep: re, Limit: c.Limit})
	if err != nil {
		return err
	}
	return printLines(lines, c.JSON, false)
}

type searchCmd struct {
	Query    []string `arg:"" help:"FTS5 query (words are joined with spaces)."`
	Job      string   `help:"Only this job (id, or name among --owner's jobs)."`
	All      bool     `help:"Every owner's jobs, not just --owner's."`
	ownerOpt `embed:""`
	Since    time.Duration `help:"Only lines newer than this, e.g. 1h."`
	Limit    int           `default:"50" help:"Max lines."`
	JSON     bool          `name:"json" help:"JSON output."`
}

func (c *searchCmd) Run() error {
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	q := store.SearchQuery{Query: strings.Join(c.Query, " "), Limit: c.Limit, Owner: c.Owner, All: c.All}
	if c.Job != "" {
		j, err := s.GetJob(c.Job, c.Owner)
		if err != nil {
			return err
		}
		q.Job = j.ID
	}
	if c.Since > 0 {
		q.SinceTS = time.Now().Add(-c.Since).UnixMilli()
	}
	lines, err := s.Search(q)
	if err != nil {
		return err
	}
	return printLines(lines, c.JSON, q.Job == "")
}

type sqlCmd struct {
	Query   []string      `arg:"" help:"One read-only SQL statement (words are joined with spaces)."`
	Limit   int           `default:"500" help:"Max rows."`
	Timeout time.Duration `default:"5s" help:"Stop the query after this long."`
	JSON    bool          `name:"json" help:"JSON output."`
}

func (c *sqlCmd) Run() error {
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	ctx := context.Background()
	if c.Timeout > 0 { // else store.Query applies its default
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	cols, rows, err := s.Query(ctx, strings.Join(c.Query, " "), c.Limit)
	if err != nil {
		return err
	}
	if c.JSON {
		if rows == nil {
			rows = [][]any{}
		}
		return json.NewEncoder(stdout).Encode(map[string]any{"columns": cols, "rows": rows})
	}
	fmt.Fprintln(stdout, strings.Join(cols, "\t"))
	flat := strings.NewReplacer("\t", " ", "\n", " ")
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, v := range r {
			if v != nil {
				cells[i] = flat.Replace(fmt.Sprint(v))
			}
		}
		fmt.Fprintln(stdout, strings.Join(cells, "\t"))
	}
	return nil
}

// ---- follow / wait / notify / mcp

type followCmd struct {
	Job      string        `arg:"" help:"Job id or name."`
	Every    time.Duration `default:"${every}" help:"Poll interval (min 5s)."`
	Match    string        `help:"Only digest lines matching this regex." placeholder:"RE"`
	MaxLines int           `default:"${maxLines}" help:"Lines per digest."`
	After    int64         `default:"-1" help:"Start cursor (-1 = the job's current end)." placeholder:"ID"`
	Deliver  string        `default:"stdout" help:"Sink: stdout | jsonl | t3:THREAD."`
	Notifier bool          `hidden:"" help:"Internal: register as the one notifier for this job and --deliver (set by notify)."`
	ownerOpt `embed:""`
}

func (c *followCmd) Run() error {
	sink, err := deliver.New(c.Deliver)
	if err != nil {
		return err
	}
	s, j, err := openJob(c.Job, c.Owner)
	if err != nil {
		return err
	}
	defer s.Close()
	if c.Notifier {
		release, err := follow.Register(s, j.ID, c.Deliver)
		if err != nil {
			return err
		}
		defer release()
	}
	ctx, stop := signalCtx()
	defer stop()
	err = follow.Follow(ctx, s, j, follow.Options{Every: c.Every, Match: c.Match, MaxLines: c.MaxLines, After: c.After}, sink)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

type waitCmd struct {
	Job      string        `arg:"" help:"Job id or name."`
	Until    string        `required:"" help:"Regex to wait for." placeholder:"RE"`
	Timeout  time.Duration `default:"30m" help:"Give up after this long (exit 3)."`
	After    int64         `default:"-1" help:"Start cursor (-1 = the job's current end)." placeholder:"ID"`
	ownerOpt `embed:""`
	JSON     bool `name:"json" help:"JSON output."`
}

func (c *waitCmd) Run() error {
	re, err := regexp.Compile(c.Until)
	if err != nil {
		return err
	}
	s, j, err := openJob(c.Job, c.Owner)
	if err != nil {
		return err
	}
	defer s.Close()
	return wait(s, j, re, c.Timeout, c.After, c.JSON)
}

// wait blocks until a line of j after cursor after matches until and prints the matches; a timeout or
// the job's end becomes an ExitError with follow's wait code.
func wait(s *store.Store, j store.Job, until *regexp.Regexp, timeout time.Duration, after int64, asJSON bool) error {
	ctx, stop := signalCtx()
	defer stop()
	lines, code, _, err := follow.WaitCursor(ctx, s, j, until, timeout, after)
	if err != nil {
		return err
	}
	if code != 0 {
		return ExitError{Code: code}
	}
	return printLines(lines, asJSON, false)
}

type notifyCmd struct {
	Job      string        `arg:"" help:"Job id or name."`
	Every    time.Duration `default:"${every}" help:"Poll interval (min 5s)."`
	Match    string        `help:"Only notify on lines matching this regex." placeholder:"RE"`
	MaxLines int           `default:"${maxLines}" help:"Lines per digest."`
	After    int64         `default:"-1" help:"Start cursor (-1 = the job's current end)." placeholder:"ID"`
	Deliver  string        `xor:"dest" help:"Sink spec, e.g. t3:THREAD." placeholder:"SPEC"`
	Thread   string        `xor:"dest" help:"T3 thread id (default $T3CODE_THREAD_ID)." placeholder:"ID"`
	ownerOpt `embed:""`
}

// resolveDeliver resolves where digests go: deliverFlag, else T3 via thread or $T3CODE_THREAD_ID, else "".
func resolveDeliver(deliverFlag, thread string) string {
	if deliverFlag != "" {
		return deliverFlag
	}
	if thread == "" {
		thread = tools.EnvThread()
	}
	if thread == "" {
		return ""
	}
	return "t3:" + thread
}

func (c *notifyCmd) deliverSpec() (string, error) {
	if spec := resolveDeliver(c.Deliver, c.Thread); spec != "" {
		return spec, nil
	}
	return "", errors.New("notify: nowhere to deliver (--deliver SPEC, --thread ID, or $T3CODE_THREAD_ID)")
}

func (c *notifyCmd) Run() error {
	spec, err := c.deliverSpec()
	if err != nil {
		return err
	}
	s, j, err := openJob(c.Job, c.Owner)
	if err != nil {
		return err
	}
	defer s.Close()
	pid, err := startNotifier(s, j.ID, follow.Options{Every: c.Every, Match: c.Match, MaxLines: c.MaxLines, After: c.After, Deliver: spec})
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, pid)
	return nil
}

type mcpCmd struct{}

func (mcpCmd) Run() error {
	ctx, stop := signalCtx()
	defer stop()
	return tools.Serve(ctx)
}

// toolCmd runs the MCP tools for clients that are not MCP (the Pi extension). The text and the exit
// status match the MCP result; tool errors are printed to stdout too.
type toolCmd struct {
	List   bool   `help:"Print the tools (name, description, inputSchema) as JSON."`
	Client string `enum:",pi" default:"" help:"pi: the client runs digest followers itself; prints {text, follow?} JSON."`
	Name   string `arg:"" optional:"" help:"Tool name."`
	Args   string `arg:"" optional:"" default:"{}" help:"JSON arguments, or - for stdin."`
}

func (c *toolCmd) Run() error {
	if c.List {
		return printJSON(tools.List())
	}
	args := []byte(c.Args)
	if c.Args == "-" {
		var err error
		if args, err = io.ReadAll(os.Stdin); err != nil {
			return err
		}
	}
	ctx, stop := signalCtx()
	defer stop()
	r, err := tools.Exec(ctx, c.Name, args, c.Client == "pi")
	switch {
	case err != nil:
		fmt.Fprintln(stdout, err)
		return ExitError{Code: 1}
	case c.Client == "pi":
		return printJSON(r)
	}
	_, err = io.WriteString(stdout, r.Text)
	return err
}

// ---- helpers

func signalCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}

// openJob opens the store and resolves ref (an id, or a name among owner's jobs).
func openJob(ref, owner string) (*store.Store, store.Job, error) {
	s, err := store.Open("")
	if err != nil {
		return nil, store.Job{}, err
	}
	j, err := s.GetJob(ref, owner)
	if err != nil {
		s.Close()
		return nil, j, err
	}
	return s, j, nil
}

func compile(re string) (*regexp.Regexp, error) {
	if re == "" {
		return nil, nil
	}
	return regexp.Compile(re)
}

// printJSON prints v as one line of compact JSON (agents parse it).
func printJSON(v any) error { return json.NewEncoder(stdout).Encode(v) }

func printLines(lines []store.Line, asJSON, withJob bool) error {
	if asJSON {
		if lines == nil {
			lines = []store.Line{}
		}
		return printJSON(lines)
	}
	for _, l := range lines {
		ts := store.Clock(l.TS)
		if withJob {
			fmt.Fprintf(stdout, "%d %s %s %s\n", l.ID, l.Job, ts, l.Text)
		} else {
			fmt.Fprintf(stdout, "%d %s %s\n", l.ID, ts, l.Text)
		}
	}
	return nil
}
