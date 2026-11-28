// Package cli is layer 5's command line, parsed by one kong grammar. Each job command maps its flags onto
// one tool's args and runs that tool (internal/tools: the same handlers and text as MCP); follow, _job, mcp
// and tool are the commands with no tool.
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
	"strings"
	"syscall"
	"time"

	"al.essio.dev/pkg/shellescape"
	"github.com/alecthomas/kong"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/tools"
)

// stdout is where command output goes (a test knob).
var stdout io.Writer = os.Stdout

// ExitError makes main exit with Code without printing anything: 1 after a tool error (already printed),
// follow.WaitTimeout / follow.WaitEnded from wait and run --until.
type ExitError struct{ Code int }

func (e ExitError) Error() string { return fmt.Sprintf("exit %d", e.Code) }

type root struct {
	Start  startCmd  `cmd:"" help:"Start a job: tail a file or run a command (tail_start)."`
	RunCmd runCmd    `cmd:"" name:"run" help:"Run a long-running command as a job; optionally notify, or block until a line matches."`
	Stop   stopCmd   `cmd:"" help:"Stop a job (tail_stop)."`
	Jobs   jobsCmd   `cmd:"" help:"List jobs (tail_jobs)."`
	Read   readCmd   `cmd:"" help:"Read a job's lines (tail_read)."`
	Search searchCmd `cmd:"" help:"Full-text search (FTS5), newest first (tail_search)."`
	SQL    sqlCmd    `cmd:"" name:"sql" help:"Read-only SQL over the jobs, lines and notifiers tables (tail_sql)."`
	Follow followCmd `cmd:"" help:"Stream digests of a job's new lines until it ends."`
	Wait   waitCmd   `cmd:"" help:"Wait for a matching line (exit 0; 3 on timeout; 4 if the job ended)."`
	Notify notifyCmd `cmd:"" help:"Push digests of a job to this T3 thread (tail_notify)."`
	MCP    mcpCmd    `cmd:"" name:"mcp" help:"Run the stdio MCP server."`
	Tool   toolCmd   `cmd:"" hidden:"" help:"Run one MCP tool (tail-mode tool --list | tail-mode tool NAME [JSON|-] [--client pi])."`
	Job    jobCmd    `cmd:"" name:"_job" hidden:"" help:"Internal: run a job's worker."`
}

func parse(r *root, args []string) (*kong.Context, error) {
	p, err := kong.New(r, kong.Name("tail-mode"),
		kong.Description("Tail local/remote logs and commands for coding agents."),
		kong.Writers(stdout, os.Stderr),
		kong.Vars(tools.Vars), kong.Vars{"owner": tools.EnvOwner()},
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

// ---- tool commands

// toolCommand is a command that is one tool call: its flags mapped onto that tool's args.
type toolCommand interface {
	tool() (name string, args any)
}

func runTool(c toolCommand) error {
	_, err := call(c.tool())
	return err
}

// call runs tool name as the CLI client and prints its text. A tool error goes to stderr (exit 1); a
// wait's timeout or job end becomes its exit code.
func call(name string, args any) (tools.Result, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return tools.Result{}, err
	}
	ctx, stop := signalCtx()
	defer stop()
	r, err := tools.Exec(ctx, tools.CLI, name, raw)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return r, ExitError{Code: 1}
	}
	if _, err := io.WriteString(stdout, r.Text); err != nil {
		return r, err
	}
	if r.Code != 0 {
		return r, ExitError{Code: r.Code}
	}
	return r, nil
}

// ownerOpt scopes a command to one owner: job names resolve among its jobs, and new jobs get it. In T3 it
// defaults to the thread id, so each agent thread has its own namespace.
type ownerOpt struct {
	Owner string `default:"${owner}" placeholder:"O" help:"Job owner: names resolve among its jobs, new jobs get it (default $T3CODE_THREAD_ID, else $TAIL_MODE_OWNER; '' = shared namespace)."`
}

func (o ownerOpt) caller() tools.Caller { return tools.Caller{Owner: o.Owner} }

// cmdOpts are the cmd source's options, shared by start and run.
type cmdOpts struct {
	Dir   string            `placeholder:"D" help:"With a command: working dir (default: the current dir; remote: home). Quote '~/x' so it is the target's home."`
	Env   map[string]string `placeholder:"K=V" mapsep:"none" help:"With a command: export K=V before it (repeatable). PYTHONUNBUFFERED=1 is set unless given."`
	Shell string            `placeholder:"S" help:"With a command: POSIX shell to run it with, name or absolute path (default ${shell})."`
	Login bool              `help:"With a command: run it in a login shell (-lc), so profile files set PATH."`
}

func (o cmdOpts) set(in *tools.StartIn) {
	in.Dir, in.Env, in.Shell, in.Login = o.Dir, o.Env, o.Shell, o.Login
}

type startCmd struct {
	Name     string `placeholder:"N" help:"Job name (reference it instead of the id)."`
	Host     string `placeholder:"H" help:"SSH host to run on (default local)."`
	Restart  *bool  `negatable:"" help:"Restart when the source ends (default: file yes, cmd no)."`
	File     string `placeholder:"PATH" help:"File to tail; sugar for --source file. Quote '~/x' so it is the target's home." xor:"source" required:""`
	Backlog  *int   `placeholder:"N" help:"With --file: lines of history to read first (default ${backlog})."`
	Cmd      string `placeholder:"CMD" help:"Command to run; sugar for --source cmd." xor:"source" required:""`
	Source   string `placeholder:"KIND" help:"Source kind." xor:"source" required:""`
	Config   string `placeholder:"JSON" help:"With --source: adapter config as JSON."`
	cmdOpts  `embed:""`
	ownerOpt `embed:""`
}

func (c *startCmd) Run() error { return runTool(c) }

func (c *startCmd) tool() (string, any) {
	in := tools.StartIn{Caller: c.caller(), File: c.File, Cmd: c.Cmd, Source: c.Source, Host: c.Host, Name: c.Name,
		Backlog: c.Backlog, Restart: c.Restart}
	if c.Config != "" {
		in.Config = c.Config // JSON text: the tool parses it
	}
	c.set(&in)
	return "tail_start", in
}

type runCmd struct {
	Name     string `placeholder:"N" help:"Job name (reference it instead of the id)."`
	Host     string `placeholder:"H" help:"Run over ssh on this host (default local)."`
	cmdOpts  `embed:""`
	ownerOpt `embed:""`
	Every    string        `placeholder:"${every}" help:"Notify with digests at most this often, from the job's first line (min ${minEvery})."`
	Match    string        `placeholder:"RE" help:"Notify only on lines matching this regex (default every ${every})."`
	Until    string        `placeholder:"RE" help:"Block until a line matches (exit 0; 3 on timeout; 4 if the job ended). The job keeps running."`
	Timeout  time.Duration `default:"30m" help:"With --until: give up after this long (exit 3)."`
	Argv     []string      `arg:"" passthrough:"" placeholder:"CMD" help:"The command, after --. One argument is used verbatim as a shell command; several are shell-quoted and joined."`
}

// cmd is the command in Argv: one argument verbatim (so `run -- 'a | b'` works), several shell-quoted
// and joined.
func (c *runCmd) cmd() string {
	argv := c.Argv
	if len(argv) > 0 && argv[0] == "--" {
		argv = argv[1:]
	}
	if len(argv) == 1 {
		return argv[0]
	}
	return shellescape.QuoteCommand(argv)
}

func (c *runCmd) tool() (string, any) {
	in := tools.StartIn{Caller: c.caller(), Cmd: c.cmd(), Host: c.Host, Name: c.Name, NotifyEvery: c.Every, NotifyMatch: c.Match}
	c.set(&in)
	return "tail_start", in
}

// wait is tail_wait on the started job from cursor 0: the job is new, and jobs.Start returns after the
// worker claimed it, so a fast "ready" line may already be stored.
func (c *runCmd) wait(job string) tools.WaitIn {
	after := int64(0)
	return tools.WaitIn{Caller: c.caller(), Job: job, Until: c.Until, Timeout: c.Timeout.String(), After: &after}
}

func (c *runCmd) Run() error { // checks the wait first, so a bad flag leaves no job
	if _, err := regexp.Compile(c.Until); err != nil {
		return fmt.Errorf("--until: %w", err)
	}
	if c.Until != "" && c.Timeout <= 0 {
		return fmt.Errorf("--timeout must be positive (got %s)", c.Timeout)
	}
	if strings.TrimSpace(c.cmd()) == "" {
		return errors.New("run needs a command after --")
	}
	r, err := call(c.tool())
	if err != nil || c.Until == "" {
		return err
	}
	_, err = call("tail_wait", c.wait(r.Job))
	return err
}

type stopCmd struct {
	Job      string `arg:"" help:"Job id or name."`
	Force    bool   `help:"Stop it even if it is another owner's job."`
	ownerOpt `embed:""`
}

func (c *stopCmd) Run() error { return runTool(c) }

func (c *stopCmd) tool() (string, any) {
	return "tail_stop", tools.StopIn{Caller: c.caller(), Job: c.Job, Force: c.Force}
}

type jobsCmd struct {
	All      bool `help:"Every owner's jobs, not just --owner's."`
	ownerOpt `embed:""`
}

func (c *jobsCmd) Run() error { return runTool(c) }

func (c *jobsCmd) tool() (string, any) {
	return "tail_jobs", tools.JobsIn{Caller: c.caller(), All: c.All}
}

type readCmd struct {
	Job      string `arg:"" help:"Job id or name."`
	After    int64  `help:"Only lines with id > ID (exclusive cursor)." placeholder:"ID"`
	Tail     int    `help:"Newest N lines." placeholder:"N"`
	Grep     string `help:"Only lines matching this regex." placeholder:"RE"`
	Limit    int    `help:"Max lines (default ${readLimit})." placeholder:"N"`
	ownerOpt `embed:""`
}

func (c *readCmd) Run() error { return runTool(c) }

func (c *readCmd) tool() (string, any) {
	return "tail_read", tools.ReadIn{Caller: c.caller(), Job: c.Job, After: c.After, Tail: c.Tail, Grep: c.Grep, Limit: c.Limit}
}

type searchCmd struct {
	Query    []string `arg:"" help:"FTS5 query (words are joined with spaces)."`
	Job      string   `help:"Only this job (id, or name among --owner's jobs)."`
	All      bool     `help:"Every owner's jobs, not just --owner's."`
	Since    string   `help:"Only lines newer than this, e.g. 1h." placeholder:"D"`
	Limit    int      `help:"Max lines (default ${searchLimit})." placeholder:"N"`
	ownerOpt `embed:""`
}

func (c *searchCmd) Run() error { return runTool(c) }

func (c *searchCmd) tool() (string, any) {
	return "tail_search", tools.SearchIn{Caller: c.caller(), Query: strings.Join(c.Query, " "), Job: c.Job, All: c.All, Since: c.Since, Limit: c.Limit}
}

type sqlCmd struct {
	Query []string `arg:"" help:"One read-only SELECT (words are joined with spaces)."`
	Limit int      `help:"Max rows (default ${sqlLimit})." placeholder:"N"`
}

func (c *sqlCmd) Run() error { return runTool(c) }

func (c *sqlCmd) tool() (string, any) {
	return "tail_sql", tools.SQLIn{Query: strings.Join(c.Query, " "), Limit: c.Limit}
}

type waitCmd struct {
	Job      string        `arg:"" help:"Job id or name."`
	Until    string        `required:"" help:"Regex to wait for." placeholder:"RE"`
	Timeout  time.Duration `default:"30m" help:"Give up after this long (exit 3)."`
	After    int64         `default:"-1" help:"Start cursor (-1 = the job's current end)." placeholder:"ID"`
	ownerOpt `embed:""`
}

func (c *waitCmd) Run() error { return runTool(c) }

func (c *waitCmd) tool() (string, any) {
	return "tail_wait", tools.WaitIn{Caller: c.caller(), Job: c.Job, Until: c.Until, Timeout: c.Timeout.String(), After: &c.After}
}

type notifyCmd struct {
	Job      string `arg:"" help:"Job id or name."`
	Every    string `placeholder:"${every}" help:"Minimum time between digests (min ${minEvery})."`
	Match    string `help:"Only notify on lines matching this regex." placeholder:"RE"`
	After    *int64 `help:"Start cursor (default: the job's current end; 0 = its first line)." placeholder:"ID"`
	ownerOpt `embed:""`
}

func (c *notifyCmd) Run() error { return runTool(c) }

func (c *notifyCmd) tool() (string, any) {
	return "tail_notify", tools.NotifyIn{Caller: c.caller(), Job: c.Job, Every: c.Every, Match: c.Match, After: c.After}
}

// ---- commands with no tool

type followCmd struct {
	Job      string        `arg:"" help:"Job id or name."`
	Every    time.Duration `default:"${every}" help:"Minimum time between digests (min ${minEvery})."`
	Match    string        `help:"Only digest lines matching this regex." placeholder:"RE"`
	MaxLines int           `default:"${maxLines}" help:"Lines per digest."`
	After    int64         `default:"-1" help:"Start cursor (-1 = the job's current end)." placeholder:"ID"`
	Deliver  string        `default:"stdout" help:"Sink: stdout | jsonl | t3:THREAD."`
	Notifier bool          `hidden:"" help:"Internal: register as the one notifier for this job and --deliver (set by tail_notify)."`
	ownerOpt `embed:""`
}

func (c *followCmd) Run() error {
	sink, err := deliver.New(c.Deliver, tools.More)
	if err != nil {
		return err
	}
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	j, err := s.GetJob(c.Job, c.Owner)
	if err != nil {
		return err
	}
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
	enc := json.NewEncoder(stdout)
	if c.List {
		return enc.Encode(tools.List())
	}
	args := []byte(c.Args)
	if c.Args == "-" {
		var err error
		if args, err = io.ReadAll(os.Stdin); err != nil {
			return err
		}
	}
	client := tools.MCP
	if c.Client == "pi" {
		client = tools.Pi
	}
	ctx, stop := signalCtx()
	defer stop()
	r, err := tools.Exec(ctx, client, c.Name, args)
	switch {
	case err != nil:
		fmt.Fprintln(stdout, err)
		return ExitError{Code: 1}
	case client.Pi:
		return enc.Encode(r)
	}
	_, err = io.WriteString(stdout, r.Text)
	return err
}

func signalCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
