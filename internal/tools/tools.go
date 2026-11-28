// Package tools is the one implementation of the tail_* agent tools: names, descriptions, input schemas
// and handlers. MCP (Serve) and `tail-mode tool` (the Pi extension, the CLI) all run them.
//
// Identity comes from the process env, never from tool params: each agent session gets its own
// `tail-mode mcp` process (MCPorter's stdio bridge spawns one per distinct caller env), so
// $T3CODE_THREAD_ID is the caller's T3 thread: it owns the caller's jobs and receives its digests. Pi
// sets $TAIL_MODE_OWNER for its session instead. Agents never pass a thread id (weak models copy the
// wrong one); clients without either may pass owner.
//
// TAIL_MODE_SHARED=1 means only that the MCP server's cwd is not the caller's: local commands need an
// absolute dir and local files an absolute path.
//
// MCP clients may drop server instructions, resources and prompts, so all guidance lives in tool
// descriptions and tool results. Results are compact TOON (toonformat.dev) with contextual help.
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

const (
	maxOut      = 8 << 10 // output budget per tool result
	footerRoom  = 512     // reserved so the footer and help always fit
	waitDefault = 50 * time.Second
)

// Vars are the defaults and limits that tool and CLI help state as ${name} (kong's syntax); each value
// comes from the package that enforces it.
var Vars = map[string]string{
	"every":       follow.DefaultEvery.String(),
	"minEvery":    follow.MinEvery.String(),
	"maxLines":    strconv.Itoa(follow.DefaultMaxLines),
	"backlog":     strconv.Itoa(source.DefaultBacklog),
	"shell":       source.DefaultShell,
	"readLimit":   strconv.Itoa(store.ReadLimit),
	"searchLimit": strconv.Itoa(store.SearchLimit),
	"sqlLimit":    strconv.Itoa(store.QueryLimit),
	"sqlTimeout":  store.QueryTimeout.String(),
	"wait":        waitDefault.String(),
	"maxWait":     MCP.MaxWait.String(),
	"everyDoc": fmt.Sprintf("minimum time between digests (default %s, min %s); matches are sent as soon as that allows",
		follow.DefaultEvery, follow.MinEvery),
}

var vars = func() *strings.Replacer {
	var pairs []string
	for k, v := range Vars {
		pairs = append(pairs, "${"+k+"}", v)
	}
	return strings.NewReplacer(pairs...)
}()

// Seams for tests: starting real jobs or notifiers detaches processes (and a notifier wakes a T3 thread).
var (
	startJob      = jobs.Start
	startNotifier = follow.StartNotifier
)

func envThread() string { return strings.TrimSpace(os.Getenv("T3CODE_THREAD_ID")) }

// EnvOwner is the caller's identity from the env ("" = none): its T3 thread, else $TAIL_MODE_OWNER (set by
// the Pi extension per session). The one identity source for the CLI's --owner default, MCP and `tool`.
func EnvOwner() string {
	if t := envThread(); t != "" {
		return t
	}
	return strings.TrimSpace(os.Getenv("TAIL_MODE_OWNER"))
}

// Serve runs the MCP server on stdio. TAIL_MODE_SHARED (1, true, yes or on) applies here only: the other
// clients run in the caller's own cwd.
func Serve(ctx context.Context) error {
	s, err := store.Open("")
	if err != nil {
		return err
	}
	defer s.Close()
	h := newHandler(s, MCP)
	h.shared = slices.Contains([]string{"1", "true", "yes", "on"}, strings.ToLower(os.Getenv("TAIL_MODE_SHARED")))
	return newServer(h).Run(ctx, &mcp.StdioTransport{})
}

func newServer(h *handler) *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{Name: "tail-mode", Version: "0.2.0"}, nil)
	for _, t := range h.tools() {
		srv.AddTool(&mcp.Tool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema},
			func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				res := &mcp.CallToolResult{}
				if text, err := t.call(ctx, req.Params.Arguments); err != nil {
					res.SetError(err)
				} else {
					res.Content = []mcp.Content{&mcp.TextContent{Text: text}}
				}
				return res, nil
			})
	}
	return srv
}

// Result is what Exec returns. Follow is set only for a client that delivers digests itself (Pi) when a
// call turned notifications on: the client runs `tail-mode <Argv…>` and wakes on each jsonl line.
type Result struct {
	Text   string    `json:"text"`
	Follow *Follower `json:"follow,omitempty"`
	Job    string    `json:"-"` // the job tail_start started
	Code   int       `json:"-"` // tail_wait: 0 matched, follow.WaitTimeout, follow.WaitEnded (CLI exit status)
}

// Client is what legitimately differs between callers of the same tools.
type Client struct {
	Pi      bool          // runs digest followers itself: notifying hands back their argv (Result.Follow)
	CLI     bool          // the caller's own process: its owner param is authoritative
	MaxWait time.Duration // tail_wait timeout cap (0 = none): MCP clients time out long calls, a shell does not
}

var (
	MCP = Client{MaxWait: 5 * time.Minute}
	Pi  = Client{Pi: true, MaxWait: 5 * time.Minute}
	CLI = Client{CLI: true}
)

type resultKey struct{}

// result is the call's Result for what a tool reports beyond its text (MCP calls discard it).
func result(ctx context.Context) *Result {
	if r, ok := ctx.Value(resultKey{}).(*Result); ok {
		return r
	}
	return &Result{}
}

type Follower struct {
	Job  string   `json:"job"`
	Argv []string `json:"argv"`
}

// List is the tool set as `tail-mode tool` runs it (sorted by name); MCP advertises the same, with the
// shared-mode path rule when that is on.
func List() []Tool {
	ts := newHandler(nil, MCP).tools()
	slices.SortFunc(ts, func(a, b Tool) int { return strings.Compare(a.Name, b.Name) })
	return ts
}

// Exec runs one tool with JSON args as MCP would, for client c.
func Exec(ctx context.Context, c Client, name string, args []byte) (Result, error) {
	s, err := store.Open("")
	if err != nil {
		return Result{}, err
	}
	defer s.Close()
	return newHandler(s, c).exec(ctx, name, args)
}

func (h *handler) exec(ctx context.Context, name string, args []byte) (Result, error) {
	ts := h.tools()
	i := slices.IndexFunc(ts, func(t Tool) bool { return t.Name == name })
	if i < 0 {
		return Result{}, fmt.Errorf("unknown tool %q (tail-mode tool --list)", name)
	}
	r := &Result{}
	text, err := ts[i].call(context.WithValue(ctx, resultKey{}, r), args)
	r.Text = text
	return *r, err
}

// handler holds what every tool needs. shared: the process's cwd is not the caller's (Serve). thread:
// the caller's T3 thread, where digests go; self: its env identity (EnvOwner); "" = none.
type handler struct {
	s            *store.Store
	c            Client
	shared       bool
	thread, self string
}

func newHandler(s *store.Store, c Client) *handler {
	return &handler{s: s, c: c, thread: envThread(), self: EnvOwner()}
}

// Tool is one tool: what clients list, and call, which validates JSON args against InputSchema, runs the
// handler and returns its text (clipped to the budget) or an "error: …\nhelp: …" error.
type Tool struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	InputSchema *jsonschema.Schema `json:"inputSchema"`
	call        func(context.Context, json.RawMessage) (string, error)
}

// tool builds a Tool from In's schema; ${var}s in desc and the param descriptions are expanded (Vars).
func tool[In any](name, desc string, fn func(context.Context, In) (string, error)) Tool {
	schema, err := jsonschema.For[In](nil)
	if err != nil {
		panic(err)
	}
	for _, p := range schema.Properties {
		p.Description = vars.Replace(p.Description)
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		panic(err)
	}
	return Tool{Name: name, Description: vars.Replace(desc), InputSchema: schema, call: func(ctx context.Context, raw json.RawMessage) (string, error) {
		if len(raw) == 0 {
			raw = json.RawMessage("{}")
		}
		var args map[string]any
		var in In
		if err := json.Unmarshal(raw, &args); err != nil {
			return "", toolError{msg: "arguments: " + err.Error()}
		}
		if err := resolved.Validate(args); err != nil {
			return "", toolError{msg: fmt.Sprintf("validating \"arguments\": %v", err)}
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return "", toolError{msg: "arguments: " + err.Error()}
		}
		text, err := fn(ctx, in)
		var te toolError
		if err != nil && !errors.As(err, &te) {
			err = toolError{msg: err.Error()}
		}
		return store.Clip(text, maxOut), err // the clip is a safety net: render already fits the budget
	}}
}

func (h *handler) tools() []Tool {
	dirRule := "Local cmd: dir defaults to the server's current dir; pass an absolute dir (or ~/x) to be sure. Local file: an absolute path (or ~/x)."
	if h.shared {
		dirRule = "Local cmd: dir is required, an absolute path (or ~/x). Local file: an absolute path (or ~/x). Relative paths are refused."
	}
	return []Tool{
		tool("tail_start", "Run a long-running command (dev server, build, test watcher, remote script) or tail a log file in the "+
			"background, instead of your own background shell; its output is stored and searchable. Required: exactly one of cmd, file, "+
			"or source+config (sources: "+strings.Join(source.Kinds(), ", ")+"). "+dirRule+" "+
			"notify_every/notify_match start tail_notify's digests from the job's first line. "+
			`Example: {"name":"build","host":"vps","dir":"/srv/app","cmd":"./build.sh","env":{"MODE":"prod"},"notify_match":"ERROR|FAIL"}. `+
			"Next: with notifications on, end your turn (digests wake you); otherwise tail_wait for a short wait until ready, "+
			"tail_read for output, tail_stop when done.",
			h.start),
		tool("tail_wait", "Block until a job prints a line matching until (a regex), e.g. a dev server's \"ready|listening\" line. "+
			"For short waits (< ~1 min) only; with notifications on, end your turn instead. "+
			"Required: job, until. Returns the first matching line with up to 9 lines before it and a cursor; or, after timeout, says it timed out "+
			"and gives the cursor to wait again from; or, if the job ends first, its exit code and last lines. For longer waits prefer "+
			"tail_notify: it wakes you without blocking. "+deliver.DataNote,
			h.wait),
		tool("tail_read", "Read a job's output lines oldest first, with its status, exit code and line count. Required: job. "+deliver.DataNote,
			h.read),
		tool("tail_search", "Full-text search across your jobs' output, newest first. Required: query. "+deliver.DataNote,
			h.search),
		tool("tail_jobs", "List your jobs: status (with exit code once ended), line count, last output time.",
			h.jobsList),
		tool("tail_stop", "Stop a job and its processes. Required: job. Stopping a job that already ended is a no-op.",
			h.stop),
		tool("tail_notify", "Get woken with digests of a job's new lines, and a final digest with the exit code when it ends, "+
			"instead of polling. Required: job. In T3 and Pi digests arrive in your own session automatically (never pass a thread "+
			"id), each starting a new turn, so end your turn after calling it; elsewhere it returns how to get woken instead. "+
			"One notifier per job and destination: calling it replaces any earlier one for this job, including the one "+
			"tail_start set up with notify_every/notify_match. "+deliver.DataNote,
			h.notifyTool),
		tool("tail_sql", "Read-only SQL when read/search are not enough: one SELECT, ${sqlTimeout} limit. Tables: jobs(id,name,owner,status,"+
			"exit_code,error,started_at,ended_at,describe,...), lines(id,job,ts,text), notifiers(job,deliver,pid). Not scoped to you: "+
			"filter on jobs.owner (tail_jobs shows yours). "+deliver.DataNote,
			h.sql),
	}
}

// toolError is an error an agent can act on: what went wrong, and the call that fixes it.
type toolError struct{ msg, help string }

func (e toolError) Error() string {
	s := "error: " + e.msg
	if e.help != "" {
		s += "\nhelp: " + e.help
	}
	return s
}

func fail(help, format string, a ...any) error {
	return toolError{msg: fmt.Sprintf(format, a...), help: help}
}

// Caller is the optional identity a client without an env identity may give.
type Caller struct {
	Owner string `json:"owner,omitempty" jsonschema:"only for clients outside T3 and Pi: a stable label for yourself (e.g. your session id) that keeps your jobs yours. Ignored in T3 and Pi, where your session is detected automatically"`
}

// owner is the caller's job owner: its env identity, else the owner label, else "" (shared). A CLI's
// owner param is authoritative (its --owner defaults to EnvOwner; empty = shared).
func (h *handler) owner(c Caller) string {
	if h.self != "" && !h.c.CLI {
		return h.self
	}
	return strings.TrimSpace(c.Owner)
}

// selfName shows the caller's env identity as such: weak agents read a raw id as something to act on.
func (h *handler) selfName() string {
	switch {
	case h.thread != "":
		return "this T3 thread"
	case h.c.Pi:
		return "this Pi session"
	}
	return "this session"
}

// scope names whose jobs a call sees, for empty states and errors.
func (h *handler) scope(owner string) string {
	switch {
	case owner != "" && owner == h.self:
		return h.selfName()
	case owner != "":
		return "owner '" + owner + "'"
	}
	return "the shared namespace (no T3 thread detected; no owner given)"
}

// pushes reports whether digests can reach the caller: a T3 thread, or a client that delivers them itself.
func (h *handler) pushes() bool { return h.c.Pi || h.thread != "" }

// destination says where notifications go.
func (h *handler) destination() string {
	switch {
	case h.c.Pi:
		return "→ this Pi session (each digest arrives as a new message)"
	case h.thread != "":
		return "→ this T3 thread (detected from your session; you do not need to pass it)"
	}
	return "no T3 thread detected so nothing can be pushed to you; tail_wait blocks until a line matches"
}

const sharedWarning = "no T3 thread detected and no owner given so this job is in the shared namespace where other agents can see and stop it; pass owner (a stable label for yourself) to keep jobs yours"

// job resolves ref for owner with an actionable not-found error.
func (h *handler) job(ref, owner string) (store.Job, error) {
	j, err := h.s.GetJob(ref, owner)
	if errors.Is(err, store.ErrNotFound) {
		return j, fail("tail_jobs lists your jobs (all=true: every owner's); a job id works for any owner's job",
			"no job %q among the jobs of %s", ref, h.scope(owner))
	}
	return j, err
}
