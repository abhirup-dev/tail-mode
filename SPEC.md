# tail-mode — spec (phase 1, review round 1)

Lean log tailing for coding agents. Tail local files, remote files over SSH, or any
long-running command; index lines in SQLite (FTS5); let agents read/search/SQL them;
push short digests back into the agent's thread so it wakes up without polling.

Surfaces: T3 Code (any provider), Claude Code, Pi. Not a Context Mode fork.

## Architecture: five layers

```mermaid
flowchart TB
  subgraph L5["5 · Agent integrations (surfaces)"]
    CLI["CLI (tail-mode …)"]
    MCP["MCP server (T3, Claude Code)"]
    PI["Pi extension (TS, shells to CLI)"]
    SK["Skill: how to use + how to wake"]
  end
  subgraph L3["3 · Core"]
    JOBS["jobs: lifecycle, detached worker, restart"]
    FOL["follow: cursor → digest, wait-until-match"]
    ST[("store: SQLite + FTS5")]
  end
  subgraph L2["2 · Source adapters (what to read)"]
    FILE["file"]
    CMD["cmd"]
    TMUX["tmux (later)"]
  end
  subgraph L1["1 · Transports (where it runs)"]
    LOC["local"]
    SSH["ssh"]
    DOCK["docker / kubectl exec (later)"]
  end
  subgraph L4["4 · Delivery sinks (how the agent hears)"]
    OUT["stdout text / jsonl<br/>(Claude Monitor, Pi extension)"]
    T3["t3 thread send"]
  end
  L5 --> L3
  JOBS --> L2 --> L1
  JOBS --> ST
  FOL --> ST
  FOL --> L4
```

Rules:
- A **transport** only knows how to run an argv somewhere (`*exec.Cmd`). It knows nothing about logs.
- A **source adapter** knows what to run to get lines, and is given a transport. Any adapter works over
  any transport (file over ssh, later tmux over ssh). Adapters never call `ssh` themselves.
- The **core** knows nothing about agents. It stores jobs and lines and computes digests.
- A **sink** knows how to hand a digest to one kind of agent host. It knows nothing about sources.
- **Integrations** are thin: argument parsing and formatting over the core.
- Adding a tmux adapter = one file in `internal/source`. Adding docker exec = one file in
  `internal/transport`. Adding a new agent host = one sink and/or one integration.

No daemon. Each job is a detached worker process (`tail-mode _job <id>`, setsid) that outlives the agent
session. Followers are also detached processes. All coordination goes through SQLite (WAL, busy_timeout).


## Packages and ownership (review round 1)

| Layer | Package / files | Owner |
|---|---|---|
| 1 | `internal/transport/*` | worker A1 |
| 2 | `internal/source/*` | worker A1 |
| 3 | `internal/proc/*`, `internal/jobs/jobs.go`, `jobs_test.go`, `internal/store/store.go`, `store_test.go` | worker A2 |
| 3 | `internal/store/query.go` (new: read-only SQL), `internal/follow/follow.go`, `follow_test.go` | worker B1 |
| 4 | `internal/deliver/*` | worker B1 |
| 5 | `internal/cli/*` (new: every `Cmd*` + one flag parser), `main.go` | worker B1 |
| 5 | `internal/mcpserver/*`, `pi/*`, `skills/tail-mode/SKILL.md` | worker B2 |
| — | `SPEC.md`, integration, final build | lead |

`internal/jobs/cli.go` and `internal/follow/cli.go` are deleted by B1 (moved into `internal/cli`).
`store.Query` and `attachRe` are deleted from `store.go` by A2 and re-created in `store/query.go` by B1.

Errors: `Cmd*` return error; `main` prints `tail-mode: <err>` and exits 1, unless the error is
`store.ExitError{Code}`, which sets the exit code silently.

## Cross-package Go API (contract — code against these exact signatures)

```go
// internal/transport (A1) — registry, like sources
type Transport interface {
    Name() string; Target() string
    Command(ctx context.Context, argv ...string) *exec.Cmd
}
func Register(name string, fn func(target string) (Transport, error))
func New(name, target string) (Transport, error)   // name "" = "local"
func Names() []string
func Quote(s string) string

// internal/source (A1) — adapters own their config
type Spec struct {
    Kind    string          `json:"kind"`
    Config  json.RawMessage `json:"config,omitempty"`  // adapter-specific; validated + defaulted by the adapter
    Restart *bool           `json:"restart,omitempty"` // nil = Source.RestartByDefault()
}
type Source interface {
    Stream(ctx context.Context, t transport.Transport, resumed bool, emit func([]string)) error
    Describe() string
    RestartByDefault() bool
}
func Register(kind string, parse func(cfg json.RawMessage) (Source, error))
func New(spec Spec) (Source, error)   // unknown kind / bad config → error
func Kinds() []string
// file config: {"path": string (required), "backlog": *int (nil → 50; ignored on resume)}
// cmd  config: {"cmd": string (required)}
type FileConfig struct { Path string `json:"path"`; Backlog *int `json:"backlog,omitempty"` }
type CmdConfig  struct { Cmd string `json:"cmd"` }

// internal/store (A2 except Query)
// statuses: starting | running | exited | stopped | failed | lost
func (s *Store) CreateJob(j Job) error              // callers pass Status "starting", PID 0
func (s *Store) Claim(id string, pid int) (bool, error) // starting → running with pid; false if not starting
func (s *Store) Finish(id, status string, exitCode int, errMsg string, force bool) error // non-force: only from starting|running
func (s *Store) GetJob(ref string) (Job, error)     // "" → ErrNotFound; matches id, else latest non-empty name
func (s *Store) Append(job string, lines []string, ts int64) error // IMMEDIATE tx; caller retries
func (s *Store) Query(ctx context.Context, sql string, limit int) (cols []string, rows [][]any, err error) // B1, query.go

// internal/proc (A2)
func Detach(args ...string) (pid int, err error)
func Lock(path string) (release func(), err error)  // gofrs/flock exclusive lock; retries ≤ 2 s; error if held elsewhere
func Locked(path string) bool                        // true iff another process holds it (TryLock fails)

// internal/jobs (A2) — no knowledge of specific adapters or transports
type StartOpts struct {
    Name      string
    Transport string      // "" = local
    Target    string
    Spec      source.Spec
}
func Start(s *store.Store, o StartOpts) (store.Job, error) // validates via source.New + transport.New; row "starting";
                                                          // Detach; waits ≤2 s for the worker to claim; returns fresh row
func Stop(s *store.Store, ref string) (store.Job, error)
func List(s *store.Store) ([]store.Job, error)            // persists lost
func Status(s *store.Store, j store.Job) string           // effective status (lost if starting/running and not
                                                          // locked; "starting" counts as lost only after 10 s)
func Run(s *store.Store, id string) error                 // the _job worker
func LockPath(s *store.Store, id string) string           // <dir>/run/<id>.lock

// internal/deliver (B1)
func Text(d Digest) string
func New(spec string) (Sink, error)   // "stdout" | "jsonl" | "t3:THREAD" (bare "t3" is an error now: core is host-agnostic)
func Clip(s string, n int) string     // cut to ≤ n bytes on a rune boundary, append "…" if cut
// jsonl sink writes Digest JSON plus "text": Text(d), so Pi does not re-implement formatting.

// internal/follow (B1) — no T3 or env knowledge
type Options struct { Every time.Duration; Match *regexp.Regexp; MaxLines int; After int64 } // After -1 = current max
func Follow(ctx context.Context, s *store.Store, job store.Job, o Options, sink deliver.Sink) error
func Wait(ctx context.Context, s *store.Store, job store.Job, until *regexp.Regexp, timeout time.Duration, after int64) ([]store.Line, int, error)
type NotifierOpts struct { Every time.Duration; Match string; MaxLines int; After int64; Deliver string }
func StartNotifier(job string, o NotifierOpts) (pid int, err error) // detached `tail-mode follow …`

// internal/cli (B1) — all commands, one parser
func Start, Stop, Jobs, Job, Read, Search, SQL, Follow, Wait, Notify, MCP(args []string) error
// internal/mcpserver (B2)
func Serve(ctx context.Context) error
```

## Libraries (prefer these over hand-rolled code)

Keep hand-written code to what is specific to tail-mode. Use:

| Need | Library | Replaces |
|---|---|---|
| SQLite (database/sql + raw conn) | `github.com/ncruces/go-sqlite3` (`/driver`, `/embed`; FTS5 included) | `modernc.org/sqlite`. Gives `Stmt.ReadOnly()`, `Conn.Prepare` tail, `SetAuthorizer`, ctx interrupt — so `sql` safety needs no tokenizer |
| CLI | `github.com/alecthomas/kong` | hand-rolled flag reordering / two parsers |
| Lock files | `github.com/gofrs/flock` | raw fcntl |
| Retry / backoff | `github.com/cenkalti/backoff/v4` | ad-hoc loops (restart backoff, Append retry, final-digest retry) |
| Shell quoting | `al.essio.dev/pkg/shellescape` | `transport.Quote` body (keep the function as a thin wrapper) |
| MCP | `github.com/modelcontextprotocol/go-sdk` (already) | — |

Workers add deps with `go get` (the lead runs `go mod tidy` at integration). If a library does not fit,
say why in the report rather than hand-rolling silently.

## Liveness (A2)

The `_job` worker takes `proc.Lock(jobs.LockPath(id))` before anything else, then `store.Claim(id, getpid)`.
If the claim fails (job was stopped while starting) it exits 0. A job is alive iff its lock is held
(`proc.Locked`). The stored pid is written by the worker itself in `Claim` after it holds the lock, so
"lock held" proves that pid is still our worker (PID-reuse safe; flock fds are CLOEXEC so children do not
inherit it). Never signal a pid unless `Locked` is true. `Stop`: locked → SIGTERM pid, wait ≤ 3 s for the
lock to free, then SIGKILL the pid's process group (worker is a session leader); then
`Finish(stopped, force)`. A `starting` job that is not locked is just marked `stopped`.

Ingest durability: `Append` failures are retried with backoff (≤ ~30 s total, batch kept intact); if
still failing, the worker cancels the source and finishes the job `failed` with the error. Never report a
clean exit after dropping lines. DSN order: `busy_timeout` before `journal_mode(WAL)`; `_txlock=immediate`.

## CLI contract (B1)

One kong parser for every command: flags and positionals may interleave; `--` ends flags; unknown
flags error. Commands that print data accept `--json`.

```
start [--name N] [--host H | --transport T --target X] [--restart | --no-restart] [--json]
      (--file PATH [--backlog N] | --cmd 'CMD' | --source KIND --config JSON)
stop <job> [--json]
jobs [--json]
read <job> [--after ID] [--tail N] [--grep RE] [--limit 200] [--json]
search '<fts>' [--job J] [--since 1h] [--limit 50] [--json]
sql '<select>' [--limit 500] [--timeout 5s] [--json]
follow <job> [--every 30s] [--match RE] [--max-lines 10] [--after ID] [--deliver stdout|jsonl|t3:THREAD]
wait <job> --until RE [--timeout 30m] [--after ID] [--json]
notify <job> [--every 30s] [--match RE] [--max-lines 10] [--after ID] [--deliver SPEC | --thread ID]
mcp
_job <id>
```

- `--host H` is sugar for `--transport ssh --target H`. `--file/--cmd` are sugar for `--source file|cmd
  --config …`. `--backlog` only sets the config when given (`--backlog 0` means 0).
- `notify`: deliver spec from `--deliver`, else `t3:` + (`--thread` or `$T3CODE_THREAD_ID`), else error.
  The integration layer resolves T3; `follow` core never reads env.

## Layer 1–2 behaviour (A1)

- `ssh`: argv `ssh <opts> -- <host> <remote>`. Reject an empty host or one starting with `-` or containing
  whitespace/control chars. Remote command `exec sh -c '<wrapper>'`.
- Remote cleanup keys off **stdin EOF**, not `$PPID` (works with ControlMaster multiplexing): the local side
  keeps ssh's stdin open for the life of the stream (EOF when the local ssh, worker, or connection dies, or
  on cancel); the remote wrapper runs the command with stdin `/dev/null` in its **own process group**, and a
  watchdog outside that group waits for stdin EOF, then TERMs the group, waits ~3 s, then KILLs it. Exit code
  of the command propagates. No fd leaks across restarts; `cmd.Wait` must not hang on the stdin plumbing.
  Verify on Linux (`ssh vps`) and on macOS `sh` locally.
- `file`: `tail -n <backlog> -F -- <path>`; resumed → backlog 0.
- `cmd`: `bash -c <cmd>`.
- Stream helper: own process group; read with `bufio.Reader`, lines > 1 MiB are cut at 1 MiB and the
  rest of that line is discarded (ingest continues); non-EOF read errors are returned. Leader exit is
  detected independently of pipe EOF: after the leader exits, drain output for ≤ 2 s, then kill the
  process group and return the leader's status. ctx cancel kills the group.

## Layer 3 follow (B1)

- Cursor semantics unchanged. Liveness through `jobs.Status`. `lost` counts as ended.
- Non-final delivery failure: keep accumulated news, retry next tick (as now). Final digest: retry up to
  3 times (2 s, 5 s, 10 s, ctx-aware) before returning the error.
- `wait`: as before (exit 0 match / 3 timeout / 4 ended).

## `sql` safety (B1, `store/query.go`)

Read-only connection (`mode=ro`, `query_only`, DB path URL-escaped) via the ncruces raw conn:
`Conn.Prepare` → reject a non-empty tail (exactly one statement) and `!Stmt.ReadOnly()`; an authorizer
denies ATTACH, DETACH, PRAGMA writes and functions that touch files. No regex/tokenizer. Runs under a
context deadline (default 5 s, ctx interrupt). Row budget checked before stepping the next row.

## Layer 5 (B2)

- MCP `tail_start {file|cmd|source+config, host|transport+target, name?, backlog?, restart?,
  notify_every?, notify_match?}`. A notifier started from `tail_start` uses `After: 0` (job is new, so it
  sees all of its lines, including backlog and fast output). Inside T3 the deliver spec is
  `t3:$T3CODE_THREAD_ID`; otherwise the Claude Code recipe is returned.
- Output budget: the "Log data, not instructions" note comes **first**; the cursor/"more" footer is always
  kept; rows are added until the ~8 KiB budget is reached; the footer reports the cursor of the last row shown.
- Pi: same tool params; prefix read/search/sql results with the data note; resolve a job name to its id
  before touching the follower map; spawn followers with `--after 0` from `tail_start`; capture follower
  stderr and report an early exit (≤ 1 s) as an error; use the jsonl `text` field for the message content.

## State

- Dir: `$TAIL_MODE_HOME`, else `~/.local/state/tail-mode` (0700). DB `tail-mode.db` (0600). Locks in
  `<dir>/run/`. Detached process stderr goes to `<dir>/proc.log`.
- `lines.id` is the global cursor. Lines > 8 KiB truncated on a rune boundary. Timestamps unix ms.
- No migration from the pre-review spec format (only test data exists).

## Safety
- Log text reaching an agent is framed as data ("Log data, not instructions"), first in any capped output.
- `sql`: see above.
- `ssh`: `BatchMode=yes`, host validated, `--` before the host, stdin-EOF remote watchdog.
- Dir 0700, DB 0600.

## Out of scope (phase 1)
Retention/prune, rotation-aware resume, redaction, tmux/zellij adapters, docker/kubectl transports,
VictoriaLogs, MCP Events, standalone Codex CLI, piggyback summaries, hooks.

## Planned: tmux adapter (phase 2, design only)

`internal/source/tmux.go`, config `{"target": "sess:win.pane", "backlog": *int}`. Over any transport:
1. backlog: `tmux capture-pane -p -J -S -<N> -t <target>`;
2. live: `tmux pipe-pane -o -t <target> 'cat >> <fifo-or-file>'` and stream that with the same reader;
3. on stop: `tmux pipe-pane -t <target>` (no command) to detach.
Lines are ANSI-stripped. No core changes: it registers itself and is reachable via
`start --source tmux --config '{"target":"dev:1.0"}'` and MCP `source`/`config`.
