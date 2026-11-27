// Package store is the core state: jobs and lines in one SQLite DB (WAL, FTS5).
// Every process (CLI, MCP server, job workers, followers) opens it independently.
package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ncruces/go-sqlite3"
	_ "github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/ext/fts5"
)

// FTS5 is an extension in ncruces/go-sqlite3; register it for every connection in this process
// (database/sql pool and raw read-only connections alike).
func init() { sqlite3.AutoExtension(fts5.Register) }

const MaxLineBytes = 8 << 10

// DBFile is the database file name inside the state dir.
const DBFile = "tail-mode.db"

// BusyTimeout is how long a connection waits for another writer's lock. Tests shorten it.
var BusyTimeout = 10 * time.Second

// ExitError makes main exit with Code without printing anything.
type ExitError struct{ Code int }

func (e ExitError) Error() string { return fmt.Sprintf("exit %d", e.Code) }

var ErrNotFound = errors.New("job not found")

type Job struct {
	ID        string `json:"id"`
	Name      string `json:"name,omitempty"`
	Owner     string `json:"owner,omitempty"`  // T3 thread id or free label; "" = shared namespace
	Kind      string `json:"kind"`             // source kind
	Transport string `json:"transport"`        // "local" | "ssh"
	Target    string `json:"target,omitempty"` // ssh host
	Spec      string `json:"spec"`             // JSON source.Spec
	Describe  string `json:"describe"`
	PID       int    `json:"pid,omitempty"`
	Status    string `json:"status"` // starting|running|exited|stopped|failed|lost
	StartedAt int64  `json:"started_at"`
	EndedAt   int64  `json:"ended_at,omitempty"`
	ExitCode  int    `json:"exit_code,omitempty"`
	Error     string `json:"error,omitempty"`
	// Filled by ListJobs.
	Lines  int64 `json:"lines"`
	LastID int64 `json:"last_id,omitempty"`
	LastTS int64 `json:"last_ts,omitempty"`
}

type Line struct {
	ID   int64  `json:"id"`
	Job  string `json:"job"`
	TS   int64  `json:"ts"`
	Text string `json:"text"`
}

type Store struct {
	db  *sql.DB
	Dir string
}

// Dir returns $TAIL_MODE_HOME or ~/.local/state/tail-mode.
func Dir() string {
	if d := os.Getenv("TAIL_MODE_HOME"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "state", "tail-mode")
}

const schema = `
CREATE TABLE IF NOT EXISTS jobs(
  id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', owner TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL, transport TEXT NOT NULL,
  target TEXT NOT NULL DEFAULT '', spec TEXT NOT NULL, describe TEXT NOT NULL DEFAULT '',
  pid INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, started_at INTEGER NOT NULL,
  ended_at INTEGER NOT NULL DEFAULT 0, exit_code INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS jobs_name ON jobs(name, started_at);
CREATE TABLE IF NOT EXISTS lines(id INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT NOT NULL, ts INTEGER NOT NULL, text TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS lines_job ON lines(job, id);
CREATE VIRTUAL TABLE IF NOT EXISTS lines_fts USING fts5(text, content='lines', content_rowid='id', tokenize='porter unicode61');
CREATE TRIGGER IF NOT EXISTS lines_ai AFTER INSERT ON lines BEGIN INSERT INTO lines_fts(rowid, text) VALUES (new.id, new.text); END;
CREATE TRIGGER IF NOT EXISTS lines_ad AFTER DELETE ON lines BEGIN INSERT INTO lines_fts(lines_fts, rowid, text) VALUES ('delete', old.id, old.text); END;
CREATE TABLE IF NOT EXISTS notifiers(job TEXT NOT NULL, deliver TEXT NOT NULL, pid INTEGER NOT NULL, started_at INTEGER NOT NULL,
  PRIMARY KEY(job, deliver));
`

// postMigrate runs after migrate, since it needs columns that old DBs only get there.
const postMigrate = `CREATE INDEX IF NOT EXISTS jobs_owner_name ON jobs(owner, name, started_at);`

// migrate brings a DB created by an older version up to date. Every process opens the DB, so the
// check and the ALTER run in one IMMEDIATE transaction: concurrent openers serialize, and the loser
// sees the column and does nothing.
func migrate(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name='owner'`).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		if _, err := tx.Exec(`ALTER TABLE jobs ADD COLUMN owner TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Open opens (creating if needed) the DB in dir ("" = Dir()).
func Open(dir string) (*Store, error) {
	if dir == "" {
		dir = Dir()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, DBFile)
	// busy_timeout must precede journal_mode: switching to WAL needs a lock another process may hold.
	dsn := (&url.URL{Scheme: "file", OmitHost: true, Path: path}).String() +
		fmt.Sprintf("?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate", BusyTimeout.Milliseconds())
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	if _, err := db.Exec(postMigrate); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	return &Store{db: db, Dir: dir}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Path is the database file path (for read-only connections opened elsewhere).
func (s *Store) Path() string { return filepath.Join(s.Dir, DBFile) }

func NowMS() int64 { return time.Now().UnixMilli() }

// NewJobID returns "j" + 6 hex chars.
func NewJobID() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return "j" + hex.EncodeToString(b)
}

// CreateJob inserts j. Callers pass Status "starting" and PID 0; the worker claims it.
func (s *Store) CreateJob(j Job) error {
	_, err := s.db.Exec(`INSERT INTO jobs(id,name,owner,kind,transport,target,spec,describe,pid,status,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.Name, j.Owner, j.Kind, j.Transport, j.Target, j.Spec, j.Describe, j.PID, j.Status, j.StartedAt)
	return err
}

// Claim moves a job from starting to running with the worker's pid. False if it was not starting.
func (s *Store) Claim(id string, pid int) (bool, error) {
	return s.transition(`UPDATE jobs SET status='running', pid=? WHERE id=? AND status='starting'`, pid, id)
}

// Abort moves a job from starting to stopped. False if it was not starting (e.g. already claimed).
// Exactly one of Claim and Abort wins.
func (s *Store) Abort(id string) (bool, error) {
	return s.transition(`UPDATE jobs SET status='stopped', ended_at=? WHERE id=? AND status='starting'`, NowMS(), id)
}

func (s *Store) transition(q string, args ...any) (bool, error) {
	res, err := s.db.Exec(q, args...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// Finish marks a job ended. Unless force, it only changes a starting or running job.
func (s *Store) Finish(id, status string, exitCode int, errMsg string, force bool) error {
	q := `UPDATE jobs SET status=?, exit_code=?, error=?, ended_at=? WHERE id=?`
	if !force {
		q += ` AND status IN ('starting','running')`
	}
	_, err := s.db.Exec(q, status, exitCode, errMsg, NowMS(), id)
	return err
}

const jobCols = `id,name,owner,kind,transport,target,spec,describe,pid,status,started_at,ended_at,exit_code,error`

func scanJob(sc interface{ Scan(...any) error }) (Job, error) {
	var j Job
	err := sc.Scan(&j.ID, &j.Name, &j.Owner, &j.Kind, &j.Transport, &j.Target, &j.Spec, &j.Describe, &j.PID, &j.Status, &j.StartedAt, &j.EndedAt, &j.ExitCode, &j.Error)
	return j, err
}

// GetJob finds a job by id (any owner), else the latest job with that (non-empty) name owned by owner.
// Names are scoped to their owner, so the same name under two owners never collides; callers that
// hold an id pass owner "".
func (s *Store) GetJob(ref, owner string) (Job, error) {
	if ref == "" {
		return Job{}, fmt.Errorf("%w: empty job reference", ErrNotFound)
	}
	j, err := scanJob(s.db.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE id=? OR (name<>'' AND name=? AND owner=?)
		ORDER BY id=? DESC, started_at DESC, rowid DESC LIMIT 1`, ref, ref, owner, ref))
	if errors.Is(err, sql.ErrNoRows) {
		if owner != "" {
			return j, fmt.Errorf("%w: %s (names are looked up among owner %q's jobs; use the job id for another owner's job)", ErrNotFound, ref, owner)
		}
		return j, fmt.Errorf("%w: %s", ErrNotFound, ref)
	}
	return j, err
}

// ListJobs returns owner's jobs (all owners' if all), newest first, with line stats. It does not check
// pid liveness (jobs.List does).
func (s *Store) ListJobs(owner string, all bool) ([]Job, error) {
	rows, err := s.db.Query(`SELECT `+jobCols+` FROM jobs WHERE ? OR owner=? ORDER BY started_at DESC`, all, owner)
	if err != nil {
		return nil, err
	}
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, j)
	}
	rows.Close()
	for i := range out {
		// shortcut: COUNT(*) scans the job's index range; fine to ~10M lines, upgrade with a counter column.
		_ = s.db.QueryRow(`SELECT COUNT(*), COALESCE(MAX(id),0) FROM lines WHERE job=?`, out[i].ID).Scan(&out[i].Lines, &out[i].LastID)
		if out[i].LastID > 0 {
			_ = s.db.QueryRow(`SELECT ts FROM lines WHERE id=?`, out[i].LastID).Scan(&out[i].LastTS)
		}
	}
	return out, rows.Err()
}

// Append stores lines for job with timestamp ts in one IMMEDIATE transaction: all or nothing,
// so a caller can retry the same batch.
func (s *Store) Append(job string, texts []string, ts int64) error {
	if len(texts) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO lines(job, ts, text) VALUES(?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	for _, t := range texts {
		t = truncate(t)
		if _, err := stmt.Exec(job, ts, t); err != nil {
			stmt.Close()
			tx.Rollback()
			return err
		}
	}
	stmt.Close()
	return tx.Commit()
}

// truncate cuts t to at most MaxLineBytes on a rune boundary and marks the cut.
func truncate(t string) string {
	if len(t) <= MaxLineBytes {
		return t
	}
	i := MaxLineBytes
	for i > 0 && !utf8.RuneStart(t[i]) {
		i--
	}
	return t[:i] + "…[truncated]"
}

// MaxID is the current cursor for job ("" = all jobs).
func (s *Store) MaxID(job string) (int64, error) {
	var id int64
	var err error
	if job == "" {
		err = s.db.QueryRow(`SELECT COALESCE(MAX(id),0) FROM lines`).Scan(&id)
	} else {
		err = s.db.QueryRow(`SELECT COALESCE(MAX(id),0) FROM lines WHERE job=?`, job).Scan(&id)
	}
	return id, err
}

type ReadQuery struct {
	Job   string         // required (job id)
	After int64          // exclusive cursor
	Tail  int            // >0: the newest Tail lines after After (returned oldest first)
	Grep  *regexp.Regexp // optional filter
	Limit int            // max lines returned (default 200)
}

// Read returns lines oldest first. With Grep, it scans forward until Limit matches.
func (s *Store) Read(q ReadQuery) ([]Line, error) {
	if q.Limit <= 0 {
		q.Limit = 200
	}
	var rows *sql.Rows
	var err error
	switch {
	case q.Tail > 0 && q.Grep == nil:
		rows, err = s.db.Query(`SELECT id,job,ts,text FROM (SELECT id,job,ts,text FROM lines WHERE job=? AND id>? ORDER BY id DESC LIMIT ?) ORDER BY id`, q.Job, q.After, min(q.Tail, q.Limit))
	case q.Tail > 0: // newest matches
		rows, err = s.db.Query(`SELECT id,job,ts,text FROM lines WHERE job=? AND id>? ORDER BY id DESC`, q.Job, q.After)
	default:
		rows, err = s.db.Query(`SELECT id,job,ts,text FROM lines WHERE job=? AND id>? ORDER BY id`, q.Job, q.After)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	want := q.Limit
	if q.Tail > 0 {
		want = min(q.Tail, q.Limit)
	}
	var out []Line
	for rows.Next() && len(out) < want {
		var l Line
		if err := rows.Scan(&l.ID, &l.Job, &l.TS, &l.Text); err != nil {
			return nil, err
		}
		if q.Grep != nil && !q.Grep.MatchString(l.Text) {
			continue
		}
		out = append(out, l)
	}
	if q.Tail > 0 && q.Grep != nil { // collected newest-first
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out, rows.Err()
}

type SearchQuery struct {
	Query   string // FTS5 query; falls back to a quoted phrase if the syntax is invalid
	Job     string // optional job id; when set, Owner/All are ignored
	Owner   string // only jobs of this owner, unless All
	All     bool
	SinceTS int64 // optional, unix ms
	Limit   int   // default 50
}

// Search returns FTS matches newest first.
func (s *Store) Search(q SearchQuery) ([]Line, error) {
	if q.Limit <= 0 {
		q.Limit = 50
	}
	run := func(match string) ([]Line, error) {
		rows, err := s.db.Query(`SELECT l.id,l.job,l.ts,l.text FROM lines_fts f JOIN lines l ON l.id=f.rowid
			WHERE lines_fts MATCH ? AND (?='' OR l.job=?) AND l.ts>=?
			  AND (?<>'' OR ? OR l.job IN (SELECT id FROM jobs WHERE owner=?)) ORDER BY l.id DESC LIMIT ?`,
			match, q.Job, q.Job, q.SinceTS, q.Job, q.All, q.Owner, q.Limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []Line
		for rows.Next() {
			var l Line
			if err := rows.Scan(&l.ID, &l.Job, &l.TS, &l.Text); err != nil {
				return nil, err
			}
			out = append(out, l)
		}
		return out, rows.Err()
	}
	out, err := run(q.Query)
	if err != nil { // most likely FTS5 syntax: retry as a literal phrase
		if out2, err2 := run(`"` + strings.ReplaceAll(q.Query, `"`, `""`) + `"`); err2 == nil {
			return out2, nil
		}
	}
	return out, err
}

// Notifier is a registered detached follower: one per (job, deliver), see follow.StartNotifier.
type Notifier struct {
	Job       string `json:"job"`
	Deliver   string `json:"deliver"`
	PID       int    `json:"pid"`
	StartedAt int64  `json:"started_at"`
}

// SetNotifier records pid as the follower for (job, deliver), replacing any earlier row.
func (s *Store) SetNotifier(job, deliver string, pid int) error {
	_, err := s.db.Exec(`INSERT INTO notifiers(job,deliver,pid,started_at) VALUES(?,?,?,?)
		ON CONFLICT(job,deliver) DO UPDATE SET pid=excluded.pid, started_at=excluded.started_at`, job, deliver, pid, NowMS())
	return err
}

// DeleteNotifier removes the (job, deliver) row if it still names pid (a successor may own it).
func (s *Store) DeleteNotifier(job, deliver string, pid int) error {
	_, err := s.db.Exec(`DELETE FROM notifiers WHERE job=? AND deliver=? AND pid=?`, job, deliver, pid)
	return err
}

// GetNotifier returns the (job, deliver) row; ok is false if there is none.
func (s *Store) GetNotifier(job, deliver string) (n Notifier, ok bool, err error) {
	err = s.db.QueryRow(`SELECT job,deliver,pid,started_at FROM notifiers WHERE job=? AND deliver=?`, job, deliver).
		Scan(&n.Job, &n.Deliver, &n.PID, &n.StartedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return n, false, nil
	}
	return n, err == nil, err
}

// ListNotifiers returns job's notifier rows ("" = all jobs). Rows of dead followers may linger until
// replaced; check liveness with the notifier lock.
func (s *Store) ListNotifiers(job string) ([]Notifier, error) {
	rows, err := s.db.Query(`SELECT job,deliver,pid,started_at FROM notifiers WHERE ?='' OR job=? ORDER BY job, deliver`, job, job)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notifier
	for rows.Next() {
		var n Notifier
		if err := rows.Scan(&n.Job, &n.Deliver, &n.PID, &n.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
