package store

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Live reports whether a status means the job has not ended ("lost" counts as ended).
func Live(status string) bool { return status == "running" || status == "starting" }

// StatusText renders a job status with what an agent needs next: the exit code of an exited job
// ("exited 3"), the error of a failed one ("failed: <err>"), else the bare status.
func StatusText(status string, exitCode int, errMsg string) string {
	switch status {
	case "exited":
		return "exited " + strconv.Itoa(exitCode)
	case "failed":
		if errMsg != "" {
			return "failed: " + OneLine(errMsg)
		}
	}
	return status
}

// StatusText is StatusText for j's status, exit code and error.
func (j Job) StatusText() string { return StatusText(j.Status, j.ExitCode, j.Error) }

// Label is how a job is referred to in output: "<id> (<name>)", or just the id.
func (j Job) Label() string {
	if j.Name != "" {
		return j.ID + " (" + j.Name + ")"
	}
	return j.ID
}

var oneLine = strings.NewReplacer("\r\n", " ⏎ ", "\n", " ⏎ ", "\r", " ", "\t", " ")

// OneLine collapses a multi-line value (a multi-line cmd, an error) onto one line for tables and
// headers; a newline becomes " ⏎ ".
func OneLine(s string) string { return strings.TrimSpace(oneLine.Replace(s)) }

// Clip cuts s to at most n bytes on a rune boundary and appends "…" if anything was cut.
func Clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n < 0 {
		n = 0
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}

// Clock renders a unix-ms time as local HH:MM:SS.
func Clock(ms int64) string { return time.UnixMilli(ms).Format("15:04:05") }

// Ago renders how long ago a unix-ms time was ("5s ago"), or never when ms is 0.
func Ago(ms int64, never string) string {
	if ms == 0 {
		return never
	}
	return time.Since(time.UnixMilli(ms)).Round(time.Second).String() + " ago"
}

// Stats fills j's line stats (Lines, LastID, LastTS), as ListJobs does.
func (s *Store) Stats(j *Job) error {
	// shortcut: COUNT(*) scans the job's index range; fine to ~10M lines, upgrade with a counter column.
	if err := s.db.QueryRow(`SELECT COUNT(*), COALESCE(MAX(id),0) FROM lines WHERE job=?`, j.ID).Scan(&j.Lines, &j.LastID); err != nil {
		return err
	}
	j.LastTS = 0
	if j.LastID > 0 {
		return s.db.QueryRow(`SELECT ts FROM lines WHERE id=?`, j.LastID).Scan(&j.LastTS)
	}
	return nil
}
