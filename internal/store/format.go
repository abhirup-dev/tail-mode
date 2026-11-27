package store

import (
	"strconv"
	"strings"
)

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

var oneLine = strings.NewReplacer("\r\n", " ⏎ ", "\n", " ⏎ ", "\r", " ", "\t", " ")

// OneLine collapses a multi-line value (a multi-line cmd, an error) onto one line for tables and
// headers; a newline becomes " ⏎ ".
func OneLine(s string) string { return strings.TrimSpace(oneLine.Replace(s)) }

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
