// Package deliver is layer 4: how a digest reaches one kind of agent host. Sinks know nothing about sources.
package deliver

import "context"

// Digest is one batch of news about a job, produced by internal/follow.
type Digest struct {
	Job     string `json:"job"`
	Name    string `json:"name,omitempty"`
	Host    string `json:"host,omitempty"`
	Status  string `json:"status"`
	New     int    `json:"new"`               // new lines since the previous digest
	Matched int    `json:"matched,omitempty"` // of those, lines matching Match
	Match   string `json:"match,omitempty"`   // the --match regex, if any
	From    int64  `json:"from"`              // cursor before (exclusive)
	To      int64  `json:"to"`                // cursor after (inclusive)
	Final   bool   `json:"final"`             // job stopped running; last digest
	// Set on the final digest: how the job ended. ExitCode is set when Status is "exited".
	ExitCode *int   `json:"exit_code,omitempty"`
	Error    string `json:"error,omitempty"` // failed jobs: why; exited jobs: a hint (exit 127)
	// Set on the final digest: the job's total line count and its last line (whether or not it matched
	// Match; LastMatched says which), so the end is visible even with a filter.
	Total       int64  `json:"total_lines,omitempty"`
	LastLine    *Line  `json:"last_line,omitempty"`
	LastMatched bool   `json:"last_matched,omitempty"`
	Lines       []Line `json:"lines"`
}

type Line struct {
	ID   int64  `json:"id"`
	TS   int64  `json:"ts"` // unix ms
	Text string `json:"text"`
}

// Sink delivers digests. Implementations: stdout (text), jsonl, t3.
type Sink interface {
	Deliver(ctx context.Context, d Digest) error
}
