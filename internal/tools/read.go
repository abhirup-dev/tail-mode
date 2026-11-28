package tools

import (
	"context"
	"fmt"
	"regexp"
	"time"

	toon "github.com/toon-format/toon-go"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

// More is the digest's "read on" call (deliver.More).
func More(job string, after int64) string {
	return fmt.Sprintf("tail_read job=%s after=%d", job, after)
}

type ReadIn struct {
	Caller
	Job   string `json:"job" jsonschema:"job id (any job), or the name of one of your jobs"`
	After int64  `json:"after,omitempty" jsonschema:"exclusive cursor: returns lines with id > after; pass the cursor from the last result to get only new lines"`
	Tail  int    `json:"tail,omitempty" jsonschema:"only the newest N lines"`
	Grep  string `json:"grep,omitempty" jsonschema:"regex filter"`
	Limit int    `json:"limit,omitempty" jsonschema:"max lines (default ${readLimit})"`
}

// jobHeader is the aggregate header of a job: status with exit code, line count, last output; and the
// hint an exited job may have (exit 127).
func (h *handler) jobHeader(j store.Job) []toon.Field {
	_ = h.s.Stats(&j)
	j, _ = jobs.Refresh(h.s, j) // fresh effective status, exit code and error; keeps the stats
	out := []toon.Field{f("job", fmt.Sprintf("%s · %s · %d lines total · last output %s",
		j.Label(), store.Clip(j.StatusText(), 300), j.Lines, store.Ago(j.LastTS, "never")))}
	if j.Status == "exited" && j.Error != "" {
		out = append(out, f("hint", store.Clip(j.Error, 300)))
	}
	return out
}

func (h *handler) read(_ context.Context, in ReadIn) (string, error) {
	j, err := h.job(in.Job, h.owner(in.Caller))
	if err != nil {
		return "", err
	}
	if in.Limit <= 0 {
		in.Limit = store.ReadLimit
	}
	q := store.ReadQuery{Job: j.ID, After: in.After, Tail: in.Tail, Limit: in.Limit}
	if in.Grep != "" {
		if q.Grep, err = regexp.Compile(in.Grep); err != nil {
			return "", fail("fix the regex (Go RE2 syntax)", "grep: %v", err)
		}
	}
	ls, err := h.s.Read(q)
	if err != nil {
		return "", err
	}
	head := deliver.DataNote + "\n" + kv(h.jobHeader(j)...)
	return render(head, lineTable("lines", ls, false), func(shown int) string {
		if shown == 0 {
			what := "new lines"
			if in.Grep != "" {
				what = "new lines matching the grep"
			}
			out := kv(f("lines", fmt.Sprintf("0 %s after cursor %d", what, in.After)), f("cursor", in.After))
			if store.Live(jobs.Status(h.s, j)) {
				out += helpBlock(fmt.Sprintf("tail_wait job=%s until=<regex> after=%d blocks until a matching line arrives", j.ID, in.After))
			}
			return out
		}
		cur := ls[shown-1].ID
		fields := []toon.Field{f("cursor", cur)}
		switch {
		case shown < len(ls):
			fields = append(fields, f("more", fmt.Sprintf("%d lines not shown (output budget); continue with after=%d", len(ls)-shown, cur)))
		case len(ls) == in.Limit:
			fields = append(fields, f("more", fmt.Sprintf("limit %d reached; continue with after=%d", in.Limit, cur)))
		}
		return kv(fields...)
	}), nil
}

type SearchIn struct {
	Caller
	Query string `json:"query" jsonschema:"FTS5 query: words, \"exact phrase\", prefix*, a OR b, a NOT b"`
	Job   string `json:"job,omitempty" jsonschema:"only this job: id (any job) or the name of one of your jobs"`
	All   bool   `json:"all,omitempty" jsonschema:"search every owner's jobs, not just yours"`
	Since string `json:"since,omitempty" jsonschema:"only lines newer than this duration, e.g. 1h"`
	Limit int    `json:"limit,omitempty" jsonschema:"max lines (default ${searchLimit})"`
}

func (h *handler) search(_ context.Context, in SearchIn) (string, error) {
	owner := h.owner(in.Caller)
	q := store.SearchQuery{Query: in.Query, Limit: in.Limit, Owner: owner, All: in.All}
	scope := "the jobs of " + h.scope(owner)
	if in.All {
		scope = "all owners' jobs"
	}
	if in.Job != "" {
		j, err := h.job(in.Job, owner)
		if err != nil {
			return "", err
		}
		q.Job, scope = j.ID, "job "+j.Label()
	}
	if in.Since != "" {
		d, err := time.ParseDuration(in.Since)
		if err != nil {
			return "", fail(`use a Go duration such as "1h" or "30m"`, "since %q: %v", in.Since, err)
		}
		q.SinceTS = store.NowMS() - d.Milliseconds()
		scope += " in the last " + d.String()
	}
	ls, literal, err := h.s.Search(q)
	if err != nil {
		return "", err
	}
	note := ""
	if literal != nil {
		note = kv(f("note", fmt.Sprintf("query had FTS syntax error (%v); searched it as a literal phrase", literal)))
	}
	if len(ls) == 0 {
		help := []string{"tail_read job=<id> grep=<regex> scans one job by regex instead"}
		if !in.All && in.Job == "" {
			help = append(help, "all=true searches every owner's jobs")
		}
		return kv(f("lines", fmt.Sprintf("0 matches for '%s' in %s", in.Query, scope))) + note + helpBlock(help...), nil
	}
	head := deliver.DataNote + "\n" + kv(f("count", fmt.Sprintf("%s for '%s' in %s · newest first", plural(len(ls), "match", "matches"), in.Query, scope))) + note
	return render(head, lineTable("lines", ls, true), func(shown int) string {
		if shown < len(ls) {
			return kv(f("more", fmt.Sprintf("%d of %d lines shown, down to id %d (output budget); narrow with job, since, limit or the query",
				shown, len(ls), ls[shown-1].ID)))
		}
		return ""
	}), nil
}

// SQLIn takes Caller (ignored) so a client that passes owner on every call never trips the strict schema.
type SQLIn struct {
	Caller
	Query string `json:"query" jsonschema:"the SELECT"`
	Limit int    `json:"limit,omitempty" jsonschema:"max rows (default ${sqlLimit})"`
}

func (h *handler) sql(ctx context.Context, in SQLIn) (string, error) {
	if in.Limit <= 0 {
		in.Limit = store.QueryLimit
	}
	cols, rows, err := h.s.Query(ctx, in.Query, in.Limit)
	if err != nil {
		return "", fail("one read-only SELECT over jobs, lines, notifiers; e.g. SELECT id,status,exit_code FROM jobs WHERE owner='<owner>'", "sql: %v", err)
	}
	if len(rows) == 0 {
		return deliver.DataNote + "\n" + kv(f("rows", "0 rows"), f("columns", cols)), nil
	}
	// TOON rows are objects: make duplicate column names (SELECT a.id, b.id) unique.
	seen := map[string]int{}
	for i, c := range cols {
		if seen[c]++; seen[c] > 1 {
			cols[i] = fmt.Sprintf("%s_%d", c, seen[c])
		}
	}
	objs := make([]toon.Object, len(rows))
	for i, r := range rows {
		fs := make([]toon.Field, len(r))
		for k, v := range r { // store.Query yields only nil, int64, float64 and string
			fs[k] = f(cols[k], v)
		}
		objs[i] = toon.NewObject(fs...)
	}
	return render(deliver.DataNote+"\n", newTable("rows", objs, len(objs)), func(shown int) string {
		c := fmt.Sprintf("%d rows", len(rows))
		if shown < len(rows) {
			c = fmt.Sprintf("%d of %d rows shown (output budget); narrow the query", shown, len(rows))
		}
		if len(rows) == in.Limit {
			c += fmt.Sprintf("; row limit %d reached", in.Limit)
		}
		return kv(f("count", c))
	}), nil
}
