package tools

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"al.essio.dev/pkg/shellescape"
	toon "github.com/toon-format/toon-go"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

type NotifyIn struct {
	Caller
	Job   string `json:"job" jsonschema:"job id, or the name of one of your jobs"`
	Every string `json:"every,omitempty" jsonschema:"${everyDoc}"`
	Match string `json:"match,omitempty" jsonschema:"regex: only digest matching lines"`
	After *int64 `json:"after,omitempty" jsonschema:"exclusive cursor: digest lines with id > after (default: lines from now on; 0 = from the job's first line)"`
}

func (h *handler) notifyTool(ctx context.Context, in NotifyIn) (string, error) {
	j, err := h.job(in.Job, h.owner(in.Caller))
	if err != nil {
		return "", err
	}
	after, from := int64(-1), "now"
	if in.After != nil {
		after, from = *in.After, fmt.Sprintf("after cursor %d", *in.After)
	}
	n, err := notifyOpts("every", in.Every, in.Match, after)
	if err != nil {
		return "", err
	}
	if !h.pushes() {
		return kv(f("notifications", "no T3 thread detected so nothing was started (this host has no push)")) +
			helpBlock(append([]string{fmt.Sprintf("tail_wait job=%s until=<regex> blocks until a line matches", j.ID)},
				recipeLines(j.ID, n)...)...), nil
	}
	if err := h.notify(ctx, j.ID, n); err != nil {
		return "", err
	}
	return kv(f("job", j.Label()), f("notifications", "on "+h.destination()), f("notify", notifyDesc(n, from))) +
		helpBlock(endTurn(j.ID)...), nil
}

// notifyDesc describes a started notifier with its effective settings.
func notifyDesc(n follow.Options, from string) string {
	s := "every " + n.Every.String()
	if n.Match != "" {
		s += " · only lines matching /" + n.Match + "/"
	}
	return s + " · from " + from
}

// endTurn is the next step once digests reach the caller. Weak agents otherwise sleep or block in a
// wait loop while the digests queue behind their turn, or re-arm the notifier after every digest.
func endTurn(id string) []string {
	return []string{
		"Notifications on. End your turn now: do not sleep, poll, or block waiting; each digest arrives here as a new message " +
			"and starts your next turn. The notifier keeps running until the job ends — do not call tail_notify again. " +
			"The last digest says FINISHED and how the job ended.",
		fmt.Sprintf("When a digest arrives: tail_read job=%s after=<cursor> reads more lines; tail_stop job=%s stops it", id, id),
	}
}

// recipeLines tell a host without push (Claude Code) how to get woken on job: arm its Monitor tool on
// `follow`, or run `wait` in the background. n.After < 0 means "from now on" (flag omitted).
func recipeLines(job string, n follow.Options) []string {
	aft, m := "", ""
	if n.After >= 0 {
		aft = fmt.Sprintf(" --after %d", n.After)
	}
	if n.Match != "" {
		m = " --match " + shellescape.Quote(n.Match)
	}
	return []string{
		fmt.Sprintf("Arm the Monitor tool with: tail-mode follow %s --every %s%s%s", job, n.Every, m, aft),
		fmt.Sprintf("or run in background: tail-mode wait %s --until RE --timeout 30m%s (exits 0 on match)", job, aft),
	}
}

// notifyOpts parses and validates notifier params (param: the interval's name in this tool) into
// Options with the effective defaults filled in. after is the follower's start cursor (-1 = now).
func notifyOpts(param, every, match string, after int64) (follow.Options, error) {
	n := follow.Options{Match: match, After: after}
	if every != "" {
		d, err := time.ParseDuration(every)
		if err != nil {
			return n, fail(`use a Go duration such as "30s" or "2m"`, "%s %q: %v", param, every, err)
		}
		n.Every = d
	}
	if err := n.Validate(); err != nil {
		return n, fail(fmt.Sprintf(`pass %s="%s" or longer, and match as a Go RE2 regex`, param, follow.MinEvery), "%v", err)
	}
	return n, nil
}

// notify starts (or replaces) the job's digests to the caller: a T3 notifier, or the follower a Pi client runs.
func (h *handler) notify(ctx context.Context, id string, n follow.Options) error {
	if h.c.Pi {
		n.Deliver = "jsonl"
		result(ctx).Follow = &Follower{Job: id, Argv: n.FollowArgs(id)}
		return nil
	}
	n.Deliver = "t3:" + h.thread
	_, err := startNotifier(h.s, id, n)
	return err
}

type WaitIn struct {
	Caller
	Job     string `json:"job" jsonschema:"job id, or the name of one of your jobs"`
	Until   string `json:"until" jsonschema:"regex to wait for, e.g. ready|listening on"`
	Timeout string `json:"timeout,omitempty" jsonschema:"how long to block (default ${wait}, max ${maxWait})"`
	After   *int64 `json:"after,omitempty" jsonschema:"exclusive cursor: only lines with id > after count (default 0 = from the job's first line; pass the returned cursor to wait for newer lines)"`
}

func (h *handler) wait(ctx context.Context, in WaitIn) (string, error) {
	j, err := h.job(in.Job, h.owner(in.Caller))
	if err != nil {
		return "", err
	}
	const waitHelp = `tail_wait job=<id> until="ready|listening"`
	if in.Until == "" {
		return "", fail(waitHelp, "until is empty: pass the regex to wait for")
	}
	until, err := regexp.Compile(in.Until)
	if err != nil {
		return "", fail(waitHelp, "until is not a valid regex (Go RE2 syntax): %v", err)
	}
	timeout := waitDefault
	if in.Timeout != "" {
		if timeout, err = time.ParseDuration(in.Timeout); err != nil || timeout <= 0 {
			return "", fail(`use a Go duration such as "50s" or "2m"`, "timeout %q is not a positive duration", in.Timeout)
		}
	}
	if limit := h.c.MaxWait; limit > 0 && timeout > limit {
		return "", fail(fmt.Sprintf("use timeout ≤ %s and call again, or tail_notify job=%s match=<regex>, which wakes you without blocking", limit, j.ID),
			"timeout %s is over the %s maximum", timeout, limit)
	}
	after := int64(0)
	if in.After != nil {
		after = *in.After
	}
	ls, code, cursor, err := follow.WaitCursor(ctx, h.s, j, until, timeout, after)
	if err != nil {
		return "", err
	}
	result(ctx).Code = code
	re := "/" + in.Until + "/"
	switch code {
	case 0:
		head := deliver.DataNote + "\n" + kv(f("wait", fmt.Sprintf("matched %s in job %s", re, j.Label())))
		for { // the match is the last line: over the budget, drop context from the oldest
			shown := 0
			out := render(head, lineTable("lines", ls, false), func(n int) string {
				shown = n
				return kv(f("cursor", cursor), f("next", fmt.Sprintf("pass after=%d to read or wait for newer lines only", cursor)))
			})
			if shown == len(ls) || len(ls) == 1 {
				return out, nil
			}
			ls = ls[1:]
		}
	case follow.WaitTimeout:
		last, _ := h.s.Read(store.ReadQuery{Job: j.ID, Tail: 3, Limit: 3})
		head := deliver.DataNote + "\n" + kv(append([]toon.Field{f("wait", fmt.Sprintf("timed out after %s; no line matched %s", timeout, re))}, h.jobHeader(j)...)...)
		return render(head, lineTable("last_lines", last, false), func(int) string {
			return kv(f("cursor", cursor)) + helpBlock(
				fmt.Sprintf("tail_wait job=%s until=%q after=%d keeps waiting", j.ID, in.Until, cursor),
				fmt.Sprintf("tail_notify job=%s match=%q wakes you when it appears (better for long waits)", j.ID, in.Until))
		}), nil
	default: // follow.WaitEnded: the job ended without a match
		last, _ := h.s.Read(store.ReadQuery{Job: j.ID, Tail: 5, Limit: 5})
		head := deliver.DataNote + "\n" + kv(append([]toon.Field{f("wait", fmt.Sprintf("job ended without a line matching %s", re))}, h.jobHeader(j)...)...)
		return render(head, lineTable("last_lines", last, false), func(int) string {
			return kv(f("cursor", cursor))
		}), nil
	}
}
