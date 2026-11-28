// Package follow turns a job's growing lines into digests (follow) or a single match (wait).
package follow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

const (
	DefaultEvery    = 30 * time.Second
	DefaultMaxLines = 10
	WaitTimeout     = 3 // WaitCursor codes (and CLI exit codes); a match is 0
	WaitEnded       = 4
)

// Test knobs.
var (
	minEvery  = 5 * time.Second
	waitEvery = time.Second
	// registerWait is how long StartNotifier waits for the new follower to take over (its lock retry is 2 s).
	registerWait = 4 * time.Second
	stopWait     = 3 * time.Second // SIGTERM → SIGKILL for a replaced follower
	// finalRetry paces the final digest's retries: 2 s, 5 s, 10 s.
	finalRetry = func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(2*time.Second), backoff.WithMultiplier(2.5),
			backoff.WithMaxInterval(10*time.Second), backoff.WithRandomizationFactor(0),
			backoff.WithMaxElapsedTime(0)), 3)
	}
)

const (
	pageSize = 10000
	maxText  = 300
)

// liveStatus refreshes job with its effective status (jobs.Refresh) and reports whether it has not ended.
// "lost" counts as ended.
func liveStatus(s *store.Store, job *store.Job) (running bool, err error) {
	j, err := jobs.Refresh(s, *job)
	if err != nil {
		return false, err
	}
	*job = j
	return store.Live(j.Status), nil
}

func startCursor(s *store.Store, job string, after int64) (int64, error) {
	if after >= 0 {
		return after, nil
	}
	return s.MaxID(job)
}

// Follow polls job every o.Every and delivers a digest per tick with news; on job end it delivers a
// final digest (retried, see finalRetry) and returns nil. Returns ctx.Err() on cancel. A failed
// non-final delivery keeps the accumulated news for the next tick.
// o is validated first (Validate); o.Deliver is ignored (the caller built sink).
func Follow(ctx context.Context, s *store.Store, job store.Job, o Options, sink deliver.Sink) error {
	if err := o.Validate(); err != nil {
		return err
	}
	var match *regexp.Regexp
	if o.Match != "" {
		match = regexp.MustCompile(o.Match)
	}
	cursor, err := startCursor(s, job.ID, o.After)
	if err != nil {
		return err
	}
	from := cursor
	var newN, matched int
	var keep []deliver.Line // last MaxLines candidates since from

	t := time.NewTicker(o.Every)
	defer t.Stop()
	for {
		running, err := liveStatus(s, &job) // check before reading so lines written before the end are seen
		if err != nil {
			return err
		}
		max, err := s.MaxID(job.ID)
		if err != nil {
			return err
		}
		for cursor < max { // page so memory stays bounded
			lines, err := s.Read(store.ReadQuery{Job: job.ID, After: cursor, Limit: pageSize})
			if err != nil {
				return err
			}
			if len(lines) == 0 {
				break
			}
			for _, l := range lines {
				if l.ID > max {
					break
				}
				cursor = l.ID
				newN++
				if match != nil {
					if !match.MatchString(l.Text) {
						continue
					}
					matched++
				}
				keep = append(keep, toLine(l))
				if len(keep) > o.MaxLines {
					keep = keep[1:]
				}
			}
		}
		if newN > 0 && (match == nil || matched > 0) || !running {
			d := deliver.Digest{Job: job.ID, Name: job.Name, Host: job.Target, Status: job.Status,
				New: newN, Matched: matched, From: from, To: cursor, Final: !running, Lines: keep, Match: o.Match}
			if !running {
				if job.Status == "exited" {
					code := job.ExitCode
					d.ExitCode = &code
				}
				d.Error = job.Error
				finalSummary(s, job, match, &d)
				return deliverFinal(ctx, sink, d)
			}
			if err := sink.Deliver(ctx, d); err != nil {
				fmt.Fprintln(os.Stderr, "tail-mode follow: deliver:", err) // keep going; next tick retries with new news
			} else {
				from, newN, matched, keep = cursor, 0, 0, nil
			}
		}
		if !running {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// finalSummary adds the job's total line count and last line (matching or not) to a final digest.
// Best effort: a store error leaves them unset rather than losing the final digest.
func finalSummary(s *store.Store, job store.Job, match *regexp.Regexp, d *deliver.Digest) {
	if err := s.Stats(&job); err == nil {
		d.Total = job.Lines
	}
	if ls, err := s.Read(store.ReadQuery{Job: job.ID, Tail: 1, Limit: 1}); err == nil && len(ls) == 1 {
		l := toLine(ls[0])
		d.LastLine = &l
		d.LastMatched = match == nil || match.MatchString(ls[0].Text)
	}
}

// deliverFinal delivers the last digest, retrying with backoff; it is the agent's only notice that the
// job ended, so a transient failure must not lose it.
func deliverFinal(ctx context.Context, sink deliver.Sink, d deliver.Digest) error {
	var last error
	err := backoff.RetryNotify(func() error {
		last = sink.Deliver(ctx, d)
		return last
	}, backoff.WithContext(finalRetry(), ctx), func(err error, next time.Duration) {
		fmt.Fprintf(os.Stderr, "tail-mode follow: final deliver: %v (retry in %v)\n", err, next)
	})
	if err != nil && last != nil {
		return fmt.Errorf("final digest: %w", last)
	}
	return err
}

func toLine(l store.Line) deliver.Line {
	return deliver.Line{ID: l.ID, TS: l.TS, Text: store.Clip(l.Text, maxText)}
}

// WaitCursor polls every second until a line after `after` matches until (code 0, ≤20 lines), the
// timeout passes (WaitTimeout), or the job ends without a match (WaitEnded). timeout 0 = none. It also
// returns the cursor reached: the last matching line on a match, else the last line checked, so
// waiting again from it misses nothing.
func WaitCursor(ctx context.Context, s *store.Store, job store.Job, until *regexp.Regexp, timeout time.Duration, after int64) ([]store.Line, int, int64, error) {
	cursor, err := startCursor(s, job.ID, after)
	if err != nil {
		return nil, 0, cursor, err
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	t := time.NewTicker(waitEvery)
	defer t.Stop()
	for {
		running, err := liveStatus(s, &job)
		if err != nil {
			return nil, 0, cursor, err
		}
		max, err := s.MaxID(job.ID)
		if err != nil {
			return nil, 0, cursor, err
		}
		lines, err := s.Read(store.ReadQuery{Job: job.ID, After: cursor, Grep: until, Limit: 20})
		if err != nil {
			return nil, 0, cursor, err
		}
		if len(lines) > 0 {
			return lines, 0, lines[len(lines)-1].ID, nil
		}
		if max > cursor {
			cursor = max
		}
		if !running {
			return nil, WaitEnded, cursor, nil
		}
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return nil, WaitTimeout, cursor, nil
			}
			return nil, 0, cursor, ctx.Err()
		case <-t.C:
		}
	}
}

// Options configure a follower. Zero Every/MaxLines mean the defaults; After 0 means from the job's
// first line, -1 its current end. Deliver is a deliver.New spec, required by StartNotifier.
type Options struct {
	Every    time.Duration
	Match    string
	MaxLines int
	After    int64
	Deliver  string
}

// Validate fills in the defaults (so callers report effective values), then rejects an interval below
// 5s, a negative MaxLines and a bad Match regex. It is the only check of these rules.
func (o *Options) Validate() error {
	if o.Every == 0 {
		o.Every = DefaultEvery
	}
	if o.MaxLines == 0 {
		o.MaxLines = DefaultMaxLines
	}
	if o.Every < minEvery {
		return fmt.Errorf("every must be ≥ %s (got %s)", minEvery, o.Every)
	}
	if o.MaxLines < 0 {
		return fmt.Errorf("max lines must be positive (got %d)", o.MaxLines)
	}
	if _, err := regexp.Compile(o.Match); err != nil {
		return fmt.Errorf("match: %w", err)
	}
	return nil
}

// NotifierLock is the liveness lock of the follower for (job, deliver): <dir>/run/n-<job>-<hash>.lock.
// The deliver spec is hashed since it holds colons and thread ids.
func NotifierLock(s *store.Store, job, deliver string) string {
	h := sha256.Sum256([]byte(deliver))
	return filepath.Join(s.Dir, "run", "n-"+job+"-"+hex.EncodeToString(h[:6])+".lock")
}

// Register makes the calling process the one notifier for (job, deliver): it takes the notifier lock
// (failing if another live follower keeps holding it) and records its pid. release deletes the row
// (if it is still ours) and unlocks. Only followers started by StartNotifier register; plain
// `tail-mode follow` (Monitor recipes, Pi) is per-session and never deduplicated.
func Register(s *store.Store, job, deliver string) (release func(), err error) {
	unlock, err := proc.Lock(NotifierLock(s, job, deliver))
	if err != nil {
		return nil, fmt.Errorf("another notifier for %s → %s is running: %w", job, deliver, err)
	}
	pid := os.Getpid()
	if err := s.SetNotifier(job, deliver, pid); err != nil {
		unlock()
		return nil, err
	}
	return func() {
		_ = s.DeleteNotifier(job, deliver, pid)
		unlock()
	}, nil
}

// stopNotifier ends the live follower for (job, deliver), if any. Like a job worker, it is signalled
// only while it holds its lock, so the recorded pid is still ours.
func stopNotifier(s *store.Store, job, deliver string) error {
	n, ok, err := s.GetNotifier(job, deliver)
	if err != nil || !ok {
		return err
	}
	// shortcut: a successor that has locked but not yet written its row leaves a stale pid for a few
	// ms; signalling it is harmless unless that pid was recycled in between.
	if !proc.Terminate(n.PID, NotifierLock(s, job, deliver), false, stopWait) {
		return fmt.Errorf("previous notifier pid %d for %s → %s did not stop", n.PID, job, deliver)
	}
	return nil
}

// StartNotifier makes a detached `tail-mode follow` process the one notifier for (job, o.Deliver):
// a live follower for the same job and destination is stopped first (followers for other
// destinations are left alone). It returns once the new follower has registered, with its pid.
func StartNotifier(s *store.Store, job string, o Options) (int, error) {
	if o.Deliver == "" {
		return 0, errors.New("notifier: no deliver spec")
	}
	if _, err := deliver.New(o.Deliver); err != nil {
		return 0, err
	}
	if err := o.Validate(); err != nil {
		return 0, err
	}
	if err := stopNotifier(s, job, o.Deliver); err != nil {
		return 0, err
	}
	pid, err := proc.Detach(s.Dir, slices.Insert(o.FollowArgs(job), 1, "--notifier")...)
	if err != nil {
		return 0, err
	}
	for end := time.Now().Add(registerWait); ; time.Sleep(25 * time.Millisecond) {
		n, ok, err := s.GetNotifier(job, o.Deliver)
		if err != nil {
			return pid, err
		}
		if ok && n.PID == pid {
			return pid, nil
		}
		if time.Now().After(end) {
			return pid, fmt.Errorf("notifier pid %d did not start (another notifier for %s → %s may have taken over); see %s",
				pid, job, o.Deliver, filepath.Join(s.Dir, "proc.log"))
		}
	}
}

// FollowArgs is the `tail-mode follow` argv (without the program) for job with validated o.
func (o Options) FollowArgs(job string) []string {
	args := []string{"follow", "--deliver=" + o.Deliver, "--after=" + strconv.FormatInt(o.After, 10),
		"--every=" + o.Every.String(), "--max-lines=" + strconv.Itoa(o.MaxLines)}
	if o.Match != "" {
		args = append(args, "--match="+o.Match)
	}
	return append(args, "--", job)
}
