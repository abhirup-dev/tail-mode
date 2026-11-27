// Package follow turns a job's growing lines into digests (follow) or a single match (wait).
package follow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

type Options struct {
	Every    time.Duration  // min 5s
	Match    *regexp.Regexp // nil = all lines
	MaxLines int            // default 10
	After    int64          // -1 = job's current max id
}

// Test knobs.
var (
	minEvery  = 5 * time.Second
	waitEvery = time.Second
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

// liveStatus refreshes job with its effective status (jobs.Status) and reports whether it has not ended.
// "lost" counts as ended.
func liveStatus(s *store.Store, job *store.Job) (running bool, err error) {
	j, err := s.GetJob(job.ID)
	if err != nil {
		return false, err
	}
	j.Status = jobs.Status(s, j)
	*job = j
	return j.Status == "running" || j.Status == "starting", nil
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
func Follow(ctx context.Context, s *store.Store, job store.Job, o Options, sink deliver.Sink) error {
	if o.Every < minEvery {
		o.Every = minEvery
	}
	if o.MaxLines <= 0 {
		o.MaxLines = 10
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
				if o.Match != nil {
					if !o.Match.MatchString(l.Text) {
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
		if newN > 0 && (o.Match == nil || matched > 0) || !running {
			d := deliver.Digest{Job: job.ID, Name: job.Name, Host: job.Target, Status: job.Status,
				New: newN, Matched: matched, From: from, To: cursor, Final: !running, Lines: keep}
			if o.Match != nil {
				d.Match = o.Match.String()
			}
			if !running {
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
	return deliver.Line{ID: l.ID, TS: l.TS, Text: deliver.Clip(l.Text, maxText)}
}

// Wait polls every second until a line after `after` matches until (exit 0, ≤20 lines), the timeout
// passes (3), or the job ends without a match (4). timeout 0 = none.
func Wait(ctx context.Context, s *store.Store, job store.Job, until *regexp.Regexp, timeout time.Duration, after int64) ([]store.Line, int, error) {
	cursor, err := startCursor(s, job.ID, after)
	if err != nil {
		return nil, 0, err
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
			return nil, 0, err
		}
		max, err := s.MaxID(job.ID)
		if err != nil {
			return nil, 0, err
		}
		lines, err := s.Read(store.ReadQuery{Job: job.ID, After: cursor, Grep: until, Limit: 20})
		if err != nil {
			return nil, 0, err
		}
		if len(lines) > 0 {
			return lines, 0, nil
		}
		cursor = max
		if !running {
			return nil, 4, nil
		}
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return nil, 3, nil
			}
			return nil, 0, ctx.Err()
		case <-t.C:
		}
	}
}

// NotifierOpts configure a detached follower. Zero Every/MaxLines use follow's defaults; After 0 means
// from the job's first line, -1 its current end. Deliver is a deliver.New spec and is required.
type NotifierOpts struct {
	Every    time.Duration
	Match    string
	MaxLines int
	After    int64
	Deliver  string
}

// StartNotifier detaches a `tail-mode follow` process for job and returns its pid.
func StartNotifier(job string, o NotifierOpts) (int, error) {
	if o.Deliver == "" {
		return 0, errors.New("notifier: no deliver spec")
	}
	if _, err := deliver.New(o.Deliver); err != nil {
		return 0, err
	}
	if _, err := regexp.Compile(o.Match); err != nil {
		return 0, err
	}
	args := []string{"follow", "--deliver=" + o.Deliver, "--after=" + strconv.FormatInt(o.After, 10)}
	if o.Every > 0 {
		args = append(args, "--every="+o.Every.String())
	}
	if o.MaxLines > 0 {
		args = append(args, "--max-lines="+strconv.Itoa(o.MaxLines))
	}
	if o.Match != "" {
		args = append(args, "--match="+o.Match)
	}
	return proc.Detach(append(args, "--", job)...)
}
