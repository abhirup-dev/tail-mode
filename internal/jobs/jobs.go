// Package jobs is the job lifecycle: start a detached worker, run the source loop, stop, list.
// It knows nothing about specific source adapters or transports.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/source"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/transport"
)

// Timings; variables so tests can shorten them.
var (
	claimWait     = 2 * time.Second  // Start waits this long for the worker to claim
	startingGrace = 10 * time.Second // an unlocked "starting" job is lost only after this
	stopWait      = 3 * time.Second  // SIGTERM → SIGKILL
	appendBudget  = 30 * time.Second // retry budget for one Append batch
	finishBudget  = 30 * time.Second // retry budget for the final status write
	restartMin    = time.Second
	restartMax    = 30 * time.Second
	healthyAfter  = 60 * time.Second // a run this long resets the restart backoff
)

type StartOpts struct {
	Name      string
	Transport string // "" = local
	Target    string
	Spec      source.Spec
}

// LockPath is the worker's liveness lock: <dir>/run/<id>.lock.
func LockPath(s *store.Store, id string) string { return filepath.Join(s.Dir, "run", id+".lock") }

// Start validates the source and transport, creates a "starting" row, launches the detached worker,
// and waits up to 2 s for it to claim the job. It returns the fresh row.
func Start(s *store.Store, o StartOpts) (store.Job, error) {
	src, err := source.New(o.Spec)
	if err != nil {
		return store.Job{}, err
	}
	t, err := transport.New(o.Transport, o.Target)
	if err != nil {
		return store.Job{}, err
	}
	spec, err := json.Marshal(o.Spec)
	if err != nil {
		return store.Job{}, err
	}
	desc := src.Describe()
	if t.Target() != "" {
		desc = t.Target() + ": " + desc
	}
	j := store.Job{ID: store.NewJobID(), Name: o.Name, Kind: o.Spec.Kind, Transport: t.Name(), Target: t.Target(),
		Spec: string(spec), Describe: desc, Status: "starting", StartedAt: store.NowMS()}
	if err := s.CreateJob(j); err != nil {
		return j, err
	}
	if _, err := proc.Detach("_job", j.ID); err != nil {
		_ = s.Finish(j.ID, "failed", 0, err.Error(), false)
		return j, err
	}
	for end := time.Now().Add(claimWait); ; time.Sleep(25 * time.Millisecond) {
		cur, err := s.GetJob(j.ID)
		if err != nil || cur.Status != "starting" || time.Now().After(end) {
			return cur, err
		}
	}
}

// Status is the effective status: a starting/running job whose worker does not hold its lock is lost
// ("starting" only after a grace period, since the worker may not have locked yet).
func Status(s *store.Store, j store.Job) string {
	if j.Status != "starting" && j.Status != "running" {
		return j.Status
	}
	if proc.Locked(LockPath(s, j.ID)) {
		return j.Status
	}
	// The worker finishes its row before releasing the lock, so a fresh read settles a stale j.
	if cur, err := s.GetJob(j.ID); err == nil && cur.Status != j.Status {
		return Status(s, cur)
	}
	if j.Status == "starting" && time.Since(time.UnixMilli(j.StartedAt)) < startingGrace {
		return "starting"
	}
	return "lost"
}

// List returns jobs newest first, persisting lost ones.
func List(s *store.Store) ([]store.Job, error) {
	js, err := s.ListJobs()
	if err != nil {
		return nil, err
	}
	for i, j := range js {
		st := Status(s, j)
		if st == j.Status {
			continue
		}
		if st == "lost" {
			_ = s.Finish(j.ID, "lost", 0, "", false)
		}
		if cur, err := s.GetJob(j.ID); err == nil {
			js[i].Status, js[i].EndedAt, js[i].ExitCode, js[i].Error = cur.Status, cur.EndedAt, cur.ExitCode, cur.Error
		}
	}
	return js, nil
}

// Stop stops a job. A starting job is marked stopped (its worker's claim then fails and it exits).
// A running job's worker is signalled only while it holds the lock (so the pid is ours):
// SIGTERM, wait ≤ 3 s, then SIGKILL its process group.
func Stop(s *store.Store, ref string) (store.Job, error) {
	j, err := s.GetJob(ref)
	if err != nil {
		return j, err
	}
	if j.Status == "starting" {
		ok, err := s.Abort(j.ID)
		if err != nil {
			return j, err
		}
		if ok {
			return s.GetJob(j.ID)
		}
		if j, err = s.GetJob(j.ID); err != nil { // claimed meanwhile
			return j, err
		}
	}
	if j.Status != "running" {
		return j, nil
	}
	lock := LockPath(s, j.ID)
	if j.PID > 0 && proc.Locked(lock) {
		_ = syscall.Kill(j.PID, syscall.SIGTERM)
		if !waitUnlocked(lock, stopWait) {
			// shortcut: the source child has its own process group and survives this until its next
			// write hits SIGPIPE; record the child's pgid if hung workers become common.
			_ = syscall.Kill(-j.PID, syscall.SIGKILL)
			waitUnlocked(lock, time.Second)
		}
	}
	// Non-force: keep a terminal status the worker wrote itself (stopped, or failed if it dropped lines).
	if err := s.Finish(j.ID, "stopped", 0, "", false); err != nil {
		return j, err
	}
	return s.GetJob(j.ID)
}

func waitUnlocked(lock string, d time.Duration) bool {
	for end := time.Now().Add(d); proc.Locked(lock); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			return false
		}
	}
	return true
}

// Run is the _job worker: lock, claim, then run the source loop. SIGTERM/SIGINT stop it.
// If the claim fails (stopped while starting) it exits cleanly without running.
func Run(s *store.Store, id string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	release, err := proc.Lock(LockPath(s, id))
	if err != nil {
		return err
	}
	defer release() // after the final Finish, so observers never see an unlocked live row
	ok, err := s.Claim(id, os.Getpid())
	if err != nil || !ok {
		return err
	}
	return run(ctx, s, id)
}

// retryWrite retries a store write with backoff for up to budget (or until ctx ends).
func retryWrite(ctx context.Context, budget time.Duration, what string, op func() error) error {
	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxInterval(5*time.Second),
		backoff.WithMaxElapsedTime(budget))
	return backoff.RetryNotify(op, backoff.WithContext(b, ctx), func(err error, d time.Duration) {
		fmt.Fprintf(os.Stderr, "tail-mode _job: %s failed, retrying in %v: %v\n", what, d.Round(time.Millisecond), err)
	})
}

func run(ctx context.Context, s *store.Store, id string) error {
	j, err := s.GetJob(id)
	if err != nil {
		return err
	}
	finish := func(status string, code int, msg string) error {
		// Not bound to ctx: the final status must be written even while stopping.
		return retryWrite(context.Background(), finishBudget, "finish", func() error { return s.Finish(j.ID, status, code, msg, false) })
	}
	fail := func(err error) error {
		return errors.Join(err, finish("failed", 0, err.Error()))
	}
	var spec source.Spec
	if err := json.Unmarshal([]byte(j.Spec), &spec); err != nil {
		return fail(err)
	}
	src, err := source.New(spec)
	if err != nil {
		return fail(err)
	}
	t, err := transport.New(j.Transport, j.Target)
	if err != nil {
		return fail(err)
	}
	restart := src.RestartByDefault()
	if spec.Restart != nil {
		restart = *spec.Restart
	}

	// A batch that cannot be stored after retries cancels the source and fails the job:
	// never a clean exit after dropping lines.
	srcCtx, cancelSrc := context.WithCancel(ctx)
	defer cancelSrc()
	var mu sync.Mutex
	var appendErr error
	emit := func(lines []string) {
		mu.Lock()
		defer mu.Unlock()
		if appendErr != nil || len(lines) == 0 {
			return
		}
		ts := store.NowMS()
		if err := retryWrite(ctx, appendBudget, "append", func() error { return s.Append(j.ID, lines, ts) }); err != nil {
			appendErr = fmt.Errorf("append failed, dropped %d line(s): %w", len(lines), err)
			cancelSrc()
		}
	}
	// ended reports how the job ends once srcCtx is done: failed on a dropped batch, else stopped.
	ended := func() (bool, error) {
		mu.Lock()
		aerr := appendErr
		mu.Unlock()
		if aerr != nil {
			return true, fail(aerr)
		}
		if ctx.Err() != nil {
			return true, finish("stopped", 0, "")
		}
		return false, nil
	}

	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(restartMin),
		backoff.WithMaxInterval(restartMax),
		backoff.WithRandomizationFactor(0),
		backoff.WithMaxElapsedTime(0)) // never give up restarting
	for attempt := 1; ; attempt++ {
		began := time.Now()
		err := src.Stream(srcCtx, t, attempt > 1, emit)
		if done, ret := ended(); done {
			return ret
		}
		if !restart {
			var ee *exec.ExitError
			switch {
			case err == nil:
				return finish("exited", 0, "")
			case errors.As(err, &ee):
				return finish("exited", ee.ExitCode(), "")
			default:
				return fail(err)
			}
		}
		if time.Since(began) >= healthyAfter {
			b.Reset()
		}
		reason := "source ended"
		if err != nil {
			reason = err.Error()
		}
		emit([]string{fmt.Sprintf("[tail-mode] restarted (attempt %d): %s", attempt, reason)})
		select {
		case <-srcCtx.Done():
			_, ret := ended()
			return ret
		case <-time.After(b.NextBackOff()):
		}
	}
}
