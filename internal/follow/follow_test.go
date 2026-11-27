package follow

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/abhirup-dev/tail-mode/internal/deliver"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/store"
)

// fakeSink records delivered digests; fail(d) decides whether a delivery attempt fails.
type fakeSink struct {
	mu    sync.Mutex
	ds    []deliver.Digest
	tries int
	fail  func(d deliver.Digest, try int) bool
}

func (f *fakeSink) Deliver(_ context.Context, d deliver.Digest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tries++
	if f.fail != nil && f.fail(d, f.tries) {
		return errors.New("sink down")
	}
	f.ds = append(f.ds, d)
	return nil
}

func (f *fakeSink) get() []deliver.Digest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deliver.Digest(nil), f.ds...)
}

// setup creates a running job; live=true holds its lock (as its worker would), so it is not lost.
func setup(t *testing.T, live bool) (*store.Store, store.Job) {
	t.Helper()
	t.Setenv("TAIL_MODE_HOME", t.TempDir())
	minEvery, waitEvery = 20*time.Millisecond, 20*time.Millisecond
	s, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	j := store.Job{ID: store.NewJobID(), Name: "web", Kind: "cmd", Transport: "local", Spec: "{}",
		Status: "starting", StartedAt: store.NowMS()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if live {
		release, err := proc.Lock(jobs.LockPath(s, j.ID))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(release)
	}
	if ok, err := s.Claim(j.ID, os.Getpid()); !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	j, _ = s.GetJob(j.ID)
	return s, j
}

func fastFinalRetry(t *testing.T) {
	old := finalRetry
	finalRetry = func() backoff.BackOff { return backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), 3) }
	t.Cleanup(func() { finalRetry = old })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out")
}

func TestFollowMatchAndFinal(t *testing.T) {
	s, j := setup(t, true)
	s.Append(j.ID, []string{"old line"}, store.NowMS())
	sink := &fakeSink{}
	done := make(chan error, 1)
	go func() {
		done <- Follow(context.Background(), s, j, Options{Match: regexp.MustCompile("ERR"), MaxLines: 2, After: -1}, sink)
	}()
	time.Sleep(50 * time.Millisecond)
	s.Append(j.ID, []string{"ok", "ERR a", "ERR b", "ERR " + strings.Repeat("x", 400)}, store.NowMS())
	waitFor(t, func() bool { return len(sink.get()) == 1 })
	d := sink.get()[0]
	if d.New != 4 || d.Matched != 3 || len(d.Lines) != 2 || d.Lines[0].Text != "ERR b" || d.Final {
		t.Fatalf("bad digest %+v", d)
	}
	if len(d.Lines[1].Text) > 310 {
		t.Fatalf("not cut: %d", len(d.Lines[1].Text))
	}
	s.Append(j.ID, []string{"no match"}, store.NowMS()) // no digest for non-matching lines
	time.Sleep(80 * time.Millisecond)
	if n := len(sink.get()); n != 1 {
		t.Fatalf("want 1 digest, got %d", n)
	}
	s.Finish(j.ID, "exited", 0, "", false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ds := sink.get()
	f := ds[len(ds)-1]
	if !f.Final || f.Status != "exited" || f.New != 1 || f.From != d.To {
		t.Fatalf("bad final %+v", f)
	}
}

func TestFollowCancel(t *testing.T) {
	s, j := setup(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	sink := &fakeSink{}
	go func() { done <- Follow(ctx, s, j, Options{After: -1}, sink) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("got %v", err)
	}
	if len(sink.get()) != 0 {
		t.Fatal("empty ticks must not deliver")
	}
}

func TestWait(t *testing.T) {
	s, j := setup(t, true)
	go func() {
		time.Sleep(50 * time.Millisecond)
		s.Append(j.ID, []string{"booting", "ready on :8080"}, store.NowMS())
	}()
	lines, code, err := Wait(context.Background(), s, j, regexp.MustCompile("ready"), time.Second, -1)
	if err != nil || code != 0 || len(lines) != 1 {
		t.Fatalf("match: %v %d %v", lines, code, err)
	}
	if _, code, _ = Wait(context.Background(), s, j, regexp.MustCompile("never"), 60*time.Millisecond, -1); code != 3 {
		t.Fatalf("timeout code %d", code)
	}
	s.Finish(j.ID, "exited", 0, "", false)
	if _, code, _ = Wait(context.Background(), s, j, regexp.MustCompile("never"), time.Second, 0); code != 4 {
		t.Fatalf("ended code %d", code)
	}
}

func TestWaitLostJob(t *testing.T) {
	s, j := setup(t, false) // running but nobody holds the lock
	if _, code, _ := Wait(context.Background(), s, j, regexp.MustCompile("x"), time.Second, -1); code != 4 {
		t.Fatalf("lost code %d", code)
	}
}

func TestFollowNonFinalFailureKeepsNews(t *testing.T) {
	s, j := setup(t, true)
	sink := &fakeSink{fail: func(d deliver.Digest, try int) bool { return try == 1 }}
	done := make(chan error, 1)
	go func() { done <- Follow(context.Background(), s, j, Options{After: 0}, sink) }()
	s.Append(j.ID, []string{"a", "b"}, store.NowMS())
	waitFor(t, func() bool { sink.mu.Lock(); defer sink.mu.Unlock(); return sink.tries >= 1 })
	s.Append(j.ID, []string{"c"}, store.NowMS())
	waitFor(t, func() bool { return len(sink.get()) == 1 })
	if d := sink.get()[0]; d.From != 0 || d.New < 2 {
		t.Fatalf("news lost after failed delivery: %+v", d)
	}
	s.Finish(j.ID, "exited", 0, "", false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFollowFinalDigestRetried(t *testing.T) {
	fastFinalRetry(t)
	s, j := setup(t, true)
	s.Finish(j.ID, "exited", 0, "", false)
	sink := &fakeSink{fail: func(d deliver.Digest, try int) bool { return try <= 2 }} // flaky: 2 failures
	if err := Follow(context.Background(), s, j, Options{After: 0}, sink); err != nil {
		t.Fatal(err)
	}
	if ds := sink.get(); len(ds) != 1 || !ds[0].Final || sink.tries != 3 {
		t.Fatalf("tries %d digests %+v", sink.tries, ds)
	}

	sink = &fakeSink{fail: func(deliver.Digest, int) bool { return true }}
	err := Follow(context.Background(), s, j, Options{After: 0}, sink)
	if err == nil || !strings.Contains(err.Error(), "sink down") || sink.tries != 4 {
		t.Fatalf("want error after 1+3 tries, got %v after %d", err, sink.tries)
	}
}

func TestFollowLostEnds(t *testing.T) {
	s, j := setup(t, false)
	sink := &fakeSink{}
	if err := Follow(context.Background(), s, j, Options{After: 0}, sink); err != nil {
		t.Fatal(err)
	}
	if ds := sink.get(); len(ds) != 1 || !ds[0].Final || ds[0].Status != "lost" {
		t.Fatalf("got %+v", ds)
	}
}
