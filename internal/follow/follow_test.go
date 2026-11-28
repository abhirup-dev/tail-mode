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
	"github.com/abhirup-dev/tail-mode/internal/proc"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/testutil"
)

// fakeSink records delivered digests and when; fail(d) decides whether a delivery attempt fails.
type fakeSink struct {
	mu    sync.Mutex
	ds    []deliver.Digest
	at    []time.Time
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
	f.ds, f.at = append(f.ds, d), append(f.at, time.Now())
	return nil
}

func (f *fakeSink) get() ([]deliver.Digest, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deliver.Digest(nil), f.ds...), f.tries
}

func (f *fakeSink) n() int { ds, _ := f.get(); return len(ds) }

// setup creates a running job "web"; live holds its lock (as its worker would), else it is lost.
func setup(t *testing.T, live bool) (*store.Store, store.Job) {
	t.Helper()
	testutil.Swap(t, &MinEvery, 20*time.Millisecond)
	testutil.Swap(t, &pollEvery, 20*time.Millisecond)
	s := testutil.Store(t)
	if live {
		return s, testutil.LiveJob(t, s, store.Job{Name: "web"})
	}
	return s, testutil.Job(t, s, store.Job{Name: "web", Status: "running"})
}

// Every is the minimum gap between digests: the first match goes out at once (on the next poll), the
// next only once Every has passed; non-matching lines alone deliver nothing; the final digest carries
// the job's last line even though it did not match.
func TestFollowMatchAndFinal(t *testing.T) {
	s, j := setup(t, true)
	s.Append(j.ID, []string{"old line"}, store.NowMS())
	sink := &fakeSink{}
	done := make(chan error, 1)
	const every = time.Second
	go func() {
		done <- Follow(context.Background(), s, j, Options{Every: every, Match: "ERR", MaxLines: 2, After: 1}, sink)
	}()
	appended := time.Now()
	s.Append(j.ID, []string{"ok", "ERR a", "ERR b", "ERR " + strings.Repeat("x", 400)}, store.NowMS())
	testutil.WaitFor(t, every, func() bool { return sink.n() == 1 })
	ds, _ := sink.get()
	d := ds[0]
	if d.New != 4 || d.Matched != 3 || len(d.Lines) != 2 || d.Lines[0].Text != "ERR b" || d.Final || len(d.Lines[1].Text) > 310 {
		t.Fatalf("bad digest %+v", d)
	}
	s.Append(j.ID, []string{"ERR c"}, store.NowMS())
	testutil.WaitFor(t, 3*every, func() bool { return sink.n() == 2 })
	s.Append(j.ID, []string{"no match"}, store.NowMS())
	time.Sleep(3 * pollEvery) // a negative check
	s.Finish(j.ID, "exited", 3, "", false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ds, _ = sink.get()
	f := ds[len(ds)-1]
	if len(ds) != 3 || !f.Final || f.Status != "exited" || f.ExitCode == nil || *f.ExitCode != 3 || f.New != 1 || f.From != ds[1].To {
		t.Fatalf("bad final %+v of %d digests", f, len(ds))
	}
	if f.LastLine == nil || f.LastLine.Text != "no match" || f.LastMatched || f.Total != 7 {
		t.Fatalf("final summary %+v %+v", f, f.LastLine)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if first, gap := sink.at[0].Sub(appended), sink.at[1].Sub(sink.at[0]); first >= every || gap < every {
		t.Fatalf("first digest after %v, second %v later", first, gap)
	}
}

func TestFollowCancel(t *testing.T) {
	s, j := setup(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	sink := &fakeSink{}
	go func() { done <- Follow(ctx, s, j, Options{Every: MinEvery, After: -1}, sink) }()
	time.Sleep(3 * MinEvery) // a negative check: empty ticks deliver nothing
	cancel()
	if err := <-done; err != context.Canceled || sink.n() != 0 {
		t.Fatalf("got %v, %d digests", err, sink.n())
	}
}

// WaitCursor returns the first match with the lines before it and where to wait again from (the match,
// or the last line checked); after -1 starts at the job's end; an ended or lost job ends the wait.
func TestWaitCursor(t *testing.T) {
	s, j := setup(t, true)
	s.Append(j.ID, []string{"a", "ready 1", "b", "ready 2", "c"}, store.NowMS())
	ready := regexp.MustCompile("ready")
	lines, code, cur, err := WaitCursor(context.Background(), s, j, ready, time.Second, 0)
	if err != nil || code != 0 || len(lines) != 2 || cur != lines[1].ID {
		t.Fatalf("match: %v %d %d %v", lines, code, cur, err)
	}
	_, code, cur, err = WaitCursor(context.Background(), s, j, ready, 60*time.Millisecond, -1)
	if max, _ := s.MaxID(j.ID); err != nil || code != WaitTimeout || cur != max {
		t.Fatalf("timeout from the end: %d %d (max %d) %v", code, cur, max, err)
	}
	s.Finish(j.ID, "exited", 0, "", false)
	if _, code, _, _ = WaitCursor(context.Background(), s, j, regexp.MustCompile("never"), time.Second, 0); code != WaitEnded {
		t.Fatalf("ended code %d", code)
	}
	s, lost := setup(t, false)
	if _, code, _, _ := WaitCursor(context.Background(), s, lost, ready, time.Second, -1); code != WaitEnded {
		t.Fatalf("lost code %d", code)
	}
}

func TestFollowNonFinalFailureKeepsNews(t *testing.T) {
	s, j := setup(t, true)
	sink := &fakeSink{fail: func(d deliver.Digest, try int) bool { return try == 1 }}
	done := make(chan error, 1)
	go func() { done <- Follow(context.Background(), s, j, Options{Every: MinEvery, After: 0}, sink) }()
	s.Append(j.ID, []string{"a", "b"}, store.NowMS())
	testutil.WaitFor(t, 2*time.Second, func() bool { _, tries := sink.get(); return tries >= 1 })
	s.Append(j.ID, []string{"c"}, store.NowMS())
	testutil.WaitFor(t, 2*time.Second, func() bool { return sink.n() == 1 })
	if ds, _ := sink.get(); ds[0].From != 0 || ds[0].New < 2 {
		t.Fatalf("news lost after failed delivery: %+v", ds[0])
	}
	s.Finish(j.ID, "exited", 0, "", false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The final digest is retried (1 + 3 tries), and a lost job ends with one.
func TestFollowFinal(t *testing.T) {
	testutil.Swap(t, &finalRetry, func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), 3)
	})
	s, j := setup(t, true)
	s.Finish(j.ID, "exited", 0, "", false)
	sink := &fakeSink{fail: func(d deliver.Digest, try int) bool { return try <= 2 }} // flaky: 2 failures
	if err := Follow(context.Background(), s, j, Options{Every: MinEvery, After: 0}, sink); err != nil {
		t.Fatal(err)
	}
	if ds, tries := sink.get(); len(ds) != 1 || !ds[0].Final || tries != 3 {
		t.Fatalf("tries %d digests %+v", tries, ds)
	}
	sink = &fakeSink{fail: func(deliver.Digest, int) bool { return true }}
	err := Follow(context.Background(), s, j, Options{Every: MinEvery, After: 0}, sink)
	if _, tries := sink.get(); err == nil || !strings.Contains(err.Error(), "sink down") || tries != 4 {
		t.Fatalf("want error after 1+3 tries, got %v after %d", err, tries)
	}

	s, lost := setup(t, false)
	sink = &fakeSink{}
	if err := Follow(context.Background(), s, lost, Options{Every: MinEvery, After: 0}, sink); err != nil {
		t.Fatal(err)
	}
	if ds, _ := sink.get(); len(ds) != 1 || !ds[0].Final || ds[0].Status != "lost" {
		t.Fatalf("lost: %+v", ds)
	}
}

// Two followers racing to register for the same job and destination: exactly one wins, and its release
// removes only its own row. Another destination is independent.
func TestRegisterRace(t *testing.T) {
	testutil.Swap(t, &proc.LockWait, 100*time.Millisecond)
	s, j := setup(t, true)
	var wg sync.WaitGroup
	releases := make(chan func(), 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if release, err := Register(s, j.ID, "t3:x"); err == nil {
				releases <- release
			}
		}()
	}
	wg.Wait()
	close(releases)
	if len(releases) != 1 {
		t.Fatalf("%d followers registered", len(releases))
	}
	other, err := Register(s, j.ID, "jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer other()
	if n, ok, _ := s.GetNotifier(j.ID, "t3:x"); !ok || n.PID != os.Getpid() || !proc.Locked(NotifierLock(s, j.ID, "t3:x")) {
		t.Fatalf("winner row %+v %v", n, ok)
	}
	(<-releases)()
	if _, ok, _ := s.GetNotifier(j.ID, "t3:x"); ok {
		t.Fatal("released row left")
	}
	if _, ok, _ := s.GetNotifier(j.ID, "jsonl"); !ok {
		t.Fatal("release removed the other destination's row")
	}
}
