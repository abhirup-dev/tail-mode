package store

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func job(id, name, status string) Job {
	return Job{ID: id, Name: name, Kind: "file", Transport: "local", Spec: `{}`, Status: status, StartedAt: NowMS()}
}

func TestStore(t *testing.T) {
	s := open(t)
	if err := s.CreateJob(job("j000001", "web", "running")); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("j000001", []string{"ok one", "ERROR db timeout", "ok two"}, NowMS()); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("j000001", []string{"ERROR again"}, NowMS()+1000); err != nil {
		t.Fatal(err)
	}
	j, err := s.GetJob("web", "")
	if err != nil || j.ID != "j000001" {
		t.Fatalf("GetJob by name: %v %v", j, err)
	}
	all, _ := s.Read(ReadQuery{Job: j.ID})
	if len(all) != 4 {
		t.Fatalf("read all: %d", len(all))
	}
	tail, _ := s.Read(ReadQuery{Job: j.ID, Tail: 2})
	if len(tail) != 2 || tail[1].Text != "ERROR again" {
		t.Fatalf("tail: %+v", tail)
	}
	g, _ := s.Read(ReadQuery{Job: j.ID, Grep: regexp.MustCompile("ERROR"), Tail: 1})
	if len(g) != 1 || g[0].Text != "ERROR again" {
		t.Fatalf("grep tail: %+v", g)
	}
	jobs, _ := s.ListJobs("", true)
	if len(jobs) != 1 || jobs[0].Lines != 4 || jobs[0].LastID != all[3].ID || jobs[0].LastTS != all[3].TS {
		t.Fatalf("list: %+v", jobs)
	}
}

func TestDriverWALAndFTS(t *testing.T) {
	s := open(t)
	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode %q %v", mode, err)
	}
	s.CreateJob(job("j1", "", "running"))
	s.Append("j1", []string{"connection refused by upstream", "all good", "retrying connections"}, NowMS())
	hits, literal, err := s.Search(SearchQuery{Query: "connection"}) // porter stemming matches "connections" too
	if err != nil || literal != nil || len(hits) != 2 {
		t.Fatalf("search: %v %v %v", hits, literal, err)
	}
	if hits, literal, err := s.Search(SearchQuery{Query: "upstream-x OR"}); err != nil || literal == nil || len(hits) != 0 {
		t.Fatalf("search fallback: %v %v %v", hits, literal, err)
	}
	if hits, _, err := s.Search(SearchQuery{Query: "upstream", Job: "nope"}); err != nil || len(hits) != 0 {
		t.Fatalf("job filter: %v %v", hits, err)
	}
}

func TestGetJobEmptyRef(t *testing.T) {
	s := open(t)
	s.CreateJob(job("j1", "", "running")) // unnamed
	if _, err := s.GetJob("", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty ref: %v", err)
	}
	if _, err := s.GetJob("nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown ref: %v", err)
	}
	s.CreateJob(job("j2", "web", "exited"))
	s.CreateJob(job("j3", "web", "running"))
	if j, _ := s.GetJob("web", ""); j.ID != "j3" {
		t.Fatalf("latest by name: %s", j.ID)
	}
	if j, _ := s.GetJob("j2", ""); j.ID != "j2" {
		t.Fatalf("by id: %s", j.ID)
	}
}

// Names resolve within an owner (ids across owners); search is owner-scoped unless all or job, and since
// drops older lines.
func TestOwnersAndSearch(t *testing.T) {
	s := open(t)
	for i, o := range []string{"a", "b", ""} {
		j := job(fmt.Sprintf("j%d", i), "web", "running")
		j.Owner = o
		s.CreateJob(j)
		s.Append(j.ID, []string{"old hit " + o}, NowMS()-time.Hour.Milliseconds())
		s.Append(j.ID, []string{"new hit " + o}, NowMS())
	}
	for ref, want := range map[string]string{"web": "j1", "j0": "j0"} {
		if j, err := s.GetJob(ref, "b"); err != nil || j.ID != want {
			t.Errorf("GetJob(%s, b) = %s %v, want %s", ref, j.ID, err, want)
		}
	}
	if _, err := s.GetJob("web", "c"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a third owner resolves web: %v", err)
	}
	if js, _ := s.ListJobs("a", false); len(js) != 1 || js[0].ID != "j0" {
		t.Errorf("a lists %+v", js)
	}
	for _, c := range []struct {
		q    SearchQuery
		want int
	}{
		{SearchQuery{Query: "hit", Owner: "a"}, 2},
		{SearchQuery{Query: "hit", Owner: ""}, 2},
		{SearchQuery{Query: "hit", Owner: "a", All: true}, 6},
		{SearchQuery{Query: "hit", Owner: "a", Job: "j1"}, 2},
		{SearchQuery{Query: "hit", All: true, SinceTS: NowMS() - time.Minute.Milliseconds()}, 3},
	} {
		if hits, _, err := s.Search(c.q); err != nil || len(hits) != c.want {
			t.Errorf("%+v: %d hits %v, want %d", c.q, len(hits), err, c.want)
		}
	}
}

// One notifier row per (job, deliver): set upserts the pid, delete only removes the row if the pid is
// still the caller's (a replaced follower must not delete its successor's row).
func TestNotifiers(t *testing.T) {
	s := open(t)
	s.SetNotifier("j1", "t3:x", 10)
	s.SetNotifier("j1", "t3:x", 11)
	s.SetNotifier("j1", "jsonl", 12)
	s.SetNotifier("j2", "jsonl", 13)
	s.DeleteNotifier("j1", "t3:x", 10)
	if n, ok, err := s.GetNotifier("j1", "t3:x"); !ok || err != nil || n.PID != 11 {
		t.Fatalf("get %+v %v %v", n, ok, err)
	}
	s.DeleteNotifier("j1", "t3:x", 11)
	if _, ok, err := s.GetNotifier("j1", "t3:x"); ok || err != nil {
		t.Fatalf("deleted row still there: %v", err)
	}
	var n int
	if s.db.QueryRow(`SELECT COUNT(*) FROM notifiers`).Scan(&n); n != 2 {
		t.Fatalf("%d rows, want 2", n)
	}
}

func TestClaimAbortFinish(t *testing.T) {
	s := open(t)
	s.CreateJob(job("j1", "", "starting"))
	if ok, err := s.Claim("j1", 4242); !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if ok, _ := s.Claim("j1", 1); ok {
		t.Fatal("second claim must fail")
	}
	if ok, _ := s.Abort("j1"); ok {
		t.Fatal("abort after claim must fail")
	}
	if j, _ := s.GetJob("j1", ""); j.Status != "running" || j.PID != 4242 {
		t.Fatalf("claimed: %+v", j)
	}

	s.CreateJob(job("j2", "", "starting"))
	if ok, _ := s.Abort("j2"); !ok {
		t.Fatal("abort starting")
	}
	if ok, _ := s.Claim("j2", 1); ok {
		t.Fatal("claim after abort must fail")
	}

	// Non-force Finish works from starting or running, never from a terminal status.
	s.CreateJob(job("j3", "", "starting"))
	s.Finish("j3", "failed", 0, "boom", false)
	s.Finish("j1", "exited", 3, "", false)
	s.Finish("j1", "lost", 0, "", false)
	s.Finish("j2", "lost", 0, "", false)
	for id, want := range map[string]string{"j1": "exited", "j2": "stopped", "j3": "failed"} {
		if j, _ := s.GetJob(id, ""); j.Status != want {
			t.Fatalf("%s: %s, want %s", id, j.Status, want)
		}
	}
	s.Finish("j1", "stopped", 0, "", true)
	if j, _ := s.GetJob("j1", ""); j.Status != "stopped" {
		t.Fatalf("force: %s", j.Status)
	}
}

func TestTruncateRuneSafe(t *testing.T) {
	s := open(t)
	s.CreateJob(job("j1", "", "running"))
	long := strings.Repeat("a", MaxLineBytes-1) + "é" + strings.Repeat("b", 10) // é straddles the cut
	s.Append("j1", []string{long}, NowMS())
	ls, _ := s.Read(ReadQuery{Job: "j1"})
	got := ls[0].Text
	if !utf8.ValidString(got) || !strings.HasSuffix(got, "…[truncated]") || strings.Contains(got, "é") {
		t.Fatalf("truncated line invalid: len %d suffix %q", len(got), got[len(got)-20:])
	}
	if short := "héllo"; truncate(short) != short {
		t.Fatal("short line changed")
	}
}
