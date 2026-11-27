package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func queryStore(t *testing.T) *Store {
	t.Helper()
	// Characters that are special in a file: URI must not break (or redirect) the read-only open.
	s, err := Open(filepath.Join(t.TempDir(), "a b#c?d%e"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	j := Job{ID: NewJobID(), Name: "web", Kind: "cmd", Transport: "local", Spec: "{}", Status: "starting", StartedAt: NowMS()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(j.ID, []string{"alpha", "beta", "attach me"}, NowMS()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestQueryReads(t *testing.T) {
	s := queryStore(t)
	ctx := context.Background()
	for _, q := range []string{
		"select count(*) n from lines",
		"select count(*) n from lines;",
		"select count(*) n from lines; -- trailing comment",
	} {
		cols, rows, err := s.Query(ctx, q, 0)
		if err != nil || len(cols) != 1 || cols[0] != "n" || len(rows) != 1 || rows[0][0] != int64(3) {
			t.Fatalf("%q: %v %v %v", q, cols, rows, err)
		}
	}
	// A string literal mentioning attach is fine; so are FTS and read pragmas.
	for _, q := range []string{
		"select 'attach database x' as t, text from lines where text like '%attach%'",
		"select rowid from lines_fts where lines_fts match 'beta'",
		"select name from pragma_table_info('lines')",
		"pragma table_info(lines)",
		"select 1.5, null, x'41'",
	} {
		if _, rows, err := s.Query(ctx, q, 0); err != nil || len(rows) == 0 {
			t.Fatalf("%q: %v %v", q, rows, err)
		}
	}
}

func TestQueryRejects(t *testing.T) {
	s := queryStore(t)
	for _, q := range []string{
		"",
		"select 1; select 2",
		"select 1; delete from lines",
		"delete from lines",
		"insert into lines(job, ts, text) values('x', 0, 'y')",
		"update jobs set status = 'x'",
		"create table t(x)",
		"PRAGMA query_only=0",
		"pragma journal_mode=delete",
		"ATTACH DATABASE '/tmp/tail-mode-evil.db' AS evil",
		"DETACH DATABASE main",
		"begin",
		"select load_extension('x')",
		"vacuum into '/tmp/tail-mode-evil.db'",
	} {
		if _, _, err := s.Query(context.Background(), q, 0); err == nil {
			t.Errorf("%q: want error", q)
		}
	}
	if _, rows, _ := s.Query(context.Background(), "select count(*) from lines", 0); rows[0][0] != int64(3) {
		t.Fatalf("data changed: %v", rows)
	}
}

func TestQueryTimeout(t *testing.T) {
	s := queryStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := s.Query(ctx, "with recursive c(x) as (select 1 union all select x+1 from c) select count(*) from c", 0)
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("want deadline error, got %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestQueryRowBudget(t *testing.T) {
	s := queryStore(t)
	// Row 2 overflows; with limit 1 it must never be evaluated.
	q := "select abs(x) from (select 1 as x union all select -9223372036854775808)"
	_, rows, err := s.Query(context.Background(), q, 1)
	if err != nil || len(rows) != 1 || rows[0][0] != int64(1) {
		t.Fatalf("limit 1: %v %v", rows, err)
	}
	if _, _, err := s.Query(context.Background(), q, 2); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("limit 2 should hit the overflow, got %v", err)
	}
}
