package store

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
)

// oldSchema is the jobs table as created before owners existed (in WAL mode, as every earlier Open left it).
const oldSchema = `
PRAGMA journal_mode=WAL;
CREATE TABLE jobs(
  id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL, transport TEXT NOT NULL,
  target TEXT NOT NULL DEFAULT '', spec TEXT NOT NULL, describe TEXT NOT NULL DEFAULT '',
  pid INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, started_at INTEGER NOT NULL,
  ended_at INTEGER NOT NULL DEFAULT 0, exit_code INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '');
CREATE INDEX jobs_name ON jobs(name, started_at);
INSERT INTO jobs(id,name,kind,transport,spec,status,started_at) VALUES('j000001','web','cmd','local','{}','exited',1);
`

// A DB without the owner column is migrated on Open (by several concurrent openers at once, as
// workers, followers and the MCP server do); old jobs land in the shared namespace.
func TestMigrateOwner(t *testing.T) {
	dir := t.TempDir()
	raw, err := sql.Open("sqlite3", "file:"+filepath.Join(dir, DBFile))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := Open(dir)
			if err == nil {
				s.Close()
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent open: %v", err)
		}
	}

	s, err := Open(dir) // and once more on an already migrated DB
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var cols, idx int
	s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name='owner'`).Scan(&cols)
	s.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='jobs_owner_name'`).Scan(&idx)
	if cols != 1 || idx != 1 {
		t.Fatalf("owner column %d, index %d", cols, idx)
	}
	j, err := s.GetJob("web", "")
	if err != nil || j.ID != "j000001" || j.Owner != "" {
		t.Fatalf("old job by name: %+v %v", j, err)
	}
	if _, err := s.GetJob("web", "thread-a"); err == nil {
		t.Fatal("old shared job resolved by name under another owner")
	}
	if j, err := s.GetJob("j000001", "thread-a"); err != nil || j.Name != "web" {
		t.Fatalf("old job by id: %+v %v", j, err)
	}
	if err := s.CreateJob(Job{ID: "j000002", Name: "web", Owner: "thread-a", Kind: "cmd", Transport: "local", Spec: `{}`, Status: "exited", StartedAt: 2}); err != nil {
		t.Fatal(err)
	}
	if j, _ := s.GetJob("web", ""); j.ID != "j000001" {
		t.Fatalf("shared web resolved to %s", j.ID)
	}
	if j, _ := s.GetJob("web", "thread-a"); j.ID != "j000002" {
		t.Fatalf("thread-a web resolved to %s", j.ID)
	}
}
