package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestMain clears T3CODE_THREAD_ID so no test can reach a real T3 thread, and so identity does not
// depend on where the tests run. Tests that set it also stub the notifier seam.
func TestMain(m *testing.M) {
	os.Unsetenv("T3CODE_THREAD_ID")
	os.Exit(m.Run())
}

func testStore(t *testing.T, lines ...string) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.CreateJob(store.Job{ID: "j000001", Name: "web", Kind: "cmd", Transport: "local", Spec: `{"kind":"cmd"}`, Describe: "x", Status: "exited", StartedAt: store.NowMS()}); err != nil {
		t.Fatal(err)
	}
	if len(lines) == 0 {
		lines = []string{"hello", "ERROR db timeout", "bye"}
	}
	if err := s.Append("j000001", lines, store.NowMS()); err != nil {
		t.Fatal(err)
	}
	return s
}

// connect connects an MCP client to a server over in-memory transports. The handler reads
// T3CODE_THREAD_ID when it is created, so set it before.
func connect(t *testing.T, s *store.Store, shared bool) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	ct, st := mcp.NewInMemoryTransports()
	ss, err := newServer(&handler{s: s, shared: shared, thread: EnvThread()}).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close(); ss.Wait() })
	return cs
}

func client(t *testing.T, s *store.Store) *mcp.ClientSession       { return connect(t, s, false) }
func sharedClient(t *testing.T, s *store.Store) *mcp.ClientSession { return connect(t, s, true) }

// callResult returns a tool's text and whether it is a tool error; transport errors are returned.
func callResult(cs *mcp.ClientSession, tool string, args map[string]any) (string, bool, error) {
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return "", false, err
	}
	return res.Content[0].(*mcp.TextContent).Text, res.IsError, nil
}

func call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) string {
	t.Helper()
	text, isErr, err := callResult(cs, tool, args)
	if err != nil {
		t.Fatal(err)
	}
	if isErr {
		t.Fatalf("%s: tool error: %s", tool, text)
	}
	return text
}

func callErr(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) string {
	t.Helper()
	text, isErr, err := callResult(cs, tool, args)
	if err != nil {
		t.Fatal(err)
	}
	if !isErr {
		t.Fatalf("%s %v: want a tool error, got:\n%s", tool, args, text)
	}
	return text
}

// fakes replaces the job/notifier seams. startJob resolves like jobs.Start and inserts a "starting" row
// (no process; Stop aborts it) with ids j000009, j00000a, … and one marker line; notifier starts are
// recorded. last is the latest StartOpts as passed in. Both are safe for concurrent use.
func fakes(t *testing.T) (last func() jobs.StartOpts, notes func() []follow.Options) {
	var mu sync.Mutex
	var started jobs.StartOpts
	var ns []follow.Options
	n := 8
	oldJob, oldNote := startJob, startNotifier
	t.Cleanup(func() { startJob, startNotifier = oldJob, oldNote })
	startJob = func(s *store.Store, o jobs.StartOpts) (store.Job, error) {
		mu.Lock()
		started, n = o, n+1
		id := fmt.Sprintf("j%06x", n)
		mu.Unlock()
		o, err := o.Resolve()
		if err != nil {
			return store.Job{}, err
		}
		spec, _ := json.Marshal(o.Spec)
		j := store.Job{ID: id, Name: o.Name, Owner: o.Owner, Kind: o.Spec.Kind, Transport: "local", Target: o.Target,
			Spec: string(spec), Describe: "stub", Status: "starting", StartedAt: store.NowMS()}
		if o.Target != "" {
			j.Transport = o.Transport
		}
		if err := s.CreateJob(j); err != nil {
			return j, err
		}
		if err := s.Append(j.ID, []string{"marker " + o.Owner + " " + j.ID}, store.NowMS()); err != nil {
			return j, err
		}
		return s.GetJob(j.ID, "")
	}
	startNotifier = func(_ *store.Store, _ string, o follow.Options) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		ns = append(ns, o)
		return 4242, nil
	}
	return func() jobs.StartOpts { mu.Lock(); defer mu.Unlock(); return started },
		func() []follow.Options { mu.Lock(); defer mu.Unlock(); return append([]follow.Options(nil), ns...) }
}
