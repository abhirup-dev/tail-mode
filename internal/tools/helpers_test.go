package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/abhirup-dev/tail-mode/internal/follow"
	"github.com/abhirup-dev/tail-mode/internal/jobs"
	"github.com/abhirup-dev/tail-mode/internal/store"
	"github.com/abhirup-dev/tail-mode/internal/testutil"
)

// TestMain clears T3CODE_THREAD_ID so no test can reach a real T3 thread, and so identity does not
// depend on where the tests run. Tests that set it also stub the notifier seam.
func TestMain(m *testing.M) {
	os.Unsetenv("T3CODE_THREAD_ID")
	os.Exit(m.Run())
}

// testStore has job j000001 "web" (shared namespace, exited 3) with lines (default: hello, ERROR db
// timeout, bye).
func testStore(t *testing.T, lines ...string) *store.Store {
	t.Helper()
	s := testutil.Store(t)
	if len(lines) == 0 {
		lines = []string{"hello", "ERROR db timeout", "bye"}
	}
	testutil.Job(t, s, store.Job{ID: "j000001", Name: "web", Describe: "x"}, lines...)
	if err := s.Finish("j000001", "exited", 3, "", true); err != nil {
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
	ss, err := newServer(&handler{s: s, c: MCP, shared: shared, thread: EnvThread()}).Connect(ctx, st, nil)
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

func client(t *testing.T, s *store.Store) *mcp.ClientSession { return connect(t, s, false) }

// callResult returns a tool's text and whether it is a tool error; transport errors are returned.
func callResult(cs *mcp.ClientSession, tool string, args map[string]any) (string, bool, error) {
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return "", false, err
	}
	return res.Content[0].(*mcp.TextContent).Text, res.IsError, nil
}

// call returns a tool's text, failing the test unless the call was (wantErr) or was not a tool error.
func call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any, wantErr ...bool) string {
	t.Helper()
	text, isErr, err := callResult(cs, tool, args)
	if err != nil || isErr != (len(wantErr) > 0 && wantErr[0]) {
		t.Fatalf("%s %v: transport error %v, tool error %v:\n%s", tool, args, err, isErr, text)
	}
	return text
}

func callErr(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) string {
	t.Helper()
	return call(t, cs, tool, args, true)
}

// fakes replaces the job/notifier seams. startJob resolves like jobs.Start and inserts a "starting" row
// (no process; Stop aborts it) with ids j000009, j00000a, … and one marker line; notifier starts are
// recorded. last is the latest StartOpts as passed in. Both are safe for concurrent use.
func fakes(t *testing.T) (last func() jobs.StartOpts, notes func() []follow.Options) {
	var mu sync.Mutex
	var started jobs.StartOpts
	var ns []follow.Options
	n := 8
	testutil.Swap(t, &startJob, func(s *store.Store, o jobs.StartOpts) (store.Job, error) {
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
	})
	testutil.Swap(t, &startNotifier, func(_ *store.Store, _ string, o follow.Options) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		ns = append(ns, o)
		return 4242, nil
	})
	return func() jobs.StartOpts { mu.Lock(); defer mu.Unlock(); return started },
		func() []follow.Options { mu.Lock(); defer mu.Unlock(); return append([]follow.Options(nil), ns...) }
}
