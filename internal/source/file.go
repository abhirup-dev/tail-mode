package source

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/abhirup-dev/tail-mode/internal/transport"
)

// FileConfig is the "file" adapter's config.
type FileConfig struct {
	Path    string `json:"path"`
	Backlog *int   `json:"backlog,omitempty"` // lines of history on first start; nil → 50; ignored on resume
}

const defaultBacklog = 50

func init() {
	Register("file", func(raw json.RawMessage) (Source, error) {
		var c FileConfig
		if err := decode("file", raw, &c); err != nil {
			return nil, err
		}
		if c.Path == "" {
			return nil, fmt.Errorf("file source needs a path")
		}
		if c.Backlog == nil {
			n := defaultBacklog
			c.Backlog = &n
		} else if *c.Backlog < 0 {
			return nil, fmt.Errorf("file source backlog must be ≥ 0, got %d", *c.Backlog)
		}
		return file{c}, nil
	})
}

type file struct{ FileConfig }

// shortcut: lines written while disconnected are lost; upgrade with byte-offset resume via stat+tail -c.
func (f file) Stream(ctx context.Context, t transport.Transport, resumed bool, emit func([]string)) error {
	n := *f.Backlog
	if resumed {
		n = 0
	}
	return stream(ctx, t, []string{"tail", "-n", strconv.Itoa(n), "-F", "--", f.Path}, emit)
}

func (f file) Describe() string       { return "tail -F " + f.Path }
func (f file) RestartByDefault() bool { return true }
