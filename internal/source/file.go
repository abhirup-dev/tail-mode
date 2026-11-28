package source

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/abhirup-dev/tail-mode/internal/transport"
)

// FileConfig is the "file" adapter's config.
type FileConfig struct {
	Path    string `json:"path"`              // "~" / "~/x" = the home dir where the transport runs
	Backlog *int   `json:"backlog,omitempty"` // lines of history on first start; nil → DefaultBacklog; ignored on resume
}

const DefaultBacklog = 50

func init() {
	Register("file", func(raw json.RawMessage) (Source, error) {
		var c FileConfig
		if err := decode("file", raw, &c); err != nil {
			return nil, err
		}
		if c.Path == "" {
			return nil, fmt.Errorf("file source needs a path")
		}
		if c.Backlog != nil && *c.Backlog < 0 {
			return nil, fmt.Errorf("file source backlog must be ≥ 0, got %d", *c.Backlog)
		}
		if _, err := homeRel(c.Path); err != nil {
			return nil, fmt.Errorf("file source path: %w", err)
		}
		return file{c}, nil
	})
}

type file struct{ FileConfig }

// shortcut: lines written while disconnected are lost; upgrade with byte-offset resume via stat+tail -c.
func (f file) Stream(ctx context.Context, t transport.Transport, resumed bool, emit func([]string)) error {
	return stream(ctx, t, f.argv(resumed), emit)
}

// argv is `tail -n N -F -- PATH`; a "~" path goes through sh so that $HOME is the target's.
func (f file) argv(resumed bool) []string {
	n := DefaultBacklog
	if f.Backlog != nil {
		n = *f.Backlog
	}
	if resumed {
		n = 0
	}
	if rest, _ := homeRel(f.Path); rest != nil {
		return []string{"sh", "-c", "exec tail -n " + strconv.Itoa(n) + " -F -- " + shellPath(f.Path)}
	}
	return []string{"tail", "-n", strconv.Itoa(n), "-F", "--", f.Path}
}

func (f file) Describe() string       { return "tail -F " + f.Path }
func (f file) RestartByDefault() bool { return true }

// LocalDefaults makes a relative path absolute against the caller's cwd ("~" paths are left alone).
// With cwd "" (caller's dir unknown) a relative path is an ErrNeedAbsolute error.
func (f file) LocalDefaults(cwd string) (any, error) {
	if rest, _ := homeRel(f.Path); rest == nil && !filepath.IsAbs(f.Path) {
		if cwd == "" {
			return nil, fmt.Errorf("%w: file path %q is relative", ErrNeedAbsolute, f.Path)
		}
		f.Path = filepath.Join(cwd, f.Path)
	}
	return f.FileConfig, nil
}
