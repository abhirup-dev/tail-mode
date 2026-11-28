package source

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/abhirup-dev/tail-mode/internal/transport"
)

// CmdConfig is the "cmd" adapter's config.
type CmdConfig struct {
	Cmd   string            `json:"cmd"`
	Dir   string            `json:"dir,omitempty"`   // working dir; "" = where the transport starts (remote: home); "~", "~/x" ok
	Env   map[string]string `json:"env,omitempty"`   // exported before cmd; PYTHONUNBUFFERED=1 unless set here
	Shell string            `json:"shell,omitempty"` // bare name or absolute path of a POSIX shell; "" = bash
	Login bool              `json:"login,omitempty"` // run as a login shell (-lc): profile files set PATH etc.
}

const DefaultShell = "bash"

// defaultEnv is exported unless the config sets the same key. Python block-buffers stdout when it is not
// a terminal, so without this its lines would arrive only at exit.
var defaultEnv = map[string]string{"PYTHONUNBUFFERED": "1"}

var envKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func init() {
	Register("cmd", func(raw json.RawMessage) (Source, error) {
		var c CmdConfig
		if err := decode("cmd", raw, &c); err != nil {
			return nil, err
		}
		if err := c.validate(); err != nil {
			return nil, err
		}
		return command{c}, nil
	})
}

func (c CmdConfig) validate() error {
	if c.Cmd == "" {
		return fmt.Errorf("cmd source needs a command")
	}
	for k := range c.Env {
		if !envKey.MatchString(k) {
			return fmt.Errorf("cmd source: invalid env name %q", k)
		}
	}
	if sh := c.Shell; sh != "" {
		if err := transport.CheckWord(sh); err != nil {
			return fmt.Errorf("cmd source: invalid shell %q: %w", sh, err)
		}
		if strings.Contains(sh, "/") && !filepath.IsAbs(sh) {
			return fmt.Errorf("cmd source: shell %q must be a bare name or an absolute path", sh)
		}
	}
	if _, err := homeRel(c.Dir); err != nil {
		return fmt.Errorf("cmd source dir: %w", err)
	}
	return nil
}

type command struct{ CmdConfig }

func (c command) Stream(ctx context.Context, t transport.Transport, _ bool, emit func([]string)) error {
	return stream(ctx, t, c.argv(), emit)
}

// argv is [shell, "-c"|"-lc", script]: one script so that it behaves the same over every transport.
func (c command) argv() []string {
	sh, flag := c.Shell, "-c"
	if sh == "" {
		sh = DefaultShell
	}
	if c.Login {
		flag = "-lc"
	}
	return []string{sh, flag, c.script()}
}

// script is `cd -- DIR || exit 97`, then `export K=V` per env var (sorted), then the command.
func (c command) script() string {
	var b strings.Builder
	if c.Dir != "" {
		b.WriteString("cd -- " + shellPath(c.Dir) + " || exit 97\n")
	}
	env := maps.Clone(defaultEnv)
	maps.Copy(env, c.Env)
	for _, k := range slices.Sorted(maps.Keys(env)) {
		b.WriteString("export " + k + "=" + transport.Quote(env[k]) + "\n")
	}
	b.WriteString(c.Cmd)
	return b.String()
}

func (c command) Describe() string       { return c.Cmd }
func (c command) RestartByDefault() bool { return false }

// LocalDefaults records the caller's cwd as the working dir when none (or a relative one) is given.
// With cwd "" (caller's dir unknown) such a dir is an ErrNeedAbsolute error.
func (c command) LocalDefaults(cwd string) (any, error) {
	if rel, _ := homeRel(c.Dir); rel == nil && !filepath.IsAbs(c.Dir) {
		if cwd == "" {
			if c.Dir == "" {
				return nil, fmt.Errorf("%w: cmd has no dir", ErrNeedAbsolute)
			}
			return nil, fmt.Errorf("%w: cmd dir %q is relative", ErrNeedAbsolute, c.Dir)
		}
		c.Dir = filepath.Join(cwd, c.Dir)
	}
	return c.CmdConfig, nil
}

// homeRel reports whether p is "~" or "~/rest" and returns rest ("" for "~"); nil if p does not start
// with "~". "~user" forms are rejected: they would need the remote's passwd database.
func homeRel(p string) (*string, error) {
	if !strings.HasPrefix(p, "~") {
		return nil, nil
	}
	if p == "~" {
		rest := ""
		return &rest, nil
	}
	if rest, ok := strings.CutPrefix(p, "~/"); ok {
		return &rest, nil
	}
	return nil, fmt.Errorf("%q: only ~ and ~/path are supported, not ~user", p)
}

// shellPath quotes p for a POSIX shell, expanding a leading "~" or "~/" through "$HOME" so that it is
// the home dir of wherever the shell runs.
func shellPath(p string) string {
	rest, _ := homeRel(p)
	switch {
	case rest == nil:
		return transport.Quote(p)
	case *rest == "":
		return `"$HOME"`
	default:
		return `"$HOME"/` + transport.Quote(*rest)
	}
}
