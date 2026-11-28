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
	Login *bool             `json:"login,omitempty"` // first take PATH from the target user's shell profile; nil = remote only
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
	return stream(ctx, t, c.argv(t.Target() != ""), emit)
}

// argv is [shell, "-c", script]: one script so that it behaves the same over every transport. With the
// profile on, sh runs profileStep first and then execs it.
func (c command) argv(remote bool) []string {
	sh := c.Shell
	if sh == "" {
		sh = DefaultShell
	}
	argv := []string{sh, "-c", c.script()}
	if c.profile(remote) {
		argv = append([]string{"sh", "-c", profileStep, "sh"}, argv...)
	}
	return argv
}

// profileStep exports the PATH the target user's own shell ends up with after its login and
// interactive profile files (the last __TM_PATH= line it prints; all its other output is discarded),
// then runs "$@". The job itself stays non-interactive.
const profileStep = `t=; command -v timeout >/dev/null 2>&1 && t='timeout 10'; ` +
	`p=$($t "${SHELL:-sh}" -lic 'printf "\n__TM_PATH=%s\n" "$PATH"' </dev/null 2>/dev/null | sed -n 's/^__TM_PATH=//p' | tail -n 1); ` +
	`[ -n "$p" ] && export PATH="$p"; exec "$@"`

// profile is login if set, else on for remote targets: local jobs already inherit the caller's env.
func (c CmdConfig) profile(remote bool) bool {
	if c.Login != nil {
		return *c.Login
	}
	return remote
}

// Profile reports whether a job of spec on a remote (or local) target loads the target's shell profile.
func Profile(spec Spec, remote bool) bool {
	var c CmdConfig
	return spec.Kind == "cmd" && decode("cmd", spec.Config, &c) == nil && c.profile(remote)
}

// ExitHint is the note recorded with a job of spec on target ("" = local) that exited with code: only
// 127, command not found.
func ExitHint(spec Spec, target string, code int) string {
	if code != 127 {
		return ""
	}
	where, loaded := target, "no"
	if where == "" {
		where = "local"
	}
	if Profile(spec, target != "") {
		loaded = "yes"
	}
	return fmt.Sprintf("exit 127: command not found on %s — check the name/PATH (profile loaded: %s) or use an absolute path", where, loaded)
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
