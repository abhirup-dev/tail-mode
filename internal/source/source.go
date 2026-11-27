// Package source is layer 2: what to read to get lines. Adapters are given a transport and never
// call ssh (or anything target-specific) themselves.
package source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/abhirup-dev/tail-mode/internal/transport"
)

// Spec is the persisted description of a source (stored as JSON in jobs.spec).
type Spec struct {
	Kind    string          `json:"kind"`
	Config  json.RawMessage `json:"config,omitempty"`  // adapter-specific; validated + defaulted by the adapter
	Restart *bool           `json:"restart,omitempty"` // nil = Source.RestartByDefault()
}

// Source streams lines until ctx is cancelled or the source ends.
type Source interface {
	// Stream runs the source on t and calls emit with batches of lines (without trailing newlines).
	// resumed is true on restarts. Returns nil on clean end; an error wrapping *exec.ExitError on a
	// non-zero exit; ctx.Err() when cancelled.
	Stream(ctx context.Context, t transport.Transport, resumed bool, emit func([]string)) error
	// Describe is a short human label, e.g. "tail -F /var/log/app.log".
	Describe() string
	// RestartByDefault reports whether the job should restart when the stream ends.
	RestartByDefault() bool
}

// parsers is the adapter registry; adapters register in init().
var parsers = map[string]func(cfg json.RawMessage) (Source, error){}

// Register adds an adapter kind. Called from adapter files' init().
func Register(kind string, parse func(cfg json.RawMessage) (Source, error)) { parsers[kind] = parse }

// New builds the adapter for spec.Kind from spec.Config.
func New(spec Spec) (Source, error) {
	parse, ok := parsers[spec.Kind]
	if !ok {
		return nil, fmt.Errorf("unknown source kind %q (have %v)", spec.Kind, Kinds())
	}
	return parse(spec.Config)
}

// localDefaulter is implemented by adapters whose config depends on where a local caller is: it returns
// the config with those defaults filled in (e.g. a relative path made absolute against cwd).
type localDefaulter interface {
	LocalDefaults(cwd string) (any, error)
}

// ErrNeedAbsolute is returned by LocalDefaults with cwd "" for a local path that would need the
// caller's working dir (a relative or missing cmd dir, a relative file path).
var ErrNeedAbsolute = errors.New("needs an absolute path")

// LocalDefaults fills in the defaults that only make sense for a job on the local transport, started by
// a caller whose working directory is cwd (the detached worker's cwd is not the caller's); cwd "" means
// unknown, and then anything that would need it is an ErrNeedAbsolute error. Integrations
// call it when the transport is local; adapters without such defaults return spec unchanged.
func LocalDefaults(spec Spec, cwd string) (Spec, error) {
	src, err := New(spec)
	if err != nil {
		return spec, err
	}
	ld, ok := src.(localDefaulter)
	if !ok {
		return spec, nil
	}
	cfg, err := ld.LocalDefaults(cwd)
	if err != nil {
		return spec, err
	}
	if spec.Config, err = json.Marshal(cfg); err != nil {
		return spec, err
	}
	return spec, nil
}

// Kinds lists the registered adapter kinds, sorted.
func Kinds() []string { return slices.Sorted(maps.Keys(parsers)) }

// decode strictly unmarshals an adapter config (unknown fields are errors; empty means {}).
func decode(kind string, cfg json.RawMessage, v any) error {
	if len(bytes.TrimSpace(cfg)) == 0 {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(cfg))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("%s source config: %w", kind, err)
	}
	if d.More() {
		return fmt.Errorf("%s source config: trailing data", kind)
	}
	return nil
}
