// Package transport is layer 1: where a command runs. It knows nothing about logs.
package transport

import (
	"context"
	"fmt"
	"maps"
	"os/exec"
	"slices"

	"al.essio.dev/pkg/shellescape"
)

// Transport builds an *exec.Cmd that runs argv on some target.
type Transport interface {
	Name() string   // "local" | "ssh"
	Target() string // "" for local, host for ssh
	Command(ctx context.Context, argv ...string) *exec.Cmd
}

// factories is the transport registry; transports register in init().
var factories = map[string]func(target string) (Transport, error){}

// Register adds a transport. Called from transport files' init().
func Register(name string, fn func(target string) (Transport, error)) { factories[name] = fn }

// New returns the transport called name ("" = "local") for target, validating the target.
func New(name, target string) (Transport, error) {
	if name == "" {
		name = "local"
	}
	fn, ok := factories[name]
	if !ok {
		return nil, fmt.Errorf("unknown transport %q (have %v)", name, Names())
	}
	return fn(target)
}

// Names lists the registered transports, sorted.
func Names() []string {
	return slices.Sorted(maps.Keys(factories))
}

// Quote quotes s as one word for a POSIX shell.
func Quote(s string) string { return shellescape.Quote(s) }
