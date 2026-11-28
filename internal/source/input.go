package source

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Errors from SpecInput.Spec; each integration rewords them in its own flag or param names.
var (
	ErrOneSource         = errors.New("needs exactly one of cmd, file, or source")
	ErrConfigNeedsSource = errors.New("config needs source")
	ErrBacklogNeedsFile  = errors.New("backlog applies to file")
	ErrCmdOptionsNeedCmd = errors.New("dir, env, shell and login apply to cmd")
	ErrConfigInvalidJSON = errors.New("config is not valid JSON")
)

// SpecInput is what an integration collects to start a job: the file or cmd shorthand (with the cmd
// options) or a generic source + config. Config nil = not given.
type SpecInput struct {
	CmdConfig
	File    string
	Backlog *int
	Source  string
	Config  json.RawMessage
	Restart *bool
}

// Spec checks which shorthand was used and builds the Spec. Adapters validate the config itself later
// (New). Errors wrap the Err* values above.
func (in SpecInput) Spec() (Spec, error) {
	n := 0
	for _, set := range []bool{in.File != "", in.Cmd != "", in.Source != ""} {
		if set {
			n++
		}
	}
	switch {
	case n != 1:
		return Spec{}, fmt.Errorf("%w (got %d)", ErrOneSource, n)
	case in.Config != nil && in.Source == "":
		return Spec{}, ErrConfigNeedsSource
	case in.Backlog != nil && in.File == "":
		return Spec{}, ErrBacklogNeedsFile
	case (in.Dir != "" || len(in.Env) > 0 || in.Shell != "" || in.Login) && in.Cmd == "":
		return Spec{}, ErrCmdOptionsNeedCmd
	}
	spec := Spec{Restart: in.Restart}
	var cfg any
	switch {
	case in.File != "":
		spec.Kind, cfg = "file", FileConfig{Path: in.File, Backlog: in.Backlog}
	case in.Cmd != "":
		spec.Kind, cfg = "cmd", in.CmdConfig
	default:
		spec.Kind = in.Source
		if in.Config != nil && !json.Valid(in.Config) {
			return Spec{}, ErrConfigInvalidJSON
		}
		spec.Config = in.Config
		return spec, nil
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return Spec{}, err
	}
	spec.Config = raw
	return spec, nil
}
