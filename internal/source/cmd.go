package source

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/abhirupdas/tail-mode/internal/transport"
)

// CmdConfig is the "cmd" adapter's config.
type CmdConfig struct {
	Cmd string `json:"cmd"`
}

func init() {
	Register("cmd", func(raw json.RawMessage) (Source, error) {
		var c CmdConfig
		if err := decode("cmd", raw, &c); err != nil {
			return nil, err
		}
		if c.Cmd == "" {
			return nil, fmt.Errorf("cmd source needs a command")
		}
		return command{c}, nil
	})
}

type command struct{ CmdConfig }

func (c command) Stream(ctx context.Context, t transport.Transport, _ bool, emit func([]string)) error {
	return stream(ctx, t, []string{"bash", "-c", c.Cmd}, emit)
}

func (c command) Describe() string       { return c.Cmd }
func (c command) RestartByDefault() bool { return false }
