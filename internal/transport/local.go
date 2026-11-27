package transport

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

func init() {
	Register("local", func(target string) (Transport, error) {
		if target != "" {
			return nil, fmt.Errorf("local transport takes no target (got %q)", target)
		}
		return local{}, nil
	})
}

type local struct{}

func (local) Name() string   { return "local" }
func (local) Target() string { return "" }
func (local) Command(ctx context.Context, argv ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdin = strings.NewReader("") // local commands see EOF; only transports that leave Stdin nil get a held-open pipe
	return cmd
}
