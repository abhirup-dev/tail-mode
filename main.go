// Command tail-mode tails local/remote logs and commands for coding agents. See SPEC.md.
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/abhirup-dev/tail-mode/internal/cli"
)

func main() {
	if err := cli.Run(os.Args[1:]); err != nil {
		var ee cli.ExitError
		if errors.As(err, &ee) {
			os.Exit(ee.Code)
		}
		fmt.Fprintf(os.Stderr, "tail-mode: %v\n", err)
		os.Exit(1)
	}
}
