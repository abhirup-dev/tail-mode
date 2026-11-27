package transport

import (
	"context"
	"encoding/base64"
	"fmt"
	"os/exec"
	"strings"
	"unicode"
)

func init() {
	Register("ssh", func(host string) (Transport, error) {
		if err := checkHost(host); err != nil {
			return nil, err
		}
		return ssh{host}, nil
	})
}

// checkHost rejects hosts that ssh could parse as options or that would split into several words.
func checkHost(h string) error {
	switch {
	case h == "":
		return fmt.Errorf("ssh transport needs a host")
	case strings.HasPrefix(h, "-"):
		return fmt.Errorf("invalid ssh host %q: starts with '-'", h)
	case strings.IndexFunc(h, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0:
		return fmt.Errorf("invalid ssh host %q: contains whitespace or control characters", h)
	}
	return nil
}

// ssh runs argv on host through the system ssh, so ~/.ssh/config, agents, ProxyJump and ControlMaster
// just work.
//
// The remote command stops when its stdin reaches EOF, so the caller must keep the command's stdin open
// (and unwritten) for as long as it should run; see remoteWrapper.
type ssh struct{ host string }

var sshOpts = []string{
	"-o", "BatchMode=yes", // never hang on a password or host-key prompt
	"-o", "ConnectTimeout=10",
	"-o", "ServerAliveInterval=15",
	"-o", "ServerAliveCountMax=3",
	"-T",
}

func (s ssh) Name() string   { return "ssh" }
func (s ssh) Target() string { return s.host }
func (s ssh) Command(ctx context.Context, argv ...string) *exec.Cmd {
	args := append(append([]string{}, sshOpts...), "--", s.host, remoteCommand(argv))
	return exec.CommandContext(ctx, "ssh", args...)
}

// remoteCommand is the string the remote login shell runs. ssh hands it to the user's login shell, which
// may be sh, bash, zsh, fish, csh or tcsh, so it is login-shell neutral: the real script (argv as
// positional parameters, then the wrapper) is base64-encoded and the login shell only sees
//
//	exec sh -c 's=$(printf %s <b64> | base64 -d) || exit 127; eval "$s"'
//
// one line, and only base64-alphabet characters between fixed syntax inside the single quotes. sh keeps
// the ssh channel as its stdin (the command substitution has its own), so the wrapper's stdin-EOF
// watchdog is unchanged. A failed decode exits 127 instead of looking like a clean, silent exit.
func remoteCommand(argv []string) string {
	return "exec sh -c 's=$(printf %s " + base64.StdEncoding.EncodeToString([]byte(remoteScript(argv))) +
		` | base64 -d) || exit 127; eval "$s"'`
}

// remoteScript is the decoded script: set the positional parameters to argv, then run the wrapper.
func remoteScript(argv []string) string {
	q := make([]string, len(argv))
	for i, a := range argv {
		q[i] = Quote(a)
	}
	return "set -- " + strings.Join(q, " ") + "\n" + remoteWrapper + "\n"
}

// remoteWrapper runs "$@" so that it cannot outlive the stream. Without a tty sshd never signals the
// remote command, and with ControlMaster there is no per-session process to watch, so cleanup keys off
// stdin EOF, which comes when the local ssh, the worker holding its stdin, or the connection goes away:
//
//   - "$@" runs with stdin /dev/null in its own process group (setsid, else `set -m`, which bash and
//     ksh honour without a tty; dash and zsh refuse it, and then only the leader can be signalled);
//   - a watchdog outside that group reads the saved stdin (fd 3) until EOF, then TERMs the group and,
//     if anything is left after ~3 s, KILLs it;
//   - when the leader exits by itself, leftovers in its group are reaped the same way, the watchdog is
//     stopped, and the leader's exit status is the wrapper's.
const remoteWrapper = `reap() { kill -TERM $1 2>/dev/null || return 0; for i in 1 2 3; do sleep 1; kill -0 $1 2>/dev/null || return 0; done; kill -KILL $1 2>/dev/null; }; ` +
	`exec 3<&0; ` +
	`if command -v setsid >/dev/null 2>&1; then setsid "$@" </dev/null 3<&- & p=$!; g=-$p; ` +
	`else command set -m 2>/dev/null; "$@" </dev/null 3<&- & p=$!; case $- in *m*) g=-$p; command set +m;; *) g=$p;; esac; fi; ` +
	`(while read -r l; do :; done <&3; reap $g) >/dev/null 2>&1 & w=$!; ` +
	`exec 3<&- 2>/dev/null; wait $p; rc=$?; kill $w 2>/dev/null; reap $g; exit $rc`
