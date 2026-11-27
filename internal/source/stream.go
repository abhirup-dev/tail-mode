package source

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/abhirupdas/tail-mode/internal/transport"
)

const (
	maxLine    = 1 << 20 // longer lines are cut here; the rest of the line is dropped
	batchLines = 500
	batchEvery = 200 * time.Millisecond
	drainAfter = 2 * time.Second // how long to keep reading after the leader exits
)

// stream runs argv on t in its own process group, merges stdout+stderr, and emits lines in batches.
//
// If the transport leaves Stdin nil, the command's stdin is a pipe that is never written to and stays open
// until stream returns: remote commands (see transport ssh) stop on its EOF, which also comes if this
// process dies. Local commands get an immediate EOF from their transport.
//
// The stream ends when the leader exits, not at pipe EOF, so a backgrounded descendant holding stdout
// cannot keep it alive: output is drained for up to drainAfter, then the group is killed and the leader's
// status returned (an error wrapping *exec.ExitError if non-zero). ctx cancel kills the group and
// returns ctx.Err().
func stream(ctx context.Context, t transport.Transport, argv []string, emit func([]string)) error {
	inR, inW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer inW.Close()
	outR, outW, err := os.Pipe()
	if err != nil {
		inR.Close()
		return err
	}
	defer outR.Close()

	cmd := t.Command(ctx, argv...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return killGroup(cmd.Process.Pid) }
	if cmd.Stdin == nil { // the transport wants a held-open stdin (ssh: remote cleanup keys off its EOF)
		cmd.Stdin = inR
	}
	cmd.Stdout, cmd.Stderr = outW, outW
	err = cmd.Start()
	inR.Close()
	outW.Close()
	if err != nil {
		return err
	}
	pgid := cmd.Process.Pid

	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	lines := make(chan string, batchLines)
	var readErr error
	go func() { readErr = readLines(outR, lines); close(lines) }()

	tick := time.NewTicker(batchEvery)
	defer tick.Stop()
	var batch []string
	flush := func() {
		if len(batch) > 0 {
			emit(batch)
			batch = nil
		}
	}
	var (
		waitErr              error
		readDone, leaderDone bool
		readFailed           bool
		drain                <-chan time.Time
		done                 = ctx.Done()
		stopReading          = func() { killGroup(pgid); outR.Close() } // Close unblocks a pending Read
	)
	for !readDone || !leaderDone {
		select {
		case l, ok := <-lines:
			if !ok {
				readDone, lines = true, nil
				if readErr != nil && !errors.Is(readErr, os.ErrClosed) {
					readFailed = true
					killGroup(pgid)
				}
				continue
			}
			if batch = append(batch, l); len(batch) >= batchLines {
				flush()
			}
		case <-tick.C:
			flush()
		case waitErr = <-exited:
			leaderDone, exited = true, nil
			drain = time.After(drainAfter)
		case <-drain:
			drain = nil
			stopReading()
		case <-done:
			done = nil
			stopReading()
		}
	}
	flush()
	killGroup(pgid) // leftovers that closed their output but outlived the leader

	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case readFailed:
		return fmt.Errorf("read output: %w", readErr)
	}
	var ee *exec.ExitError
	if errors.As(waitErr, &ee) {
		return fmt.Errorf("exit %d: %w", ee.ExitCode(), ee)
	}
	return waitErr
}

func killGroup(pgid int) error { return syscall.Kill(-pgid, syscall.SIGKILL) }

// readLines sends each line of r (without "\n" or "\r\n") to out. Lines longer than maxLine are cut at
// maxLine bytes and the rest of the line is discarded. Returns nil at EOF, else the read error.
func readLines(r io.Reader, out chan<- string) error {
	br := bufio.NewReaderSize(r, 64<<10)
	var line []byte
	cut := false
	for {
		chunk, err := br.ReadSlice('\n')
		if !cut {
			if room := maxLine - len(line); len(chunk) > room {
				line, cut = append(line, chunk[:room]...), true
			} else {
				line = append(line, chunk...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == nil || len(line) > 0 {
			if !cut {
				line = bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
			}
			out <- string(line)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		line, cut = line[:0], false
	}
}
