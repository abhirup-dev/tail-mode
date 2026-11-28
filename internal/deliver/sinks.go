package deliver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// EnvThread is the caller's T3 thread from $T3CODE_THREAD_ID, trimmed ("" = not in T3). Only the
// integration layer calls it; sinks and the core never read the environment.
func EnvThread() string { return strings.TrimSpace(os.Getenv("T3CODE_THREAD_ID")) }

// New builds a sink from a --deliver spec: "stdout" | "jsonl" | "t3:THREAD". The thread is always
// explicit: resolving it from the environment is the integration layer's job (EnvThread).
func New(spec string) (Sink, error) {
	switch {
	case spec == "" || spec == "stdout":
		return writerSink{w: os.Stdout}, nil
	case spec == "jsonl":
		return writerSink{w: os.Stdout, json: true}, nil
	case spec == "t3" || spec == "t3:":
		return nil, fmt.Errorf("t3 sink: no thread (use t3:THREAD)")
	case strings.HasPrefix(spec, "t3:"):
		return t3Sink{thread: spec[len("t3:"):]}, nil
	}
	return nil, fmt.Errorf("unknown sink %q (stdout|jsonl|t3:THREAD)", spec)
}

type writerSink struct {
	w    io.Writer
	json bool
}

func (s writerSink) Deliver(_ context.Context, d Digest) error {
	if s.json {
		if d.Lines == nil {
			d.Lines = []Line{}
		}
		// text lets consumers (the Pi extension) show the digest without re-implementing Text.
		return json.NewEncoder(s.w).Encode(struct {
			Digest
			Text string `json:"text"`
		}{d, Text(d)})
	}
	_, err := io.WriteString(s.w, Text(d))
	return err
}

type t3Sink struct{ thread string }

func (s t3Sink) Deliver(ctx context.Context, d Digest) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, t3Bin(), "thread", "send", s.thread, "-")
	cmd.Stdin = strings.NewReader(PushText(d)) // the thread's agent has the MCP tools: no CLI hint
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("t3 thread send: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func t3Bin() string {
	if b := os.Getenv("TAIL_MODE_T3_BIN"); b != "" {
		return b
	}
	if b, err := exec.LookPath("t3"); err == nil {
		return b
	}
	if h := os.Getenv("T3CODE_HOME"); h != "" {
		if b := filepath.Join(h, "bin", "t3"); fileExists(b) {
			return b
		}
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".t3", "bin", "t3")
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }
