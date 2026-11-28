// tail-mode for Pi: registers the tools `tail-mode tool --list` advertises (the MCP tool set) and runs each
// through `tail-mode tool <name> - --client=pi`. The only Pi logic is the wake mechanism: when a result
// carries a follower, run it and turn each jsonl digest into a message that starts a turn.
// Type-check: see README.md ("Develop").

import { execFile, execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const BIN = process.env.TAIL_MODE_BIN || "tail-mode";

type Spec = { name: string; description: string; inputSchema: object };
type Follower = { job: string; argv: string[] };

const result = (text: string, isError = false) => ({ content: [{ type: "text" as const, text }], details: {}, isError });

export default function tailMode(pi: ExtensionAPI): void {
  const followers = new Map<string, ChildProcess>(); // one per job, like a T3 notifier

  const wake = (content: string, details: unknown) =>
    pi.sendMessage({ customType: "tail-mode-digest", content, display: true, details }, { deliverAs: "followUp", triggerTurn: true });

  // follow runs the follower the tool asked for, replacing this job's earlier one. It exits by itself
  // after the final digest.
  const follow = ({ job, argv }: Follower) => {
    followers.get(job)?.kill("SIGTERM");
    const child = spawn(BIN, argv, { stdio: ["ignore", "pipe", "pipe"] });
    followers.set(job, child);
    let stderr = "";
    child.stderr!.on("data", (b: Buffer) => { stderr = (stderr + b.toString()).slice(-2048); });
    createInterface({ input: child.stdout! }).on("line", (line) => {
      try {
        const d = JSON.parse(line) as { text: string };
        wake(d.text, d);
      } catch { /* not a digest */ }
    });
    child.on("error", (e) => wake(`[tail-mode] follower for ${job} did not start: ${e.message}`, { job }));
    child.on("exit", (code) => {
      if (followers.get(job) === child) followers.delete(job);
      if (code) wake(`[tail-mode] follower for ${job} failed (exit ${code}): ${stderr.trim()}`, { job, code });
    });
  };

  const tools = JSON.parse(execFileSync(BIN, ["tool", "--list"], { encoding: "utf8" })) as Spec[];
  for (const t of tools) {
    pi.registerTool({
      name: t.name,
      label: t.name.replace("_", " "),
      description: t.description,
      parameters: Type.Unsafe<Record<string, unknown>>(t.inputSchema),
      execute: (_id, params, signal) => new Promise<ReturnType<typeof result>>((resolve) => {
        const child = execFile(BIN, ["tool", t.name, "-", "--client=pi"], { signal, maxBuffer: 16 << 20 }, (err, out, errOut) => {
          if (err) return resolve(result(out.trim() || errOut.trim() || err.message, true));
          const r = JSON.parse(out) as { text: string; follow?: Follower };
          if (r.follow) follow(r.follow);
          resolve(result(r.text));
        });
        child.stdin!.end(JSON.stringify(params));
      }),
    });
  }

  pi.on("session_shutdown", async () => {
    for (const c of followers.values()) c.kill("SIGTERM");
    followers.clear();
  });
}
