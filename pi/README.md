# tail-mode for Pi

A shim over the Go tools: at load it registers what `tail-mode tool --list` prints (the MCP tool set:
names, descriptions, schemas) and runs each call as `tail-mode tool <name> - --client=pi`, so results
and errors match MCP. Its jobs are owned by the session: `TAIL_MODE_OWNER=pi:<session id>` (a T3 thread
id, if Pi runs in T3, wins). When a result turns
notifications on, it runs the `tail-mode follow … --deliver jsonl` the result names, as a child of Pi, and
wakes the session with each digest (`customType: "tail-mode-digest"`). A follower stops after the final
digest, when replaced for the same job, or at session shutdown; the jobs keep running.

## Install

Needs a `tail-mode` binary with `tail-mode tool` on `PATH` (or `TAIL_MODE_BIN`):
`go install github.com/abhirup-dev/tail-mode@latest` (or mise `go:github.com/abhirup-dev/tail-mode`). Then:

```sh
pi install git:github.com/abhirup-dev/tail-mode
```

Try a checkout without installing: `pi -e ./pi/tail-mode.ts`.

## Develop

Type-check without installing anything into `~/.pi`: in an empty scratch dir, link Pi's own dependencies
plus Pi itself, then run tsc (adjust `P` to your Pi install).

```sh
P=/opt/homebrew/Cellar/pi-coding-agent/1.6.0/libexec/lib/node_modules/@earendil-works/pi-coding-agent
mkdir -p node_modules/@earendil-works
for d in "$P"/node_modules/* "$P"/node_modules/@earendil-works/*; do
  case $d in */@earendil-works) ;; *) ln -s "$d" "node_modules/${d#"$P"/node_modules/}" ;; esac
done
ln -s "$P" node_modules/@earendil-works/pi-coding-agent
cp /path/to/tail-mode/pi/tail-mode.ts .
npx -y -p typescript tsc --strict --noEmit --module nodenext --target es2022 --skipLibCheck --types node tail-mode.ts
```
