# tail-mode for Pi

Pi extension exposing `tail_start`, `tail_stop`, `tail_jobs`, `tail_read`, `tail_search`, `tail_sql`, `tail_notify`.
Each tool shells out to `tail-mode … --json`. Notifiers (`tail_notify`, or `tail_start` with `notify_every`) run
`tail-mode follow <job> --deliver jsonl` as a child of Pi and wake the session with a digest message
(`customType: "tail-mode-digest"`). They stop when the job ends or the Pi session shuts down; the jobs themselves keep running.

## Install

Requires `tail-mode` on `PATH`, or `TAIL_MODE_BIN=/path/to/tail-mode`.

```sh
ln -s "$PWD/pi/tail-mode.ts" ~/.pi/agent/extensions/tail-mode.ts   # or cp
```

Try without installing: `pi -e ./pi/tail-mode.ts`.

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
