package tools

import (
	"fmt"
	"strings"

	toon "github.com/toon-format/toon-go"

	"github.com/abhirup-dev/tail-mode/internal/store"
)

// kv renders ordered key/value pairs as a TOON object.
func kv(fields ...toon.Field) string {
	s, err := toon.MarshalString(toon.NewObject(fields...))
	if err != nil {
		return fmt.Sprintf("error: encoding output: %v", err)
	}
	return s + "\n"
}

func f(k string, v any) toon.Field { return toon.Field{Key: k, Value: v} }

// helpBlock renders 1–3 next steps (omitted when empty).
func helpBlock(lines ...string) string {
	if len(lines) == 0 {
		return ""
	}
	return fmt.Sprintf("help[%d]:\n  %s\n", len(lines), strings.Join(lines, "\n  "))
}

// table is a TOON tabular array split into its header and row lines, so render can cut rows to the
// budget and re-count the header.
type table struct {
	key  string
	head string // e.g. "{id,time,text}:" (after the [N])
	rows []string
}

func newTable(key string, rows any, n int) table {
	s, err := toon.MarshalString(toon.NewObject(f(key, rows)))
	if err != nil || n == 0 {
		return table{key: key}
	}
	ls := strings.Split(s, "\n")
	prefix := fmt.Sprintf("%s[%d]", key, n)
	t := table{key: key, head: strings.TrimPrefix(ls[0], prefix), rows: ls[1:]}
	if !strings.HasPrefix(ls[0], prefix) { // key needed quoting: never for ours
		t.head = ls[0]
	}
	return t
}

func (t table) header(shown int) string { return fmt.Sprintf("%s[%d]%s", t.key, shown, t.head) }

// render puts head first (it starts with the data note where log text follows), then the table's rows
// until the budget, then the footer for the rows actually shown. The footer is never cut.
func render(head string, t table, footer func(shown int) string) string {
	var b strings.Builder
	b.WriteString(head)
	var body strings.Builder
	shown := 0
	room0 := maxOut - footerRoom - b.Len() - len(t.header(len(t.rows))) - 1
	for _, r := range t.rows {
		room := room0 - body.Len() - 1
		if len(r) > room {
			if shown == 0 && room > 0 { // a single oversized row: show what fits rather than nothing
				body.WriteString(store.Clip(r, room))
				body.WriteByte('\n')
				shown++
			}
			break
		}
		body.WriteString(r)
		body.WriteByte('\n')
		shown++
	}
	if shown > 0 {
		b.WriteString(t.header(shown))
		b.WriteByte('\n')
		b.WriteString(body.String())
	}
	b.WriteString(footer(shown))
	return b.String()
}

func plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// lineTable is lines as {id,time,text} rows, or {id,job,time,text} with withJob.
func lineTable(key string, ls []store.Line, withJob bool) table {
	rows := make([]toon.Object, len(ls))
	for i, l := range ls {
		fs := []toon.Field{f("id", l.ID)}
		if withJob {
			fs = append(fs, f("job", l.Job))
		}
		rows[i] = toon.NewObject(append(fs, f("time", store.Clock(l.TS)), f("text", l.Text))...)
	}
	return newTable(key, rows, len(rows))
}
