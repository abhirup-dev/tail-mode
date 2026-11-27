package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/ext/fts5"
)

// DefaultQueryTimeout bounds Query when ctx has no deadline.
const DefaultQueryTimeout = 5 * time.Second

// readPragmas may take an argument (a table or index name); any other pragma is allowed only bare.
var readPragmas = map[string]bool{
	"table_info": true, "table_xinfo": true, "table_list": true, "index_list": true,
	"index_info": true, "index_xinfo": true, "foreign_key_list": true,
}

// fileFuncs touch the filesystem or load code. Most are not compiled in; deny them anyway.
var fileFuncs = map[string]bool{
	"load_extension": true, "readfile": true, "writefile": true, "edit": true,
	"fsdir": true, "lsdir": true, "fts3_tokenizer": true,
}

// authorize allows reading only: SELECT, column reads, recursive CTEs, safe functions, read pragmas.
// Everything else (ATTACH, DETACH, writes, transactions, DDL, pragma writes) is denied.
func authorize(action sqlite3.AuthorizerActionCode, name3rd, name4th, _, _ string) sqlite3.AuthorizerReturnCode {
	switch action {
	case sqlite3.AUTH_SELECT, sqlite3.AUTH_READ, sqlite3.AUTH_RECURSIVE:
		return sqlite3.AUTH_OK
	case sqlite3.AUTH_FUNCTION:
		if !fileFuncs[strings.ToLower(name4th)] {
			return sqlite3.AUTH_OK
		}
	case sqlite3.AUTH_PRAGMA:
		if name4th == "" || readPragmas[strings.ToLower(name3rd)] {
			return sqlite3.AUTH_OK
		}
	}
	return sqlite3.AUTH_DENY
}

// Query runs one agent-supplied read-only statement on its own read-only connection and returns the
// column names and at most limit rows (≤0 → 500). It stops at ctx's deadline (DefaultQueryTimeout if
// none) by interrupting SQLite, and never steps past the row budget.
func (s *Store) Query(ctx context.Context, query string, limit int) (cols []string, rows [][]any, err error) {
	if limit <= 0 {
		limit = 500
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultQueryTimeout)
		defer cancel()
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(s.Dir, "tail-mode.db"),
		RawQuery: "mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(1)"}
	conn, err := sqlite3.OpenFlags(u.String(), sqlite3.OPEN_READONLY|sqlite3.OPEN_URI)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	_ = fts5.Register(conn) // so lines_fts is queryable; fails harmlessly if an auto-extension already did it
	if err := conn.SetAuthorizer(authorize); err != nil {
		return nil, nil, err
	}
	conn.SetInterrupt(ctx)
	defer func() {
		if err != nil && ctx.Err() != nil {
			err = fmt.Errorf("query stopped: %w", ctx.Err())
		}
	}()

	stmt, tail, err := conn.Prepare(query)
	if err != nil {
		return nil, nil, err
	}
	defer stmt.Close()
	if stmt == nil {
		return nil, nil, errors.New("empty query")
	}
	if tail != "" { // only whitespace/comments may follow: preparing them must yield no statement
		extra, _, err := conn.Prepare(tail)
		extra.Close()
		if err != nil || extra != nil {
			return nil, nil, errors.New("only one SQL statement is allowed")
		}
	}
	if !stmt.ReadOnly() {
		return nil, nil, errors.New("only read-only statements are allowed")
	}

	cols = make([]string, stmt.ColumnCount())
	for i := range cols {
		cols[i] = stmt.ColumnName(i)
	}
	rows = [][]any{}
	for len(rows) < limit && stmt.Step() {
		row := make([]any, len(cols))
		for i := range row {
			switch stmt.ColumnType(i) {
			case sqlite3.INTEGER:
				row[i] = stmt.ColumnInt64(i)
			case sqlite3.FLOAT:
				row[i] = stmt.ColumnFloat(i)
			case sqlite3.TEXT, sqlite3.BLOB:
				row[i] = stmt.ColumnText(i)
			}
		}
		rows = append(rows, row)
	}
	if err := stmt.Err(); err != nil {
		return nil, nil, err
	}
	return cols, rows, nil
}
