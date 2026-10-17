// Package testdb is a minimal in-memory database/sql driver for unit tests.
// It records every statement the code under test runs and answers each one
// from a handler function, so SQL paths (including MySQL error numbers such
// as 1054 "unknown column") can be exercised without a MySQL server.
package testdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

// Call is one statement the code ran.
type Call struct {
	Query   string
	Args    []any
	IsQuery bool // true for Query/QueryRow, false for Exec
}

// Result is the handler's answer to a Call.
type Result struct {
	Err  error
	Cols []string
	Rows [][]driver.Value
	// NoRowsAffected makes an Exec report 0 rows affected (default 1).
	NoRowsAffected bool
}

// Handler answers a statement. A zero Result is an empty success.
type Handler func(c Call) Result

// DB is the connection pool plus the recording.
type DB struct {
	*sql.DB
	mu      sync.Mutex
	calls   []Call
	handler Handler
}

var seq atomic.Int64

type drv struct{ db *DB }

func (d drv) Open(string) (driver.Conn, error) { return &conn{db: d.db}, nil }

// New opens a database whose statements are answered by h (nil = all succeed).
func New(h Handler) *DB {
	d := &DB{handler: h}
	name := fmt.Sprintf("testdb-%d", seq.Add(1))
	sql.Register(name, drv{db: d})
	db, err := sql.Open(name, "")
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1)
	d.DB = db
	return d
}

// Calls returns a copy of the recorded statements.
func (d *DB) Calls() []Call {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]Call(nil), d.calls...)
}

// Reset forgets the recording.
func (d *DB) Reset() {
	d.mu.Lock()
	d.calls = nil
	d.mu.Unlock()
}

// Norm collapses whitespace so tests can match on SQL text.
func Norm(q string) string { return strings.Join(strings.Fields(q), " ") }

func (d *DB) run(query string, args []driver.NamedValue, isQuery bool) Result {
	c := Call{Query: Norm(query), IsQuery: isQuery}
	for _, a := range args {
		c.Args = append(c.Args, a.Value)
	}
	d.mu.Lock()
	d.calls = append(d.calls, c)
	h := d.handler
	d.mu.Unlock()
	if h == nil {
		return Result{}
	}
	return h(c)
}

type conn struct{ db *DB }

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("testdb: Prepare unsupported")
}
func (c *conn) Close() error              { return nil }
func (c *conn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("testdb: Begin unsupported") }

func (c *conn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	r := c.db.run(q, args, false)
	if r.Err != nil {
		return nil, r.Err
	}
	if r.NoRowsAffected {
		return driver.RowsAffected(0), nil
	}
	return driver.RowsAffected(1), nil
}

func (c *conn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	r := c.db.run(q, args, true)
	if r.Err != nil {
		return nil, r.Err
	}
	return &rows{cols: r.Cols, data: r.Rows}, nil
}

type rows struct {
	cols []string
	data [][]driver.Value
	i    int
}

func (r *rows) Columns() []string { return r.cols }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.i >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.i])
	r.i++
	return nil
}
