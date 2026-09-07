package auth

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type webSessionExecutor struct {
	otpExecutorStub
	result sql.Result
}

func (e *webSessionExecutor) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	return e.result, e.err
}

type webSessionResult struct {
	count int64
	err   error
}

func (r webSessionResult) LastInsertId() (int64, error) { return 0, nil }
func (r webSessionResult) RowsAffected() (int64, error) { return r.count, r.err }

func validWebSessionParams() WebSessionInsertParams {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	return WebSessionInsertParams{ID: 1, SessionTokenHash: strings.Repeat("a", 64), Subject: "123", CreatedAt: now,
		LastActivityAt: now, ExpiresAt: now.Add(24 * time.Hour), AbsoluteExpiresAt: now.Add(7 * 24 * time.Hour)}
}

func newWebSessionTestStore(t *testing.T) *WebSessionStore {
	t.Helper()
	s, err := NewWebSessionStore("console_web_sessions")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWebSessionValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*WebSessionInsertParams)
		want   string
	}{
		{"empty hash", func(p *WebSessionInsertParams) { p.SessionTokenHash = "" }, "session_token_hash=empty"},
		{"raw token", func(p *WebSessionInsertParams) { p.SessionTokenHash = "private-cookie-token" }, "session_token_hash=invalid"},
		{"uppercase hash", func(p *WebSessionInsertParams) { p.SessionTokenHash = strings.Repeat("A", 64) }, "session_token_hash=invalid"},
		{"empty subject", func(p *WebSessionInsertParams) { p.Subject = " " }, "subject=empty"},
		{"long subject", func(p *WebSessionInsertParams) { p.Subject = strings.Repeat("界", 256) }, "subject=too_long"},
		{"invalid utf8", func(p *WebSessionInsertParams) { p.Subject = string([]byte{255}) }, "subject=invalid"},
		{"zero time", func(p *WebSessionInsertParams) { p.CreatedAt = time.Time{} }, "created_at=empty"},
		{"activity before creation", func(p *WebSessionInsertParams) { p.LastActivityAt = p.CreatedAt.Add(-time.Second) }, "last_activity_at=out_of_range"},
		{"expiry boundary", func(p *WebSessionInsertParams) { p.ExpiresAt = p.LastActivityAt }, "expires_at=out_of_range"},
		{"past absolute limit", func(p *WebSessionInsertParams) { p.ExpiresAt = p.AbsoluteExpiresAt.Add(time.Second) }, "expires_at=out_of_range"},
		{"precision loss", func(p *WebSessionInsertParams) { p.CreatedAt = p.CreatedAt.Add(time.Nanosecond) }, "created_at=out_of_range"},
		{"mysql year", func(p *WebSessionInsertParams) { p.CreatedAt = time.Date(999, 1, 1, 0, 0, 0, 0, time.UTC) }, "created_at=out_of_range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := validWebSessionParams()
			tc.modify(&p)
			err := p.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want %s", err, tc.want)
			}
			if strings.Contains(err.Error(), "private-cookie-token") {
				t.Fatal("raw token leaked")
			}
		})
	}
	p := validWebSessionParams()
	p.Subject = strings.Repeat("界", 255)
	p.ExpiresAt = p.AbsoluteExpiresAt
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	var missing *WebSessionInsertParams
	if err := missing.Validate(); err == nil {
		t.Fatal("nil params accepted")
	}
}

func TestWebSessionCreateAndInsert(t *testing.T) {
	s := newWebSessionTestStore(t)
	e := &webSessionExecutor{}
	if err := s.CreateTable(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"CREATE TABLE IF NOT EXISTS console_web_sessions", "session_token_hash CHAR(64)", "UNIQUE KEY uq_web_session_token_hash", "expires_at <= absolute_expires_at", "last_activity_at >= created_at", "revoked_at >= created_at"} {
		if !strings.Contains(e.query, part) {
			t.Fatalf("missing schema constraint %q", part)
		}
	}
	p := validWebSessionParams()
	p.ID = 0
	p.CreatedAt = p.CreatedAt.In(time.FixedZone("JST", 9*3600))
	original := p
	if err := s.Insert(context.Background(), e, &p); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, original) {
		t.Fatal("Insert mutated caller parameters")
	}
	if e.args[0].(uint64) == 0 || e.args[3].(time.Time).Location() != time.UTC {
		t.Fatal("ID or UTC normalization missing")
	}
	if strings.Contains(e.query, p.SessionTokenHash) {
		t.Fatal("hash interpolated into SQL")
	}
	underlying := &mysql.MySQLError{Number: 1062, Message: "duplicate"}
	e.err = underlying
	err := s.Insert(context.Background(), e, &p)
	var mysqlErr *mysql.MySQLError
	if !errors.Is(err, ErrDuplicateKey) || !errors.As(err, &mysqlErr) || mysqlErr != underlying {
		t.Fatalf("duplicate error chain lost: %v", err)
	}
}

func TestWebSessionActivityAndRevocationGuards(t *testing.T) {
	s := newWebSessionTestStore(t)
	p := validWebSessionParams()
	e := &webSessionExecutor{result: webSessionResult{count: 1}}
	now := p.CreatedAt.Add(6 * 24 * time.Hour)
	changed, err := s.Touch(context.Background(), e, WebSessionTouchParams{SessionTokenHash: p.SessionTokenHash, Now: now, IdleTimeout: 48 * time.Hour})
	if err != nil || !changed {
		t.Fatalf("Touch = %v, %v", changed, err)
	}
	for _, guard := range []string{"LEAST(absolute_expires_at, GREATEST(expires_at, ?))", "revoked_at IS NULL", "created_at <= ?", "expires_at > ?", "absolute_expires_at > ?", "last_activity_at < ?"} {
		if !strings.Contains(e.query, guard) {
			t.Fatalf("missing atomic guard %q", guard)
		}
	}
	if e.args[1] != now.Add(48*time.Hour) {
		t.Fatal("idle deadline not passed to capped SQL update")
	}
	for _, timeout := range []time.Duration{0, -time.Second, time.Nanosecond} {
		if _, err := s.Touch(context.Background(), e, WebSessionTouchParams{SessionTokenHash: p.SessionTokenHash, Now: now, IdleTimeout: timeout}); err == nil {
			t.Fatal("invalid timeout accepted")
		}
	}
	for _, count := range []int64{0, 1, 2, -1} {
		e.result = webSessionResult{count: count}
		changed, err := s.RevokeByTokenHash(context.Background(), e, p.SessionTokenHash, now)
		if count < 0 || count > 1 {
			if err == nil {
				t.Fatal("invalid affected rows accepted")
			}
			continue
		}
		if err != nil || changed != (count == 1) {
			t.Fatalf("Revoke count %d: %v, %v", count, changed, err)
		}
	}
	if !strings.Contains(e.query, "revoked_at IS NULL") || !strings.Contains(e.query, "created_at <= ?") {
		t.Fatal("revocation guard missing")
	}
	underlying := errors.New("row count unavailable")
	e.result = webSessionResult{err: underlying}
	if _, err := s.RevokeByTokenHash(context.Background(), e, p.SessionTokenHash, now); !errors.Is(err, underlying) {
		t.Fatalf("RowsAffected error lost: %v", err)
	}
	e.result = nil
	if _, err := s.RevokeByTokenHash(context.Background(), e, p.SessionTokenHash, now); err == nil {
		t.Fatal("nil result accepted")
	}
}

func TestWebSessionRejectsInvalidDependencies(t *testing.T) {
	if _, err := NewWebSessionStore("sessions; DROP TABLE sessions"); err == nil {
		t.Fatal("unsafe table accepted")
	}
	s := newWebSessionTestStore(t)
	if err := s.CreateTable(nil, &webSessionExecutor{}); err == nil {
		t.Fatal("nil context accepted")
	}
	if err := s.CreateTable(context.Background(), nil); err == nil {
		t.Fatal("nil executor accepted")
	}
	var absent *WebSessionStore
	if err := absent.CreateTable(context.Background(), &webSessionExecutor{}); err == nil {
		t.Fatal("nil store accepted")
	}
}

// A minimal database/sql driver exercises Scan and sql.ErrNoRows without adding
// a test dependency. SQL predicates are checked separately above and below.
type webSessionConnector struct {
	values []driver.Value
	query  string
	args   []driver.NamedValue
	err    error
}

func (c *webSessionConnector) Connect(context.Context) (driver.Conn, error) {
	return &webSessionConn{c}, nil
}
func (c *webSessionConnector) Driver() driver.Driver { return webSessionDriver{} }

type webSessionDriver struct{}

func (webSessionDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type webSessionConn struct{ c *webSessionConnector }

func (*webSessionConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*webSessionConn) Close() error              { return nil }
func (*webSessionConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c *webSessionConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.c.query, c.c.args = query, args
	if c.c.err != nil {
		return nil, c.c.err
	}
	return &webSessionRows{values: c.c.values}, nil
}

type webSessionRows struct{ values []driver.Value }

func (*webSessionRows) Columns() []string {
	return []string{ColID, ColSessionTokenHash, ColSubject, ColCreatedAt, ColLastActivityAt, ColExpiresAt, ColAbsoluteExpiresAt, ColRevokedAt}
}
func (*webSessionRows) Close() error { return nil }
func (r *webSessionRows) Next(dst []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(dst, r.values)
	r.values = nil
	return nil
}

func TestWebSessionSelectScanning(t *testing.T) {
	p := validWebSessionParams()
	s := newWebSessionTestStore(t)
	underlying := errors.New("query failure")
	for _, mode := range []string{"active", "missing", "corrupt", "query failure"} {
		t.Run(mode, func(t *testing.T) {
			c := &webSessionConnector{values: []driver.Value{int64(p.ID), p.SessionTokenHash, p.Subject, p.CreatedAt, p.LastActivityAt, p.ExpiresAt, p.AbsoluteExpiresAt, nil}}
			switch mode {
			case "missing":
				c.values = nil
			case "corrupt":
				c.values[5] = p.CreatedAt
			case "query failure":
				c.err = underlying
			}
			db := sql.OpenDB(c)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			got, err := s.SelectActiveByTokenHash(context.Background(), db, WebSessionSelectActiveParams{SessionTokenHash: p.SessionTokenHash, Now: p.CreatedAt})
			switch mode {
			case "active":
				if err != nil || got == nil || got.Subject != p.Subject || got.ExpiresAt != p.ExpiresAt {
					t.Fatalf("lookup = %v, %v", got, err)
				}
			case "missing":
				if err != nil || got != nil {
					t.Fatalf("missing = %v, %v", got, err)
				}
			case "corrupt":
				if err == nil {
					t.Fatal("corrupt stored expiry accepted")
				}
			case "query failure":
				if !errors.Is(err, underlying) {
					t.Fatalf("query error lost: %v", err)
				}
			}
			for _, guard := range []string{"revoked_at IS NULL", "created_at <= ?", "expires_at > ?", "absolute_expires_at > ?"} {
				if !strings.Contains(c.query, guard) {
					t.Fatalf("missing lookup guard %q", guard)
				}
			}
			if len(c.args) != 4 || c.args[0].Value != p.SessionTokenHash {
				t.Fatal("invalid bound lookup arguments")
			}
		})
	}
}
