package oauth

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type accessTokenResultStub struct {
	rowsAffected int64
	err          error
}

func (*accessTokenResultStub) LastInsertId() (int64, error)   { return 0, nil }
func (r *accessTokenResultStub) RowsAffected() (int64, error) { return r.rowsAffected, r.err }

type accessTokenExecutorStub struct {
	query  string
	args   []any
	result sql.Result
	err    error
}

func (e *accessTokenExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}
func (e *accessTokenExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	return e.result, e.err
}
func (*accessTokenExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}
func (*accessTokenExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}
func (*accessTokenExecutorStub) QueryRow(string, ...any) *sql.Row { return &sql.Row{} }
func (*accessTokenExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return &sql.Row{}
}

func validAccessTokenInsertParams() *AccessTokenInsertParams {
	createdAt := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	return &AccessTokenInsertParams{ID: 1, TokenHash: strings.Repeat("a", 64),
		ClientID: "chatgpt-production", Subject: "1786180518874776239",
		Scopes: []string{"mcp.read"}, Resources: []string{"https://mcp.k4k3ru.com"},
		ExpiresAt: createdAt.Add(15 * time.Minute), CreatedAt: createdAt}
}

func TestAccessTokenStoreCreateTableContract(t *testing.T) {
	store, err := NewAccessTokenStore(DefaultAccessTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &accessTokenExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	for _, check := range []string{"token_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL", "resources JSON NOT NULL", "UNIQUE KEY uq_oauth_access_token_hash (token_hash)", "KEY idx_oauth_access_token_client_subject (client_id, subject)", "DATETIME(6)"} {
		if !strings.Contains(executor.query, check) {
			t.Fatalf("CreateTable() query does not contain %q", check)
		}
	}
}

func TestAccessTokenInsertEncodesSortedCollections(t *testing.T) {
	store, err := NewAccessTokenStore(DefaultAccessTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	params := validAccessTokenInsertParams()
	params.Scopes = []string{"write", "read"}
	params.Resources = []string{"https://z.example.com", "https://a.example.com"}
	executor := &accessTokenExecutorStub{}
	if err := store.Insert(context.Background(), executor, params); err != nil {
		t.Fatal(err)
	}
	if got := string(executor.args[4].([]byte)); got != `["read","write"]` {
		t.Fatalf("scopes = %s", got)
	}
	if got := string(executor.args[5].([]byte)); got != `["https://a.example.com","https://z.example.com"]` {
		t.Fatalf("resources = %s", got)
	}
}

func TestAccessTokenInsertNormalizesDuplicateKey(t *testing.T) {
	store, err := NewAccessTokenStore(DefaultAccessTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &accessTokenExecutorStub{err: &mysql.MySQLError{Number: 1062, Message: "duplicate"}}
	err = store.Insert(context.Background(), executor, validAccessTokenInsertParams())
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Insert() error = %v", err)
	}
}

func TestAccessTokenRevokeByTokenHashIsIdempotent(t *testing.T) {
	store, err := NewAccessTokenStore(DefaultAccessTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &accessTokenExecutorStub{result: &accessTokenResultStub{rowsAffected: 0}}
	revoked, err := store.RevokeByTokenHash(context.Background(), executor, strings.Repeat("a", 64), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if revoked {
		t.Fatal("RevokeByTokenHash() = true, want false")
	}
}

func TestAccessTokenRevokeByClientIDAndSubjectReturnsCount(t *testing.T) {
	store, err := NewAccessTokenStore(DefaultAccessTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &accessTokenExecutorStub{result: &accessTokenResultStub{rowsAffected: 3}}
	count, err := store.RevokeByClientIDAndSubject(context.Background(), executor, AccessTokenRevokeSubjectParams{ClientID: "chatgpt", Subject: "123", RevokedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}

func TestAccessTokenInsertRejectsExpiredAtCreation(t *testing.T) {
	params := validAccessTokenInsertParams()
	params.ExpiresAt = params.CreatedAt
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "expires_at=out_of_range") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAccessTokenStoreRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewAccessTokenStore("tokens; DROP TABLE tokens"); err == nil {
		t.Fatal("unsafe table name accepted")
	}
}
