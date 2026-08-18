package oauth

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type refreshTokenResultStub struct {
	rowsAffected int64
	err          error
}

func (*refreshTokenResultStub) LastInsertId() (int64, error)   { return 0, nil }
func (r *refreshTokenResultStub) RowsAffected() (int64, error) { return r.rowsAffected, r.err }

type refreshTokenExecutorStub struct {
	query  string
	args   []any
	result sql.Result
	err    error
}

func (e *refreshTokenExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}
func (e *refreshTokenExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	return e.result, e.err
}
func (*refreshTokenExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}
func (*refreshTokenExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}
func (*refreshTokenExecutorStub) QueryRow(string, ...any) *sql.Row { return &sql.Row{} }
func (*refreshTokenExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return &sql.Row{}
}

func validRefreshTokenInsertParams() *RefreshTokenInsertParams {
	createdAt := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	return &RefreshTokenInsertParams{
		ID: 100, TokenHash: strings.Repeat("a", 64), ClientID: "chatgpt-production",
		Subject: "1786180518874776239", Scopes: []string{"mcp.read"},
		Resources: []string{"https://mcp.k4k3ru.com"}, ExpiresAt: createdAt.Add(24 * time.Hour),
		FamilyExpiresAt: createdAt.Add(30 * 24 * time.Hour), CreatedAt: createdAt,
	}
}

func TestRefreshTokenStoreCreateTableContract(t *testing.T) {
	store, err := NewRefreshTokenStore(DefaultRefreshTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &refreshTokenExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	checks := []string{
		"token_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
		"family_id BIGINT UNSIGNED NOT NULL",
		"parent_id BIGINT UNSIGNED NULL",
		"family_expires_at DATETIME(6) NOT NULL",
		"UNIQUE KEY uq_oauth_refresh_token_hash (token_hash)",
		"KEY idx_oauth_refresh_token_family_id (family_id)",
		"KEY idx_oauth_refresh_token_family_expires_at (family_expires_at)",
	}
	for _, check := range checks {
		if !strings.Contains(executor.query, check) {
			t.Fatalf("CreateTable() query does not contain %q", check)
		}
	}
}

func TestRefreshTokenInsertCreatesRootFamilyAndSortedCollections(t *testing.T) {
	store, err := NewRefreshTokenStore(DefaultRefreshTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	params := validRefreshTokenInsertParams()
	params.Scopes = []string{"write", "read"}
	params.Resources = []string{"https://z.example.com", "https://a.example.com"}
	executor := &refreshTokenExecutorStub{}
	if err := store.Insert(context.Background(), executor, params); err != nil {
		t.Fatal(err)
	}
	if got := executor.args[2]; got != params.ID {
		t.Fatalf("family ID = %v, want %d", got, params.ID)
	}
	if got := string(executor.args[5].([]byte)); got != `["read","write"]` {
		t.Fatalf("scopes = %s", got)
	}
	if got := string(executor.args[6].([]byte)); got != `["https://a.example.com","https://z.example.com"]` {
		t.Fatalf("resources = %s", got)
	}
}

func TestRefreshTokenInsertNormalizesDuplicateKey(t *testing.T) {
	store, err := NewRefreshTokenStore(DefaultRefreshTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &refreshTokenExecutorStub{err: &mysql.MySQLError{Number: 1062, Message: "duplicate"}}
	err = store.Insert(context.Background(), executor, validRefreshTokenInsertParams())
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Insert() error = %v, want ErrDuplicateKey", err)
	}
}

func TestRefreshTokenDeleteExpiredUsesFamilyExpiryAndLimit(t *testing.T) {
	store, err := NewRefreshTokenStore(DefaultRefreshTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &refreshTokenExecutorStub{result: &refreshTokenResultStub{rowsAffected: 25}}
	before := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)
	count, err := store.DeleteExpired(context.Background(), executor, RefreshTokenDeleteExpiredParams{Before: before, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if count != 25 {
		t.Fatalf("count = %d, want 25", count)
	}
	if !strings.Contains(executor.query, "WHERE family_expires_at < ? ORDER BY family_expires_at, id LIMIT ?") {
		t.Fatalf("query = %s", executor.query)
	}
}

func TestRefreshTokenInsertRejectsTokenExpiryAfterFamilyExpiry(t *testing.T) {
	params := validRefreshTokenInsertParams()
	params.ExpiresAt = params.FamilyExpiresAt.Add(time.Microsecond)
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "family_expires_at=out_of_range") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestRefreshTokenRotateRejectsReusedHash(t *testing.T) {
	now := time.Now().UTC()
	params := &RefreshTokenRotateParams{TokenHash: strings.Repeat("a", 64), ClientID: "chatgpt",
		Now: now, NewID: 2, NewTokenHash: strings.Repeat("a", 64), NewScopes: []string{"read"},
		NewResources: []string{"https://mcp.example.com"}, NewExpiresAt: now.Add(time.Hour)}
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "new_token_hash=invalid") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestRefreshTokenStoreRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewRefreshTokenStore("tokens; DROP TABLE tokens"); err == nil {
		t.Fatal("unsafe table name accepted")
	}
}

var refreshTokenDriverState = struct {
	sync.Mutex
	rows  [][]driver.Value
	execs []string
}{}

type refreshTokenDriver struct{}
type refreshTokenConn struct{}
type refreshTokenTx struct{}
type refreshTokenRows struct {
	rows  [][]driver.Value
	index int
}

func (refreshTokenDriver) Open(string) (driver.Conn, error) { return refreshTokenConn{}, nil }
func (refreshTokenConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare call")
}
func (refreshTokenConn) Close() error              { return nil }
func (refreshTokenConn) Begin() (driver.Tx, error) { return refreshTokenTx{}, nil }
func (refreshTokenConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return refreshTokenTx{}, nil
}
func (refreshTokenConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	refreshTokenDriverState.Lock()
	defer refreshTokenDriverState.Unlock()
	rows := append([][]driver.Value(nil), refreshTokenDriverState.rows...)
	return &refreshTokenRows{rows: rows}, nil
}
func (refreshTokenConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	refreshTokenDriverState.Lock()
	defer refreshTokenDriverState.Unlock()
	refreshTokenDriverState.execs = append(refreshTokenDriverState.execs, query)
	return driver.RowsAffected(1), nil
}
func (refreshTokenTx) Commit() error   { return nil }
func (refreshTokenTx) Rollback() error { return nil }
func (*refreshTokenRows) Columns() []string {
	return []string{"id", "token_hash", "family_id", "parent_id", "client_id", "subject", "scopes", "resources", "expires_at", "family_expires_at", "consumed_at", "revoked_at", "created_at"}
}
func (r *refreshTokenRows) Close() error { return nil }
func (r *refreshTokenRows) Next(values []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(values, r.rows[r.index])
	r.index++
	return nil
}

func init() {
	sql.Register("oauth-refresh-token-test", refreshTokenDriver{})
}

func TestRefreshTokenRotateConsumesAndInsertsChild(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	refreshTokenDriverState.Lock()
	refreshTokenDriverState.rows = [][]driver.Value{{
		uint64(100), strings.Repeat("a", 64), uint64(100), nil, "chatgpt", "123",
		[]byte(`["read"]`), []byte(`["https://mcp.example.com"]`), now.Add(time.Hour),
		now.Add(30 * 24 * time.Hour), nil, nil, now.Add(-time.Hour),
	}}
	refreshTokenDriverState.execs = nil
	refreshTokenDriverState.Unlock()
	database, err := sql.Open("oauth-refresh-token-test", "")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	store, err := NewRefreshTokenStore(DefaultRefreshTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Rotate(context.Background(), tx, &RefreshTokenRotateParams{
		TokenHash: strings.Repeat("a", 64), ClientID: "chatgpt", Now: now, NewID: 101,
		NewTokenHash: strings.Repeat("b", 64), NewScopes: []string{"read"},
		NewResources: []string{"https://mcp.example.com"}, NewExpiresAt: now.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != RefreshTokenRotateStatusSucceeded || result.NewToken == nil {
		t.Fatalf("result = %#v", result)
	}
	if result.NewToken.FamilyID != 100 || result.NewToken.ParentID == nil || *result.NewToken.ParentID != 100 {
		t.Fatalf("new token lineage = %#v", result.NewToken)
	}
	refreshTokenDriverState.Lock()
	defer refreshTokenDriverState.Unlock()
	if len(refreshTokenDriverState.execs) != 2 {
		t.Fatalf("exec count = %d, want 2", len(refreshTokenDriverState.execs))
	}
	if !strings.Contains(refreshTokenDriverState.execs[0], "SET consumed_at=?") {
		t.Fatalf("consume query = %s", refreshTokenDriverState.execs[0])
	}
	if !strings.HasPrefix(refreshTokenDriverState.execs[1], "INSERT INTO oauth_refresh_tokens") {
		t.Fatalf("insert query = %s", refreshTokenDriverState.execs[1])
	}
}

func TestRefreshTokenRotateReuseRevokesFamily(t *testing.T) {
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	consumedAt := now.Add(-time.Minute)
	refreshTokenDriverState.Lock()
	refreshTokenDriverState.rows = [][]driver.Value{{
		uint64(100), strings.Repeat("a", 64), uint64(100), nil, "chatgpt", "123",
		[]byte(`["read"]`), []byte(`["https://mcp.example.com"]`), now.Add(time.Hour),
		now.Add(30 * 24 * time.Hour), consumedAt, nil, now.Add(-time.Hour),
	}}
	refreshTokenDriverState.execs = nil
	refreshTokenDriverState.Unlock()
	database, err := sql.Open("oauth-refresh-token-test", "")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	store, err := NewRefreshTokenStore(DefaultRefreshTokenTableName)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Rotate(context.Background(), tx, &RefreshTokenRotateParams{
		TokenHash: strings.Repeat("a", 64), ClientID: "chatgpt", Now: now, NewID: 101,
		NewTokenHash: strings.Repeat("b", 64), NewScopes: []string{"read"},
		NewResources: []string{"https://mcp.example.com"}, NewExpiresAt: now.Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != RefreshTokenRotateStatusAlreadyConsumed || !result.FamilyRevoked {
		t.Fatalf("result = %#v", result)
	}
	refreshTokenDriverState.Lock()
	defer refreshTokenDriverState.Unlock()
	if len(refreshTokenDriverState.execs) != 1 || !strings.Contains(refreshTokenDriverState.execs[0], "WHERE family_id=?") {
		t.Fatalf("execs = %#v", refreshTokenDriverState.execs)
	}
}
