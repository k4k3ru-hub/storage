package oauth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type authorizationCodeExecutorStub struct {
	query string
	args  []any
	err   error
}

func (e *authorizationCodeExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}

func (e *authorizationCodeExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	return nil, e.err
}

func (*authorizationCodeExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

func (*authorizationCodeExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}

func (*authorizationCodeExecutorStub) QueryRow(string, ...any) *sql.Row { return &sql.Row{} }

func (*authorizationCodeExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return &sql.Row{}
}

func validAuthorizationCodeInsertParams() *AuthorizationCodeInsertParams {
	createdAt := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	challenge := sha256.Sum256([]byte(strings.Repeat("v", 43)))
	return &AuthorizationCodeInsertParams{
		ID:                  1,
		CodeHash:            strings.Repeat("a", 64),
		ClientID:            "chatgpt-production",
		Subject:             "1786180518874776239",
		RedirectURI:         "https://chatgpt.example.com/oauth/callback",
		Scopes:              []string{"mcp.read", "mcp.write"},
		Resources:           []string{"https://mcp.k4k3ru.com"},
		CodeChallenge:       base64.RawURLEncoding.EncodeToString(challenge[:]),
		CodeChallengeMethod: CodeChallengeMethodS256,
		ExpiresAt:           createdAt.Add(5 * time.Minute),
		CreatedAt:           createdAt,
	}
}

func TestAuthorizationCodeStoreCreateTableContract(t *testing.T) {
	store, err := NewAuthorizationCodeStore(DefaultAuthorizationCodeTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &authorizationCodeExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	checks := []string{
		"code_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
		"resources JSON NOT NULL",
		"code_challenge VARCHAR(43) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
		"UNIQUE KEY uq_oauth_authorization_code_hash (code_hash)",
		"KEY idx_oauth_authorization_code_client_subject (client_id, subject)",
		"DATETIME(6)",
	}
	for _, check := range checks {
		if !strings.Contains(executor.query, check) {
			t.Fatalf("CreateTable() query does not contain %q", check)
		}
	}
}

func TestAuthorizationCodeInsertEncodesSortedCollections(t *testing.T) {
	store, err := NewAuthorizationCodeStore(DefaultAuthorizationCodeTableName)
	if err != nil {
		t.Fatal(err)
	}
	params := validAuthorizationCodeInsertParams()
	params.Scopes = []string{"write", "read"}
	params.Resources = []string{"https://z.example.com", "https://a.example.com"}
	executor := &authorizationCodeExecutorStub{}
	if err := store.Insert(context.Background(), executor, params); err != nil {
		t.Fatal(err)
	}
	if got, want := string(executor.args[5].([]byte)), `["read","write"]`; got != want {
		t.Fatalf("scopes = %s, want %s", got, want)
	}
	if got, want := string(executor.args[6].([]byte)), `["https://a.example.com","https://z.example.com"]`; got != want {
		t.Fatalf("resources = %s, want %s", got, want)
	}
}

func TestAuthorizationCodeInsertNormalizesDuplicateKey(t *testing.T) {
	store, err := NewAuthorizationCodeStore(DefaultAuthorizationCodeTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &authorizationCodeExecutorStub{err: &mysql.MySQLError{Number: 1062, Message: "duplicate"}}
	err = store.Insert(context.Background(), executor, validAuthorizationCodeInsertParams())
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Insert() error = %v, want ErrDuplicateKey", err)
	}
}

func TestAuthorizationCodeInsertRejectsDuplicateResource(t *testing.T) {
	params := validAuthorizationCodeInsertParams()
	params.Resources = []string{"https://mcp.example.com", "https://mcp.example.com"}
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "resources=invalid") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAuthorizationCodeInsertRejectsResourceFragment(t *testing.T) {
	params := validAuthorizationCodeInsertParams()
	params.Resources = []string{"https://mcp.example.com#fragment"}
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "resource=invalid") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAuthorizationCodeInsertRejectsPlainPKCE(t *testing.T) {
	params := validAuthorizationCodeInsertParams()
	params.CodeChallengeMethod = CodeChallengeMethod("plain")
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "code_challenge_method=invalid") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAuthorizationCodeInsertRejectsInvalidS256Challenge(t *testing.T) {
	params := validAuthorizationCodeInsertParams()
	params.CodeChallenge = strings.Repeat("a", 42) + "="
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "code_challenge=invalid") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestAuthorizationCodeStoreRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewAuthorizationCodeStore("codes; DROP TABLE codes"); err == nil {
		t.Fatal("unsafe table name accepted")
	}
}
