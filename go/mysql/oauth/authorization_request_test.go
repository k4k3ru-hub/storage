package oauth

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

type authorizationRequestExecutorStub struct {
	query  string
	args   []any
	result sql.Result
	err    error
}

// Exec executes the authorization request test statement.
//
// Version:
//   - 2026-08-20: Added.
func (e *authorizationRequestExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}

// ExecContext records the authorization request test statement.
//
// Version:
//   - 2026-08-20: Added.
func (e *authorizationRequestExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	return e.result, e.err
}

// Query rejects unexpected test queries.
//
// Version:
//   - 2026-08-20: Added.
func (*authorizationRequestExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

// QueryContext rejects unexpected test queries.
//
// Version:
//   - 2026-08-20: Added.
func (*authorizationRequestExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}

// QueryRow returns an empty test row.
//
// Version:
//   - 2026-08-20: Added.
func (*authorizationRequestExecutorStub) QueryRow(string, ...any) *sql.Row { return &sql.Row{} }

// QueryRowContext returns an empty test row.
//
// Version:
//   - 2026-08-20: Added.
func (*authorizationRequestExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return &sql.Row{}
}

type authorizationRequestResult int64

// LastInsertId returns an unused test insert ID.
//
// Version:
//   - 2026-08-20: Added.
func (authorizationRequestResult) LastInsertId() (int64, error) { return 0, nil }

// RowsAffected returns the test affected-row count.
//
// Version:
//   - 2026-08-20: Added.
func (r authorizationRequestResult) RowsAffected() (int64, error) { return int64(r), nil }

func TestAuthorizationRequestStoreCreateTableContract(t *testing.T) {
	store, err := NewAuthorizationRequestStore(DefaultAuthorizationRequestTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &authorizationRequestExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"UNIQUE KEY uq_oauth_authorization_request_hash (request_hash)", "KEY idx_oauth_authorization_request_status_expires (status, expires_at)", "code_challenge VARCHAR(43)"} {
		if !strings.Contains(executor.query, expected) {
			t.Fatalf("CreateTable query does not contain %q", expected)
		}
	}
}

func TestAuthorizationRequestInsert(t *testing.T) {
	store, err := NewAuthorizationRequestStore(DefaultAuthorizationRequestTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &authorizationRequestExecutorStub{}
	params := validAuthorizationRequestInsertParams()
	params.ID = 0
	if err := store.Insert(context.Background(), executor, params); err != nil {
		t.Fatal(err)
	}
	if params.ID == 0 {
		t.Fatal("expected generated internal ID")
	}
	if !strings.Contains(executor.query, "INSERT INTO oauth_authorization_requests") {
		t.Fatalf("unexpected query: %s", executor.query)
	}
}

func TestAuthorizationRequestStateValidation(t *testing.T) {
	params := validAuthorizationRequestInsertParams()
	now := params.CreatedAt
	params.Status = AuthorizationRequestStatusAuthenticated
	params.Subject = stringPointer("123")
	params.OTPRequestedAt = timePointer(now)
	params.AuthenticatedAt = timePointer(now)
	if err := params.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	params.ApprovedAt = timePointer(now)
	if err := params.Validate(); err == nil {
		t.Fatal("expected invalid authenticated state")
	}
}

func TestAuthorizationRequestTransitionsRequireOneRow(t *testing.T) {
	store, err := NewAuthorizationRequestStore(DefaultAuthorizationRequestTableName)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.MarkOTPRequested(context.Background(), &authorizationRequestExecutorStub{result: authorizationRequestResult(1)}, 1, now); err != nil {
		t.Fatalf("unexpected transition error: %v", err)
	}
	if err := store.Approve(context.Background(), &authorizationRequestExecutorStub{result: authorizationRequestResult(0)}, 1, now); err == nil {
		t.Fatal("expected rejected transition")
	}
}

func validAuthorizationRequestInsertParams() *AuthorizationRequestInsertParams {
	now := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	return &AuthorizationRequestInsertParams{ID: 1, RequestHash: strings.Repeat("a", 64), Status: AuthorizationRequestStatusPrepared, ClientID: "k4k3ru_client_1", RedirectURI: "https://chatgpt.com/connector_platform_oauth_redirect", State: "state", Scopes: []string{}, Resources: []string{"https://mcp.k4k3ru.com"}, CodeChallenge: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", CodeChallengeMethod: CodeChallengeMethodS256, ExpiresAt: now.Add(10 * time.Minute), CreatedAt: now}
}

func stringPointer(value string) *string     { return &value }
func timePointer(value time.Time) *time.Time { return &value }
