package oauth

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type clientExecutorStub struct {
	query string
	args  []any
	err   error
}

func (e *clientExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}

func (e *clientExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query = query
	e.args = args
	return nil, e.err
}

func (*clientExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

func (*clientExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}

func (*clientExecutorStub) QueryRow(string, ...any) *sql.Row { return &sql.Row{} }

func (*clientExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return &sql.Row{}
}

func validClientInsertParams() *ClientInsertParams {
	secretHash := "sha256:client-secret-hash"
	return &ClientInsertParams{
		ID:                      1,
		Status:                  ClientStatusActive,
		ClientID:                "chatgpt-production",
		SecretHash:              &secretHash,
		Name:                    "ChatGPT",
		RedirectURIs:            []string{"https://chatgpt.com/oauth/callback"},
		GrantTypes:              []GrantType{GrantTypeAuthorizationCode, GrantTypeRefreshToken},
		ResponseTypes:           []ResponseType{ResponseTypeCode},
		Scopes:                  []string{"mcp.read", "mcp.write"},
		TokenEndpointAuthMethod: TokenEndpointAuthMethodClientSecretBasic,
		CreatedAt:               time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC),
	}
}

func TestClientStoreCreateTableContract(t *testing.T) {
	store, err := NewClientStore(DefaultClientTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &clientExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	checks := []string{
		"client_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
		"client_secret_hash VARCHAR(1024) CHARACTER SET ascii COLLATE ascii_bin NULL",
		"redirect_uris JSON NOT NULL",
		"grant_types JSON NOT NULL",
		"response_types JSON NOT NULL",
		"scopes JSON NOT NULL",
		"UNIQUE KEY uq_oauth_client_client_id (client_id)",
		"DATETIME(6)",
	}
	for _, check := range checks {
		if !strings.Contains(executor.query, check) {
			t.Fatalf("CreateTable() query does not contain %q: %s", check, executor.query)
		}
	}
}

func TestClientInsertEncodesSortedCollections(t *testing.T) {
	store, err := NewClientStore(DefaultClientTableName)
	if err != nil {
		t.Fatal(err)
	}
	params := validClientInsertParams()
	params.RedirectURIs = []string{"https://example.com/z", "https://example.com/a"}
	params.Scopes = []string{"write", "read"}
	executor := &clientExecutorStub{}
	if err := store.Insert(context.Background(), executor, params); err != nil {
		t.Fatal(err)
	}
	if got, want := string(executor.args[5].([]byte)), `["https://example.com/a","https://example.com/z"]`; got != want {
		t.Fatalf("redirect_uris = %s, want %s", got, want)
	}
	if got, want := string(executor.args[8].([]byte)), `["read","write"]`; got != want {
		t.Fatalf("scopes = %s, want %s", got, want)
	}
}

func TestClientInsertRejectsNilParams(t *testing.T) {
	store, err := NewClientStore(DefaultClientTableName)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Insert(context.Background(), &sql.DB{}, nil)
	if err == nil || !strings.Contains(err.Error(), "client_insert_params=null") {
		t.Fatalf("Insert() error = %v, want client_insert_params=null error", err)
	}
}

func TestClientInsertNormalizesDuplicateKey(t *testing.T) {
	store, err := NewClientStore(DefaultClientTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &clientExecutorStub{err: &mysql.MySQLError{Number: 1062, Message: "duplicate entry"}}
	err = store.Insert(context.Background(), executor, validClientInsertParams())
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Insert() error = %v, want ErrDuplicateKey", err)
	}
}

func TestClientInsertParamsValidatePublicClient(t *testing.T) {
	params := validClientInsertParams()
	params.SecretHash = nil
	params.TokenEndpointAuthMethod = TokenEndpointAuthMethodNone
	if err := params.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestClientInsertParamsRejectsSecretForPublicClient(t *testing.T) {
	params := validClientInsertParams()
	params.TokenEndpointAuthMethod = TokenEndpointAuthMethodNone
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "secret_hash=invalid") {
		t.Fatalf("Validate() error = %v, want secret_hash=invalid error", err)
	}
}

func TestClientInsertParamsRejectsDuplicateRedirectURI(t *testing.T) {
	params := validClientInsertParams()
	params.RedirectURIs = []string{"https://example.com/callback", "https://example.com/callback"}
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "redirect_uris=invalid") {
		t.Fatalf("Validate() error = %v, want redirect_uris=invalid error", err)
	}
}

func TestClientInsertParamsRejectsRedirectURIFragment(t *testing.T) {
	params := validClientInsertParams()
	params.RedirectURIs = []string{"https://example.com/callback#fragment"}
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "redirect_uri=invalid") {
		t.Fatalf("Validate() error = %v, want redirect_uri=invalid error", err)
	}
}

func TestClientCollectionsRoundTrip(t *testing.T) {
	params := validClientInsertParams()
	redirectURIs, grantTypes, responseTypes, scopes, err := encodeClientCollections(
		params.RedirectURIs, params.GrantTypes, params.ResponseTypes, params.Scopes,
	)
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{}
	if err := decodeClientCollections(client, redirectURIs, grantTypes, responseTypes, scopes); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(client.RedirectURIs, params.RedirectURIs) ||
		!reflect.DeepEqual(client.GrantTypes, params.GrantTypes) ||
		!reflect.DeepEqual(client.ResponseTypes, params.ResponseTypes) ||
		!reflect.DeepEqual(client.Scopes, params.Scopes) {
		t.Fatalf("decoded collections = %#v", client)
	}
}

func TestClientCollectionsRejectInvalidJSON(t *testing.T) {
	client := &Client{}
	err := decodeClientCollections(client, []byte("{"), []byte("[]"), []byte("[]"), []byte("[]"))
	if err == nil || !strings.Contains(err.Error(), "failed to decode client redirect URIs") {
		t.Fatalf("decodeClientCollections() error = %v, want redirect URI decode error", err)
	}
}

func TestClientStoreRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewClientStore("oauth_clients; DROP TABLE oauth_clients"); err == nil {
		t.Fatal("NewClientStore() accepted an unsafe table name")
	}
}

func TestClientStatusRejectsUnknownDatabaseValue(t *testing.T) {
	var status ClientStatus
	err := status.Scan(uint8(255))
	if err == nil || !strings.Contains(err.Error(), "client_status=invalid") {
		t.Fatalf("Scan() error = %v, want client_status=invalid error", err)
	}
}
