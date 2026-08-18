package oauth

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruInternalSQLScan "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultClientTableName  = "oauth_clients"
	maxClientIDLength       = 255
	maxClientNameLength     = 128
	maxClientSecretHashSize = 1024
	maxRedirectURICount     = 16
	maxRedirectURILength    = 2048
	maxClientValueCount     = 64
	maxClientValueLength    = 128
)

var clientIDGenerator = &k4k3ruInternalGenerator.ID{}

type Client struct {
	ID                      uint64
	Status                  ClientStatus
	ClientID                string
	SecretHash              *string
	Name                    string
	RedirectURIs            []string
	GrantTypes              []GrantType
	ResponseTypes           []ResponseType
	Scopes                  []string
	TokenEndpointAuthMethod TokenEndpointAuthMethod
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type ClientStatus uint8

const (
	ClientStatusActive ClientStatus = iota + 1
	ClientStatusDisabled
)

type GrantType string

const (
	GrantTypeAuthorizationCode GrantType = "authorization_code"
	GrantTypeRefreshToken      GrantType = "refresh_token"
)

type ResponseType string

const (
	ResponseTypeCode ResponseType = "code"
)

type TokenEndpointAuthMethod string

const (
	TokenEndpointAuthMethodNone              TokenEndpointAuthMethod = "none"
	TokenEndpointAuthMethodClientSecretBasic TokenEndpointAuthMethod = "client_secret_basic"
	TokenEndpointAuthMethodClientSecretPost  TokenEndpointAuthMethod = "client_secret_post"
)

type ClientStore struct {
	tableName string
}

type ClientInsertParams struct {
	ID                      uint64
	Status                  ClientStatus
	ClientID                string
	SecretHash              *string
	Name                    string
	RedirectURIs            []string
	GrantTypes              []GrantType
	ResponseTypes           []ResponseType
	Scopes                  []string
	TokenEndpointAuthMethod TokenEndpointAuthMethod
	CreatedAt               time.Time
	Ignore                  bool
}

// GenerateClientID generates an internal client record ID.
//
// Returns:
//   - Generated internal client record ID.
//
// Version:
//   - 2026-08-18: Added.
func GenerateClientID() uint64 {
	return clientIDGenerator.Generate()
}

// NewClientStore creates a client store.
//
// Parameters:
//   - tableName: Client table name.
//
// Returns:
//   - Client store.
//   - Creation error.
//
// Version:
//   - 2026-08-18: Added.
func NewClientStore(tableName string) (*ClientStore, error) {
	operationErr := "failed to create oauth client store"
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return &ClientStore{tableName: tableName}, nil
}

// CreateTable creates the client table.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//
// Version:
//   - 2026-08-18: Added.
func (s *ClientStore) CreateTable(ctx context.Context, executor Executor) error {
	operationErr := "failed to create oauth client table"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			%s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
			%s TINYINT UNSIGNED NOT NULL COMMENT 'Status',
			%s VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'OAuth client ID',
			%s VARCHAR(1024) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT 'Client secret hash',
			%s VARCHAR(128) NOT NULL COMMENT 'Name',
			%s JSON NOT NULL COMMENT 'Redirect URIs',
			%s JSON NOT NULL COMMENT 'Grant types',
			%s JSON NOT NULL COMMENT 'Response types',
			%s JSON NOT NULL COMMENT 'Scopes',
			%s VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Token endpoint auth method',
			%s DATETIME(6) NOT NULL COMMENT 'Created at',
			%s DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'Updated at',
			PRIMARY KEY (%s),
			UNIQUE KEY uq_oauth_client_client_id (%s),
			KEY idx_oauth_client_status (%s)
		) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`,
		s.tableName, ColID, ColStatus, ColClientID, ColClientSecretHash, ColName,
		ColRedirectURIs, ColGrantTypes, ColResponseTypes, ColScopes,
		ColTokenEndpointAuthMethod, ColCreatedAt, ColUpdatedAt, ColID, ColClientID, ColStatus,
	)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Insert inserts a client.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//   - params: Client values.
//
// Version:
//   - 2026-08-18: Added.
func (s *ClientStore) Insert(ctx context.Context, executor Executor, params *ClientInsertParams) error {
	operationErr := "failed to insert oauth client"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: invalid parameter: client_insert_params=null", operationErr)
	}
	if params.ID == 0 {
		params.ID = GenerateClientID()
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	} else {
		params.CreatedAt = params.CreatedAt.UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	redirectURIs, grantTypes, responseTypes, scopes, err := encodeClientCollections(
		params.RedirectURIs, params.GrantTypes, params.ResponseTypes, params.Scopes,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	prefix := "INSERT"
	if params.Ignore {
		prefix = "INSERT IGNORE"
	}
	query := fmt.Sprintf(
		"%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
		prefix, s.tableName, ColID, ColStatus, ColClientID, ColClientSecretHash, ColName,
		ColRedirectURIs, ColGrantTypes, ColResponseTypes, ColScopes, ColTokenEndpointAuthMethod, ColCreatedAt,
	)
	_, err = executor.ExecContext(ctx, query, params.ID, params.Status, params.ClientID,
		params.SecretHash, params.Name, redirectURIs, grantTypes, responseTypes, scopes,
		params.TokenEndpointAuthMethod, params.CreatedAt)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// SelectByID selects a client by its internal record ID.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//   - id: Internal client record ID.
//
// Returns:
//   - Client, or nil when no client exists.
//   - Selection error.
//
// Version:
//   - 2026-08-18: Added.
func (s *ClientStore) SelectByID(ctx context.Context, executor Executor, id uint64) (*Client, error) {
	operationErr := "failed to select oauth client by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := ValidateClientID(id); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColID)
	return scanClient(executor.QueryRowContext(ctx, query, id), operationErr)
}

// SelectByClientID selects a client by its OAuth client ID.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//   - clientID: OAuth client ID.
//
// Returns:
//   - Client, or nil when no client exists.
//   - Selection error.
//
// Version:
//   - 2026-08-18: Added.
func (s *ClientStore) SelectByClientID(ctx context.Context, executor Executor, clientID string) (*Client, error) {
	operationErr := "failed to select oauth client by client id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := ValidateOAuthClientID(clientID); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColClientID)
	return scanClient(executor.QueryRowContext(ctx, query, clientID), operationErr)
}

// ValidateClientID validates an internal client record ID.
//
// Parameters:
//   - id: Internal client record ID.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func ValidateClientID(id uint64) error {
	if id == 0 {
		return fmt.Errorf("invalid parameter: id=empty")
	}
	return nil
}

// ValidateOAuthClientID validates an OAuth client ID.
//
// Parameters:
//   - clientID: OAuth client ID.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func ValidateOAuthClientID(clientID string) error {
	if strings.TrimSpace(clientID) == "" {
		return fmt.Errorf("invalid parameter: client_id=empty")
	}
	if len(clientID) > maxClientIDLength {
		return fmt.Errorf("invalid parameter: client_id=too_long actual_length=%d max_length=%d", len(clientID), maxClientIDLength)
	}
	return nil
}

// Validate validates client insert parameters.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func (p *ClientInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: client_insert_params=null")
	}
	if err := ValidateClientID(p.ID); err != nil {
		return err
	}
	if err := p.Status.Validate(); err != nil {
		return err
	}
	if err := ValidateOAuthClientID(p.ClientID); err != nil {
		return err
	}
	if err := validateClientSecret(p.SecretHash, p.TokenEndpointAuthMethod); err != nil {
		return err
	}
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("invalid parameter: name=empty")
	}
	if utf8.RuneCountInString(p.Name) > maxClientNameLength {
		return fmt.Errorf("invalid parameter: name=too_long max_length=%d", maxClientNameLength)
	}
	if err := validateRedirectURIs(p.RedirectURIs); err != nil {
		return err
	}
	if err := validateGrantTypes(p.GrantTypes); err != nil {
		return err
	}
	if err := validateResponseTypes(p.ResponseTypes); err != nil {
		return err
	}
	if err := validateStrings("scopes", p.Scopes, maxClientValueCount, maxClientValueLength, true); err != nil {
		return err
	}
	return p.TokenEndpointAuthMethod.Validate()
}

// IsValid reports whether the client status is valid.
//
// Version:
//   - 2026-08-18: Added.
func (s ClientStatus) IsValid() bool {
	return s == ClientStatusActive || s == ClientStatusDisabled
}

// Validate validates the client status.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func (s ClientStatus) Validate() error {
	if !s.IsValid() {
		return fmt.Errorf("invalid parameter: client_status=invalid")
	}
	return nil
}

// Value returns the client status as a driver.Value.
//
// Returns:
//   - SQL driver value.
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func (s ClientStatus) Value() (driver.Value, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return int64(s), nil
}

// Scan scans and validates a client status.
//
// Parameters:
//   - value: SQL value.
//
// Version:
//   - 2026-08-18: Added.
func (s *ClientStatus) Scan(value any) error {
	if s == nil {
		return fmt.Errorf("failed to scan oauth client status: client_status=null")
	}
	parsed, err := k4k3ruInternalSQLScan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan oauth client status: %w", err)
	}
	result := ClientStatus(parsed)
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan oauth client status: %w", err)
	}
	*s = result
	return nil
}

// IsValid reports whether the grant type is supported.
//
// Version:
//   - 2026-08-18: Added.
func (t GrantType) IsValid() bool {
	return t == GrantTypeAuthorizationCode || t == GrantTypeRefreshToken
}

// Validate validates the grant type.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func (t GrantType) Validate() error {
	if !t.IsValid() {
		return fmt.Errorf("invalid parameter: grant_type=invalid")
	}
	return nil
}

// IsValid reports whether the response type is supported.
//
// Version:
//   - 2026-08-18: Added.
func (t ResponseType) IsValid() bool {
	return t == ResponseTypeCode
}

// Validate validates the response type.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func (t ResponseType) Validate() error {
	if !t.IsValid() {
		return fmt.Errorf("invalid parameter: response_type=invalid")
	}
	return nil
}

// IsValid reports whether the token endpoint authentication method is supported.
//
// Version:
//   - 2026-08-18: Added.
func (m TokenEndpointAuthMethod) IsValid() bool {
	switch m {
	case TokenEndpointAuthMethodNone, TokenEndpointAuthMethodClientSecretBasic, TokenEndpointAuthMethodClientSecretPost:
		return true
	default:
		return false
	}
}

// Validate validates the token endpoint authentication method.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-18: Added.
func (m TokenEndpointAuthMethod) Validate() error {
	if !m.IsValid() {
		return fmt.Errorf("invalid parameter: token_endpoint_auth_method=invalid")
	}
	return nil
}

func (s *ClientStore) validateOperation(ctx context.Context, executor Executor, operationErr string) error {
	if s == nil {
		return fmt.Errorf("%s: client_store=null", operationErr)
	}
	if s.tableName == "" {
		return fmt.Errorf("%s: table_name=empty", operationErr)
	}
	if ctx == nil {
		return fmt.Errorf("%s: context=null", operationErr)
	}
	if executor == nil {
		return fmt.Errorf("%s: executor=null", operationErr)
	}
	return nil
}

func (s *ClientStore) selectClause() string {
	return fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s",
		ColID, ColStatus, ColClientID, ColClientSecretHash, ColName, ColRedirectURIs,
		ColGrantTypes, ColResponseTypes, ColScopes, ColTokenEndpointAuthMethod,
		ColCreatedAt, ColUpdatedAt, s.tableName,
	)
}

func scanClient(row *sql.Row, operationErr string) (*Client, error) {
	result := &Client{}
	var redirectURIs, grantTypes, responseTypes, scopes []byte
	if err := row.Scan(&result.ID, &result.Status, &result.ClientID, &result.SecretHash, &result.Name,
		&redirectURIs, &grantTypes, &responseTypes, &scopes, &result.TokenEndpointAuthMethod,
		&result.CreatedAt, &result.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := decodeClientCollections(result, redirectURIs, grantTypes, responseTypes, scopes); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := validateClient(result); err != nil {
		return nil, fmt.Errorf("%s: invalid stored client: %w", operationErr, err)
	}
	result.CreatedAt = result.CreatedAt.UTC()
	result.UpdatedAt = result.UpdatedAt.UTC()
	return result, nil
}

func validateClient(client *Client) error {
	if client == nil {
		return fmt.Errorf("client=null")
	}
	params := &ClientInsertParams{
		ID: client.ID, Status: client.Status, ClientID: client.ClientID, SecretHash: client.SecretHash,
		Name: client.Name, RedirectURIs: client.RedirectURIs, GrantTypes: client.GrantTypes,
		ResponseTypes: client.ResponseTypes, Scopes: client.Scopes,
		TokenEndpointAuthMethod: client.TokenEndpointAuthMethod, CreatedAt: client.CreatedAt,
	}
	return params.Validate()
}

func validateClientSecret(secretHash *string, method TokenEndpointAuthMethod) error {
	if secretHash == nil {
		if method != TokenEndpointAuthMethodNone {
			return fmt.Errorf("invalid parameter: secret_hash=null")
		}
		return nil
	}
	if *secretHash == "" {
		return fmt.Errorf("invalid parameter: secret_hash=empty")
	}
	if len(*secretHash) > maxClientSecretHashSize {
		return fmt.Errorf("invalid parameter: secret_hash=too_long actual_length=%d max_length=%d", len(*secretHash), maxClientSecretHashSize)
	}
	if method == TokenEndpointAuthMethodNone {
		return fmt.Errorf("invalid parameter: secret_hash=invalid")
	}
	return nil
}

func validateRedirectURIs(values []string) error {
	if err := validateStrings("redirect_uris", values, maxRedirectURICount, maxRedirectURILength, false); err != nil {
		return err
	}
	for _, value := range values {
		parsed, err := url.Parse(value)
		if err != nil || parsed.Scheme == "" || parsed.Fragment != "" {
			return fmt.Errorf("invalid parameter: redirect_uri=invalid")
		}
	}
	return nil
}

func validateGrantTypes(values []GrantType) error {
	if len(values) == 0 {
		return fmt.Errorf("invalid parameter: grant_types=empty")
	}
	seen := make(map[GrantType]struct{}, len(values))
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return err
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("invalid parameter: grant_types=invalid")
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateResponseTypes(values []ResponseType) error {
	if len(values) == 0 {
		return fmt.Errorf("invalid parameter: response_types=empty")
	}
	seen := make(map[ResponseType]struct{}, len(values))
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return err
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("invalid parameter: response_types=invalid")
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateStrings(parameter string, values []string, maxCount, maxLength int, allowEmptyCollection bool) error {
	if !allowEmptyCollection && len(values) == 0 {
		return fmt.Errorf("invalid parameter: %s=empty", parameter)
	}
	if len(values) > maxCount {
		return fmt.Errorf("invalid parameter: %s=too_long actual_length=%d max_length=%d", parameter, len(values), maxCount)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("invalid parameter: %s=invalid", parameter)
		}
		if len(value) > maxLength {
			return fmt.Errorf("invalid parameter: %s=too_long max_length=%d", parameter, maxLength)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("invalid parameter: %s=invalid", parameter)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func encodeClientCollections(redirectURIs []string, grantTypes []GrantType, responseTypes []ResponseType, scopes []string) ([]byte, []byte, []byte, []byte, error) {
	redirectURIs = sortedCopy(redirectURIs)
	grantTypes = sortedCopy(grantTypes)
	responseTypes = sortedCopy(responseTypes)
	scopes = sortedCopy(scopes)
	redirectJSON, err := json.Marshal(redirectURIs)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to encode client redirect URIs: %w", err)
	}
	grantJSON, err := json.Marshal(grantTypes)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to encode client grant types: %w", err)
	}
	responseJSON, err := json.Marshal(responseTypes)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to encode client response types: %w", err)
	}
	scopeJSON, err := json.Marshal(scopes)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to encode client scopes: %w", err)
	}
	return redirectJSON, grantJSON, responseJSON, scopeJSON, nil
}

func decodeClientCollections(client *Client, redirectURIs, grantTypes, responseTypes, scopes []byte) error {
	if err := json.Unmarshal(redirectURIs, &client.RedirectURIs); err != nil {
		return fmt.Errorf("failed to decode client redirect URIs: %w", err)
	}
	if err := json.Unmarshal(grantTypes, &client.GrantTypes); err != nil {
		return fmt.Errorf("failed to decode client grant types: %w", err)
	}
	if err := json.Unmarshal(responseTypes, &client.ResponseTypes); err != nil {
		return fmt.Errorf("failed to decode client response types: %w", err)
	}
	if err := json.Unmarshal(scopes, &client.Scopes); err != nil {
		return fmt.Errorf("failed to decode client scopes: %w", err)
	}
	return nil
}

func sortedCopy[S ~[]E, E ~string](values S) S {
	result := slices.Clone(values)
	slices.Sort(result)
	return result
}
