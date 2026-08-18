package oauth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultAccessTokenTableName = "oauth_access_tokens"
	maxAccessTokenHashLength    = 128
	maxAccessTokenSubjectLength = 255
)

var accessTokenIDGenerator = &k4k3ruInternalGenerator.ID{}

type AccessToken struct {
	ID        uint64
	TokenHash string
	ClientID  string
	Subject   string
	Scopes    []string
	Resources []string
	ExpiresAt time.Time
	RevokedAt *time.Time
	CreatedAt time.Time
}

type AccessTokenStore struct {
	tableName string
}

type AccessTokenInsertParams struct {
	ID        uint64
	TokenHash string
	ClientID  string
	Subject   string
	Scopes    []string
	Resources []string
	ExpiresAt time.Time
	CreatedAt time.Time
	Ignore    bool
}

type AccessTokenSelectActiveParams struct {
	TokenHash string
	Now       time.Time
}

type AccessTokenRevokeSubjectParams struct {
	ClientID  string
	Subject   string
	RevokedAt time.Time
}

// GenerateAccessTokenID generates an access token record ID.
//
// Version:
//   - 2026-08-18: Added.
func GenerateAccessTokenID() uint64 {
	return accessTokenIDGenerator.Generate()
}

// NewAccessTokenStore creates an access token store.
//
// Parameters:
//   - tableName: Access token table name.
//
// Returns:
//   - Access token store.
//   - Creation error.
//
// Version:
//   - 2026-08-18: Added.
func NewAccessTokenStore(tableName string) (*AccessTokenStore, error) {
	operationErr := "failed to create oauth access token store"
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return &AccessTokenStore{tableName: tableName}, nil
}

// CreateTable creates the access token table.
//
// Version:
//   - 2026-08-18: Added.
func (s *AccessTokenStore) CreateTable(ctx context.Context, executor Executor) error {
	operationErr := "failed to create oauth access token table"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
		%s VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Access token hash',
		%s VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'OAuth client ID',
		%s VARCHAR(255) NOT NULL COMMENT 'Subject',
		%s JSON NOT NULL COMMENT 'Scopes',
		%s JSON NOT NULL COMMENT 'Resources',
		%s DATETIME(6) NOT NULL COMMENT 'Expires at',
		%s DATETIME(6) NULL COMMENT 'Revoked at',
		%s DATETIME(6) NOT NULL COMMENT 'Created at',
		PRIMARY KEY (%s),
		UNIQUE KEY uq_oauth_access_token_hash (%s),
		KEY idx_oauth_access_token_client_subject (%s, %s),
		KEY idx_oauth_access_token_expires_at (%s),
		KEY idx_oauth_access_token_revoked_at (%s)
	) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`, s.tableName,
		ColID, ColTokenHash, ColClientID, ColSubject, ColScopes, ColResources,
		ColExpiresAt, ColRevokedAt, ColCreatedAt, ColID, ColTokenHash,
		ColClientID, ColSubject, ColExpiresAt, ColRevokedAt)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Insert inserts an opaque access token record.
//
// Version:
//   - 2026-08-18: Added.
func (s *AccessTokenStore) Insert(ctx context.Context, executor Executor, params *AccessTokenInsertParams) error {
	operationErr := "failed to insert oauth access token"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: access_token_insert_params=null", operationErr)
	}
	if params.ID == 0 {
		params.ID = GenerateAccessTokenID()
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	} else {
		params.CreatedAt = params.CreatedAt.UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	scopes, err := encodeAuthorizationCodeStrings(params.Scopes)
	if err != nil {
		return fmt.Errorf("%s: failed to encode scopes: %w", operationErr, err)
	}
	resources, err := encodeAuthorizationCodeStrings(params.Resources)
	if err != nil {
		return fmt.Errorf("%s: failed to encode resources: %w", operationErr, err)
	}
	prefix := "INSERT"
	if params.Ignore {
		prefix = "INSERT IGNORE"
	}
	query := fmt.Sprintf("%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
		prefix, s.tableName, ColID, ColTokenHash, ColClientID, ColSubject, ColScopes,
		ColResources, ColExpiresAt, ColCreatedAt)
	_, err = executor.ExecContext(ctx, query, params.ID, params.TokenHash, params.ClientID,
		params.Subject, scopes, resources, params.ExpiresAt.UTC(), params.CreatedAt)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// SelectByID selects an access token by record ID.
//
// Version:
//   - 2026-08-18: Added.
func (s *AccessTokenStore) SelectByID(ctx context.Context, executor Executor, id uint64) (*AccessToken, error) {
	operationErr := "failed to select oauth access token by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, fmt.Errorf("%s: id=empty", operationErr)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColID)
	return scanAccessToken(executor.QueryRowContext(ctx, query, id), operationErr)
}

// SelectActiveByTokenHash selects an active opaque access token.
//
// Missing, expired, and revoked tokens all return nil without an error.
//
// Version:
//   - 2026-08-18: Added.
func (s *AccessTokenStore) SelectActiveByTokenHash(ctx context.Context, executor Executor, params AccessTokenSelectActiveParams) (*AccessToken, error) {
	operationErr := "failed to select active oauth access token"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := params.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? AND %s IS NULL AND %s > ? LIMIT 1;", ColTokenHash, ColRevokedAt, ColExpiresAt)
	return scanAccessToken(executor.QueryRowContext(ctx, query, params.TokenHash, params.Now.UTC()), operationErr)
}

// RevokeByID revokes an access token by record ID.
//
// Returns true only when this call newly revoked the token.
//
// Version:
//   - 2026-08-18: Added.
func (s *AccessTokenStore) RevokeByID(ctx context.Context, executor Executor, id uint64, revokedAt time.Time) (bool, error) {
	operationErr := "failed to revoke oauth access token by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return false, err
	}
	if id == 0 {
		return false, fmt.Errorf("%s: id=empty", operationErr)
	}
	if revokedAt.IsZero() {
		return false, fmt.Errorf("%s: revoked_at=empty", operationErr)
	}
	query := fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s IS NULL;", s.tableName, ColRevokedAt, ColID, ColRevokedAt)
	return executeAccessTokenRevoke(ctx, executor, query, operationErr, revokedAt.UTC(), id)
}

// RevokeByTokenHash revokes an access token by token hash.
//
// Returns true only when this call newly revoked the token.
//
// Version:
//   - 2026-08-18: Added.
func (s *AccessTokenStore) RevokeByTokenHash(ctx context.Context, executor Executor, tokenHash string, revokedAt time.Time) (bool, error) {
	operationErr := "failed to revoke oauth access token by token hash"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return false, err
	}
	if err := validateAccessTokenHash(tokenHash); err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	if revokedAt.IsZero() {
		return false, fmt.Errorf("%s: revoked_at=empty", operationErr)
	}
	query := fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s IS NULL;", s.tableName, ColRevokedAt, ColTokenHash, ColRevokedAt)
	return executeAccessTokenRevoke(ctx, executor, query, operationErr, revokedAt.UTC(), tokenHash)
}

// RevokeByClientIDAndSubject revokes all active access tokens for a client and subject.
//
// Returns the number of newly revoked tokens.
//
// Version:
//   - 2026-08-18: Added.
func (s *AccessTokenStore) RevokeByClientIDAndSubject(ctx context.Context, executor Executor, params AccessTokenRevokeSubjectParams) (int64, error) {
	operationErr := "failed to revoke oauth access tokens by client id and subject"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return 0, err
	}
	if err := params.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s=? AND %s IS NULL;",
		s.tableName, ColRevokedAt, ColClientID, ColSubject, ColRevokedAt)
	result, err := executor.ExecContext(ctx, query, params.RevokedAt.UTC(), params.ClientID, params.Subject)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", operationErr, err)
	}
	if result == nil {
		return 0, fmt.Errorf("%s: sql_result=null", operationErr)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: failed to read affected rows: %w", operationErr, err)
	}
	return count, nil
}

// Validate validates access token insert parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p *AccessTokenInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: access_token_insert_params=null")
	}
	if p.ID == 0 {
		return fmt.Errorf("invalid parameter: id=empty")
	}
	if err := validateAccessTokenFields(p.TokenHash, p.ClientID, p.Subject, p.Scopes, p.Resources); err != nil {
		return err
	}
	if p.ExpiresAt.IsZero() {
		return fmt.Errorf("invalid parameter: expires_at=empty")
	}
	if p.CreatedAt.IsZero() {
		return fmt.Errorf("invalid parameter: created_at=empty")
	}
	if !p.ExpiresAt.After(p.CreatedAt) {
		return fmt.Errorf("invalid parameter: expires_at=out_of_range")
	}
	return nil
}

// Validate validates active access token selection parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p AccessTokenSelectActiveParams) Validate() error {
	if err := validateAccessTokenHash(p.TokenHash); err != nil {
		return err
	}
	if p.Now.IsZero() {
		return fmt.Errorf("invalid parameter: now=empty")
	}
	return nil
}

// Validate validates subject-wide revocation parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p AccessTokenRevokeSubjectParams) Validate() error {
	if err := ValidateOAuthClientID(p.ClientID); err != nil {
		return err
	}
	if err := validateAccessTokenSubject(p.Subject); err != nil {
		return err
	}
	if p.RevokedAt.IsZero() {
		return fmt.Errorf("invalid parameter: revoked_at=empty")
	}
	return nil
}

func (s *AccessTokenStore) validateOperation(ctx context.Context, executor Executor, operationErr string) error {
	if s == nil {
		return fmt.Errorf("%s: access_token_store=null", operationErr)
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

func (s *AccessTokenStore) selectClause() string {
	return fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s",
		ColID, ColTokenHash, ColClientID, ColSubject, ColScopes, ColResources,
		ColExpiresAt, ColRevokedAt, ColCreatedAt, s.tableName)
}

func scanAccessToken(row *sql.Row, operationErr string) (*AccessToken, error) {
	result := &AccessToken{}
	var scopes, resources []byte
	if err := row.Scan(&result.ID, &result.TokenHash, &result.ClientID, &result.Subject,
		&scopes, &resources, &result.ExpiresAt, &result.RevokedAt, &result.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := json.Unmarshal(scopes, &result.Scopes); err != nil {
		return nil, fmt.Errorf("%s: failed to decode scopes: %w", operationErr, err)
	}
	if err := json.Unmarshal(resources, &result.Resources); err != nil {
		return nil, fmt.Errorf("%s: failed to decode resources: %w", operationErr, err)
	}
	if err := validateAccessToken(result); err != nil {
		return nil, fmt.Errorf("%s: invalid stored access token: %w", operationErr, err)
	}
	result.ExpiresAt = result.ExpiresAt.UTC()
	result.RevokedAt = utcTimePointer(result.RevokedAt)
	result.CreatedAt = result.CreatedAt.UTC()
	return result, nil
}

func validateAccessToken(value *AccessToken) error {
	if value == nil {
		return fmt.Errorf("access_token=null")
	}
	if value.ID == 0 {
		return fmt.Errorf("id=empty")
	}
	if err := validateAccessTokenFields(value.TokenHash, value.ClientID, value.Subject, value.Scopes, value.Resources); err != nil {
		return err
	}
	if value.ExpiresAt.IsZero() {
		return fmt.Errorf("expires_at=empty")
	}
	if value.CreatedAt.IsZero() {
		return fmt.Errorf("created_at=empty")
	}
	if !value.ExpiresAt.After(value.CreatedAt) {
		return fmt.Errorf("expires_at=out_of_range")
	}
	if value.RevokedAt != nil && value.RevokedAt.IsZero() {
		return fmt.Errorf("revoked_at=empty")
	}
	return nil
}

func validateAccessTokenFields(tokenHash, clientID, subject string, scopes, resources []string) error {
	if err := validateAccessTokenHash(tokenHash); err != nil {
		return err
	}
	if err := ValidateOAuthClientID(clientID); err != nil {
		return err
	}
	if err := validateAccessTokenSubject(subject); err != nil {
		return err
	}
	if err := validateStrings("scopes", scopes, maxClientValueCount, maxClientValueLength, true); err != nil {
		return err
	}
	return validateAuthorizationCodeResources(resources)
}

func validateAccessTokenHash(value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: token_hash=empty")
	}
	if len(value) > maxAccessTokenHashLength {
		return fmt.Errorf("invalid parameter: token_hash=too_long actual_length=%d max_length=%d", len(value), maxAccessTokenHashLength)
	}
	return validatePrintableASCII("token_hash", value)
}

func validateAccessTokenSubject(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("invalid parameter: subject=empty")
	}
	if utf8.RuneCountInString(value) > maxAccessTokenSubjectLength {
		return fmt.Errorf("invalid parameter: subject=too_long max_length=%d", maxAccessTokenSubjectLength)
	}
	return nil
}

func executeAccessTokenRevoke(ctx context.Context, executor Executor, query, operationErr string, args ...any) (bool, error) {
	result, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	if result == nil {
		return false, fmt.Errorf("%s: sql_result=null", operationErr)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: failed to read affected rows: %w", operationErr, err)
	}
	if count < 0 || count > 1 {
		return false, fmt.Errorf("%s: affected_rows=invalid actual_count=%d max_count=1", operationErr, count)
	}
	return count == 1, nil
}
