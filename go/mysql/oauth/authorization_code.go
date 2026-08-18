package oauth

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultAuthorizationCodeTableName = "oauth_authorization_codes"
	maxAuthorizationCodeHashLength    = 128
	maxAuthorizationCodeSubjectLength = 255
	maxAuthorizationCodeURILength     = 2048
	maxAuthorizationCodeResourceCount = 8
	maxCodeChallengeLength            = 43
	sha256Size                        = 32
)

var authorizationCodeIDGenerator = &k4k3ruInternalGenerator.ID{}

type AuthorizationCode struct {
	ID                  uint64
	CodeHash            string
	ClientID            string
	Subject             string
	RedirectURI         string
	Scopes              []string
	Resources           []string
	CodeChallenge       string
	CodeChallengeMethod CodeChallengeMethod
	ExpiresAt           time.Time
	ConsumedAt          *time.Time
	CreatedAt           time.Time
}

type CodeChallengeMethod string

const CodeChallengeMethodS256 CodeChallengeMethod = "S256"

type AuthorizationCodeStore struct {
	tableName string
}

type AuthorizationCodeInsertParams struct {
	ID                  uint64
	CodeHash            string
	ClientID            string
	Subject             string
	RedirectURI         string
	Scopes              []string
	Resources           []string
	CodeChallenge       string
	CodeChallengeMethod CodeChallengeMethod
	ExpiresAt           time.Time
	ConsumedAt          *time.Time
	CreatedAt           time.Time
	Ignore              bool
}

type AuthorizationCodeConsumeParams struct {
	CodeHash string
	Now      time.Time
}

type AuthorizationCodeConsumeStatus uint8

const (
	AuthorizationCodeConsumeStatusSucceeded AuthorizationCodeConsumeStatus = iota + 1
	AuthorizationCodeConsumeStatusNotFound
	AuthorizationCodeConsumeStatusAlreadyConsumed
	AuthorizationCodeConsumeStatusExpired
)

type AuthorizationCodeConsumeResult struct {
	Status            AuthorizationCodeConsumeStatus
	AuthorizationCode *AuthorizationCode
}

// GenerateAuthorizationCodeID generates an authorization code record ID.
//
// Version:
//   - 2026-08-18: Added.
func GenerateAuthorizationCodeID() uint64 {
	return authorizationCodeIDGenerator.Generate()
}

// NewAuthorizationCodeStore creates an authorization code store.
//
// Parameters:
//   - tableName: Authorization code table name.
//
// Returns:
//   - Authorization code store.
//   - Creation error.
//
// Version:
//   - 2026-08-18: Added.
func NewAuthorizationCodeStore(tableName string) (*AuthorizationCodeStore, error) {
	operationErr := "failed to create oauth authorization code store"
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return &AuthorizationCodeStore{tableName: tableName}, nil
}

// CreateTable creates the authorization code table.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//
// Version:
//   - 2026-08-18: Added.
func (s *AuthorizationCodeStore) CreateTable(ctx context.Context, executor Executor) error {
	operationErr := "failed to create oauth authorization code table"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
		%s VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Authorization code hash',
		%s VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'OAuth client ID',
		%s VARCHAR(255) NOT NULL COMMENT 'Subject',
		%s VARCHAR(2048) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Redirect URI',
		%s JSON NOT NULL COMMENT 'Scopes',
		%s JSON NOT NULL COMMENT 'Resources',
		%s VARCHAR(43) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'PKCE code challenge',
		%s VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'PKCE code challenge method',
		%s DATETIME(6) NOT NULL COMMENT 'Expires at',
		%s DATETIME(6) NULL COMMENT 'Consumed at',
		%s DATETIME(6) NOT NULL COMMENT 'Created at',
		PRIMARY KEY (%s),
		UNIQUE KEY uq_oauth_authorization_code_hash (%s),
		KEY idx_oauth_authorization_code_client_subject (%s, %s),
		KEY idx_oauth_authorization_code_expires_at (%s)
	) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`, s.tableName,
		ColID, ColCodeHash, ColClientID, ColSubject, ColRedirectURI, ColScopes, ColResources,
		ColCodeChallenge, ColCodeChallengeMethod, ColExpiresAt, ColConsumedAt, ColCreatedAt,
		ColID, ColCodeHash, ColClientID, ColSubject, ColExpiresAt)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Insert inserts an authorization code record.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//   - params: Authorization code values.
//
// Version:
//   - 2026-08-18: Added.
func (s *AuthorizationCodeStore) Insert(ctx context.Context, executor Executor, params *AuthorizationCodeInsertParams) error {
	operationErr := "failed to insert oauth authorization code"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: authorization_code_insert_params=null", operationErr)
	}
	if params.ID == 0 {
		params.ID = GenerateAuthorizationCodeID()
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
	query := fmt.Sprintf("%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
		prefix, s.tableName, ColID, ColCodeHash, ColClientID, ColSubject, ColRedirectURI,
		ColScopes, ColResources, ColCodeChallenge, ColCodeChallengeMethod, ColExpiresAt,
		ColConsumedAt, ColCreatedAt)
	_, err = executor.ExecContext(ctx, query, params.ID, params.CodeHash, params.ClientID,
		params.Subject, params.RedirectURI, scopes, resources, params.CodeChallenge,
		params.CodeChallengeMethod, params.ExpiresAt.UTC(), utcTimePointer(params.ConsumedAt), params.CreatedAt)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// SelectByID selects an authorization code by record ID.
//
// Version:
//   - 2026-08-18: Added.
func (s *AuthorizationCodeStore) SelectByID(ctx context.Context, executor Executor, id uint64) (*AuthorizationCode, error) {
	operationErr := "failed to select oauth authorization code by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, fmt.Errorf("%s: id=empty", operationErr)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColID)
	return scanAuthorizationCode(executor.QueryRowContext(ctx, query, id), operationErr)
}

// ConsumeByCodeHash locks and consumes an authorization code.
//
// The caller must validate the returned client, redirect URI, resources, and
// PKCE values before committing the transaction. Roll back the transaction
// when protocol validation or subsequent token creation fails.
//
// Version:
//   - 2026-08-18: Added.
func (s *AuthorizationCodeStore) ConsumeByCodeHash(ctx context.Context, tx *sql.Tx, params AuthorizationCodeConsumeParams) (AuthorizationCodeConsumeResult, error) {
	operationErr := "failed to consume oauth authorization code"
	if s == nil || s.tableName == "" {
		return AuthorizationCodeConsumeResult{}, fmt.Errorf("%s: authorization_code_store=null_or_empty", operationErr)
	}
	if ctx == nil {
		return AuthorizationCodeConsumeResult{}, fmt.Errorf("%s: context=null", operationErr)
	}
	if tx == nil {
		return AuthorizationCodeConsumeResult{}, fmt.Errorf("%s: sql_tx=null", operationErr)
	}
	if err := params.Validate(); err != nil {
		return AuthorizationCodeConsumeResult{}, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1 FOR UPDATE;", ColCodeHash)
	value, err := scanAuthorizationCode(tx.QueryRowContext(ctx, query, params.CodeHash), operationErr)
	if err != nil {
		return AuthorizationCodeConsumeResult{}, err
	}
	if value == nil {
		return AuthorizationCodeConsumeResult{Status: AuthorizationCodeConsumeStatusNotFound}, nil
	}
	result := AuthorizationCodeConsumeResult{AuthorizationCode: value}
	if value.ConsumedAt != nil {
		result.Status = AuthorizationCodeConsumeStatusAlreadyConsumed
		return result, nil
	}
	if !value.ExpiresAt.After(params.Now) {
		result.Status = AuthorizationCodeConsumeStatusExpired
		return result, nil
	}
	query = fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s IS NULL AND %s>?;",
		s.tableName, ColConsumedAt, ColID, ColConsumedAt, ColExpiresAt)
	dbResult, err := tx.ExecContext(ctx, query, params.Now.UTC(), value.ID, params.Now.UTC())
	if err != nil {
		return AuthorizationCodeConsumeResult{}, fmt.Errorf("%s: failed to update consumed timestamp: %w", operationErr, err)
	}
	if err := requireAuthorizationCodeAffectedRow(dbResult, operationErr); err != nil {
		return AuthorizationCodeConsumeResult{}, err
	}
	consumedAt := params.Now.UTC()
	value.ConsumedAt = &consumedAt
	result.Status = AuthorizationCodeConsumeStatusSucceeded
	return result, nil
}

// IsValid reports whether the PKCE code challenge method is supported.
//
// Version:
//   - 2026-08-18: Added.
func (m CodeChallengeMethod) IsValid() bool {
	return m == CodeChallengeMethodS256
}

// Validate validates the PKCE code challenge method.
//
// Version:
//   - 2026-08-18: Added.
func (m CodeChallengeMethod) Validate() error {
	if !m.IsValid() {
		return fmt.Errorf("invalid parameter: code_challenge_method=invalid")
	}
	return nil
}

// Validate validates authorization code insert parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p *AuthorizationCodeInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: authorization_code_insert_params=null")
	}
	if p.ID == 0 {
		return fmt.Errorf("invalid parameter: id=empty")
	}
	if err := validateAuthorizationCodeHash(p.CodeHash); err != nil {
		return err
	}
	if err := ValidateOAuthClientID(p.ClientID); err != nil {
		return err
	}
	if strings.TrimSpace(p.Subject) == "" {
		return fmt.Errorf("invalid parameter: subject=empty")
	}
	if utf8.RuneCountInString(p.Subject) > maxAuthorizationCodeSubjectLength {
		return fmt.Errorf("invalid parameter: subject=too_long max_length=%d", maxAuthorizationCodeSubjectLength)
	}
	if err := validateAuthorizationCodeURI("redirect_uri", p.RedirectURI); err != nil {
		return err
	}
	if err := validateStrings("scopes", p.Scopes, maxClientValueCount, maxClientValueLength, true); err != nil {
		return err
	}
	if err := validateAuthorizationCodeResources(p.Resources); err != nil {
		return err
	}
	if err := validateCodeChallenge(p.CodeChallenge); err != nil {
		return err
	}
	if err := p.CodeChallengeMethod.Validate(); err != nil {
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
	if p.ConsumedAt != nil && p.ConsumedAt.IsZero() {
		return fmt.Errorf("invalid parameter: consumed_at=empty")
	}
	return nil
}

// Validate validates authorization code consume parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p AuthorizationCodeConsumeParams) Validate() error {
	if err := validateAuthorizationCodeHash(p.CodeHash); err != nil {
		return err
	}
	if p.Now.IsZero() {
		return fmt.Errorf("invalid parameter: now=empty")
	}
	return nil
}

func (s *AuthorizationCodeStore) validateOperation(ctx context.Context, executor Executor, operationErr string) error {
	if s == nil {
		return fmt.Errorf("%s: authorization_code_store=null", operationErr)
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

func (s *AuthorizationCodeStore) selectClause() string {
	return fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s",
		ColID, ColCodeHash, ColClientID, ColSubject, ColRedirectURI, ColScopes, ColResources,
		ColCodeChallenge, ColCodeChallengeMethod, ColExpiresAt, ColConsumedAt, ColCreatedAt, s.tableName)
}

func scanAuthorizationCode(row *sql.Row, operationErr string) (*AuthorizationCode, error) {
	result := &AuthorizationCode{}
	var scopes, resources []byte
	if err := row.Scan(&result.ID, &result.CodeHash, &result.ClientID, &result.Subject,
		&result.RedirectURI, &scopes, &resources, &result.CodeChallenge,
		&result.CodeChallengeMethod, &result.ExpiresAt, &result.ConsumedAt,
		&result.CreatedAt); err != nil {
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
	if err := validateAuthorizationCode(result); err != nil {
		return nil, fmt.Errorf("%s: invalid stored authorization code: %w", operationErr, err)
	}
	result.ExpiresAt = result.ExpiresAt.UTC()
	result.ConsumedAt = utcTimePointer(result.ConsumedAt)
	result.CreatedAt = result.CreatedAt.UTC()
	return result, nil
}

func validateAuthorizationCode(value *AuthorizationCode) error {
	if value == nil {
		return fmt.Errorf("authorization_code=null")
	}
	params := &AuthorizationCodeInsertParams{
		ID: value.ID, CodeHash: value.CodeHash, ClientID: value.ClientID, Subject: value.Subject,
		RedirectURI: value.RedirectURI, Scopes: value.Scopes, Resources: value.Resources,
		CodeChallenge: value.CodeChallenge, CodeChallengeMethod: value.CodeChallengeMethod,
		ExpiresAt: value.ExpiresAt, ConsumedAt: value.ConsumedAt, CreatedAt: value.CreatedAt,
	}
	return params.Validate()
}

func validateAuthorizationCodeHash(value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: code_hash=empty")
	}
	if len(value) > maxAuthorizationCodeHashLength {
		return fmt.Errorf("invalid parameter: code_hash=too_long actual_length=%d max_length=%d", len(value), maxAuthorizationCodeHashLength)
	}
	return validatePrintableASCII("code_hash", value)
}

func validateAuthorizationCodeResources(values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("invalid parameter: resources=empty")
	}
	if len(values) > maxAuthorizationCodeResourceCount {
		return fmt.Errorf("invalid parameter: resources=too_long actual_length=%d max_length=%d", len(values), maxAuthorizationCodeResourceCount)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := validateAuthorizationCodeURI("resource", value); err != nil {
			return err
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("invalid parameter: resources=invalid")
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateAuthorizationCodeURI(parameter, value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: %s=empty", parameter)
	}
	if len(value) > maxAuthorizationCodeURILength {
		return fmt.Errorf("invalid parameter: %s=too_long actual_length=%d max_length=%d", parameter, len(value), maxAuthorizationCodeURILength)
	}
	if err := validatePrintableASCII(parameter, value); err != nil {
		return err
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Fragment != "" {
		return fmt.Errorf("invalid parameter: %s=invalid", parameter)
	}
	return nil
}

func validateCodeChallenge(value string) error {
	if len(value) != maxCodeChallengeLength {
		return fmt.Errorf("invalid parameter: code_challenge=invalid")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != sha256Size || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return fmt.Errorf("invalid parameter: code_challenge=invalid")
	}
	return nil
}

func validatePrintableASCII(parameter, value string) error {
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return fmt.Errorf("invalid parameter: %s=invalid", parameter)
		}
	}
	return nil
}

func encodeAuthorizationCodeStrings(values []string) ([]byte, error) {
	values = sortedCopy(values)
	if values == nil {
		values = []string{}
	}
	return json.Marshal(values)
}

func requireAuthorizationCodeAffectedRow(result sql.Result, operationErr string) error {
	if result == nil {
		return fmt.Errorf("%s: sql_result=null", operationErr)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: failed to read affected rows: %w", operationErr, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: affected_rows=invalid actual_count=%d expected_count=1", operationErr, count)
	}
	return nil
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.UTC()
	return &result
}
