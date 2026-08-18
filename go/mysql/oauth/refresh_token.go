package oauth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultRefreshTokenTableName = "oauth_refresh_tokens"
	maxRefreshTokenHashLength    = 128
	maxRefreshTokenDeleteLimit   = 10000
)

var refreshTokenIDGenerator = &k4k3ruInternalGenerator.ID{}

type RefreshToken struct {
	ID              uint64
	TokenHash       string
	FamilyID        uint64
	ParentID        *uint64
	ClientID        string
	Subject         string
	Scopes          []string
	Resources       []string
	ExpiresAt       time.Time
	FamilyExpiresAt time.Time
	ConsumedAt      *time.Time
	RevokedAt       *time.Time
	CreatedAt       time.Time
}

type RefreshTokenStore struct {
	tableName string
}

type RefreshTokenInsertParams struct {
	ID              uint64
	TokenHash       string
	ClientID        string
	Subject         string
	Scopes          []string
	Resources       []string
	ExpiresAt       time.Time
	FamilyExpiresAt time.Time
	CreatedAt       time.Time
	Ignore          bool
}

type RefreshTokenRotateParams struct {
	TokenHash    string
	ClientID     string
	Now          time.Time
	NewID        uint64
	NewTokenHash string
	NewScopes    []string
	NewResources []string
	NewExpiresAt time.Time
}

type RefreshTokenRotateStatus uint8

const (
	RefreshTokenRotateStatusSucceeded RefreshTokenRotateStatus = iota + 1
	RefreshTokenRotateStatusNotFound
	RefreshTokenRotateStatusAlreadyConsumed
	RefreshTokenRotateStatusExpired
	RefreshTokenRotateStatusRevoked
	RefreshTokenRotateStatusFamilyExpired
)

type RefreshTokenRotateResult struct {
	Status        RefreshTokenRotateStatus
	PreviousToken *RefreshToken
	NewToken      *RefreshToken
	FamilyRevoked bool
}

type RefreshTokenRevokeSubjectParams struct {
	ClientID  string
	Subject   string
	RevokedAt time.Time
}

type RefreshTokenDeleteExpiredParams struct {
	Before time.Time
	Limit  uint64
}

// GenerateRefreshTokenID generates a refresh token record ID.
//
// Returns:
//   - Generated refresh token record ID.
//
// Version:
//   - 2026-08-18: Added.
func GenerateRefreshTokenID() uint64 {
	return refreshTokenIDGenerator.Generate()
}

// NewRefreshTokenStore creates a refresh token store.
//
// Parameters:
//   - tableName: Refresh token table name.
//
// Returns:
//   - Refresh token store.
//   - Creation error.
//
// Version:
//   - 2026-08-18: Added.
func NewRefreshTokenStore(tableName string) (*RefreshTokenStore, error) {
	operationErr := "failed to create oauth refresh token store"
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return &RefreshTokenStore{tableName: tableName}, nil
}

// CreateTable creates the refresh token table.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) CreateTable(ctx context.Context, executor Executor) error {
	operationErr := "failed to create oauth refresh token table"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
		%s VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Refresh token hash',
		%s BIGINT UNSIGNED NOT NULL COMMENT 'Token family ID',
		%s BIGINT UNSIGNED NULL COMMENT 'Parent token ID',
		%s VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'OAuth client ID',
		%s VARCHAR(255) NOT NULL COMMENT 'Subject',
		%s JSON NOT NULL COMMENT 'Scopes',
		%s JSON NOT NULL COMMENT 'Resources',
		%s DATETIME(6) NOT NULL COMMENT 'Expires at',
		%s DATETIME(6) NOT NULL COMMENT 'Family expires at',
		%s DATETIME(6) NULL COMMENT 'Consumed at',
		%s DATETIME(6) NULL COMMENT 'Revoked at',
		%s DATETIME(6) NOT NULL COMMENT 'Created at',
		PRIMARY KEY (%s),
		UNIQUE KEY uq_oauth_refresh_token_hash (%s),
		KEY idx_oauth_refresh_token_family_id (%s),
		KEY idx_oauth_refresh_token_parent_id (%s),
		KEY idx_oauth_refresh_token_client_subject (%s, %s),
		KEY idx_oauth_refresh_token_expires_at (%s),
		KEY idx_oauth_refresh_token_family_expires_at (%s),
		KEY idx_oauth_refresh_token_revoked_at (%s)
	) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`, s.tableName,
		ColID, ColTokenHash, ColFamilyID, ColParentID, ColClientID, ColSubject,
		ColScopes, ColResources, ColExpiresAt, ColFamilyExpiresAt, ColConsumedAt,
		ColRevokedAt, ColCreatedAt, ColID, ColTokenHash, ColFamilyID, ColParentID,
		ColClientID, ColSubject, ColExpiresAt, ColFamilyExpiresAt, ColRevokedAt)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Insert inserts a root refresh token record.
//
// The generated record ID is also used as the token family ID.
//
// Parameters:
//   - ctx: Context for the operation.
//   - executor: SQL executor.
//   - params: Root refresh token values.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) Insert(ctx context.Context, executor Executor, params *RefreshTokenInsertParams) error {
	operationErr := "failed to insert oauth refresh token"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: refresh_token_insert_params=null", operationErr)
	}
	if params.ID == 0 {
		params.ID = GenerateRefreshTokenID()
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	} else {
		params.CreatedAt = params.CreatedAt.UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	scopes, resources, err := encodeRefreshTokenCollections(params.Scopes, params.Resources)
	if err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	prefix := "INSERT"
	if params.Ignore {
		prefix = "INSERT IGNORE"
	}
	query := fmt.Sprintf("%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?);",
		prefix, s.tableName, ColID, ColTokenHash, ColFamilyID, ColParentID, ColClientID,
		ColSubject, ColScopes, ColResources, ColExpiresAt, ColFamilyExpiresAt, ColCreatedAt)
	_, err = executor.ExecContext(ctx, query, params.ID, params.TokenHash, params.ID,
		params.ClientID, params.Subject, scopes, resources, params.ExpiresAt.UTC(),
		params.FamilyExpiresAt.UTC(), params.CreatedAt)
	return normalizeRefreshTokenWriteError(operationErr, err)
}

// SelectByID selects a refresh token by record ID.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) SelectByID(ctx context.Context, executor Executor, id uint64) (*RefreshToken, error) {
	operationErr := "failed to select oauth refresh token by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, fmt.Errorf("%s: id=empty", operationErr)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColID)
	return scanRefreshToken(executor.QueryRowContext(ctx, query, id), operationErr)
}

// SelectByTokenHash selects a refresh token by token hash.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) SelectByTokenHash(ctx context.Context, executor Executor, tokenHash string) (*RefreshToken, error) {
	operationErr := "failed to select oauth refresh token by token hash"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := validateRefreshTokenHash("token_hash", tokenHash); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColTokenHash)
	return scanRefreshToken(executor.QueryRowContext(ctx, query, tokenHash), operationErr)
}

// Rotate consumes a refresh token and inserts its replacement atomically.
//
// The caller must commit successful rotations. When Status is
// RefreshTokenRotateStatusAlreadyConsumed and FamilyRevoked is true, the
// caller must commit to persist replay-triggered family revocation. Roll back
// protocol validation failures and database errors.
//
// Parameters:
//   - ctx: Context for the operation.
//   - tx: Caller-owned SQL transaction.
//   - params: Presented and replacement token values.
//
// Returns:
//   - Rotation result.
//   - Rotation error.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) Rotate(ctx context.Context, tx *sql.Tx, params *RefreshTokenRotateParams) (RefreshTokenRotateResult, error) {
	operationErr := "failed to rotate oauth refresh token"
	if s == nil || s.tableName == "" {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: refresh_token_store=null_or_empty", operationErr)
	}
	if ctx == nil {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: context=null", operationErr)
	}
	if tx == nil {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: sql_tx=null", operationErr)
	}
	if params == nil {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: refresh_token_rotate_params=null", operationErr)
	}
	if params.NewID == 0 {
		params.NewID = GenerateRefreshTokenID()
	}
	params.Now = params.Now.UTC()
	params.NewExpiresAt = params.NewExpiresAt.UTC()
	if err := params.Validate(); err != nil {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? AND %s = ? LIMIT 1 FOR UPDATE;", ColTokenHash, ColClientID)
	previous, err := scanRefreshToken(tx.QueryRowContext(ctx, query, params.TokenHash, params.ClientID), operationErr)
	if err != nil {
		return RefreshTokenRotateResult{}, err
	}
	if previous == nil {
		return RefreshTokenRotateResult{Status: RefreshTokenRotateStatusNotFound}, nil
	}
	result := RefreshTokenRotateResult{PreviousToken: previous}
	if previous.ConsumedAt != nil {
		count, err := s.revokeFamily(ctx, tx, previous.FamilyID, params.Now, operationErr)
		if err != nil {
			return RefreshTokenRotateResult{}, err
		}
		result.Status = RefreshTokenRotateStatusAlreadyConsumed
		result.FamilyRevoked = count > 0
		return result, nil
	}
	if previous.RevokedAt != nil {
		result.Status = RefreshTokenRotateStatusRevoked
		return result, nil
	}
	if !previous.FamilyExpiresAt.After(params.Now) {
		result.Status = RefreshTokenRotateStatusFamilyExpired
		return result, nil
	}
	if !previous.ExpiresAt.After(params.Now) {
		result.Status = RefreshTokenRotateStatusExpired
		return result, nil
	}
	if params.NewExpiresAt.After(previous.FamilyExpiresAt) {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: new_expires_at=out_of_range", operationErr)
	}
	scopes, resources, err := encodeRefreshTokenCollections(params.NewScopes, params.NewResources)
	if err != nil {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: %w", operationErr, err)
	}
	updateQuery := fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s IS NULL AND %s IS NULL AND %s>? AND %s>?;",
		s.tableName, ColConsumedAt, ColID, ColConsumedAt, ColRevokedAt, ColExpiresAt, ColFamilyExpiresAt)
	dbResult, err := tx.ExecContext(ctx, updateQuery, params.Now, previous.ID, params.Now, params.Now)
	if err != nil {
		return RefreshTokenRotateResult{}, fmt.Errorf("%s: failed to consume refresh token: %w", operationErr, err)
	}
	if err := requireRefreshTokenAffectedRow(dbResult, operationErr); err != nil {
		return RefreshTokenRotateResult{}, err
	}
	insertQuery := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
		s.tableName, ColID, ColTokenHash, ColFamilyID, ColParentID, ColClientID, ColSubject,
		ColScopes, ColResources, ColExpiresAt, ColFamilyExpiresAt, ColCreatedAt)
	_, err = tx.ExecContext(ctx, insertQuery, params.NewID, params.NewTokenHash, previous.FamilyID,
		previous.ID, previous.ClientID, previous.Subject, scopes, resources, params.NewExpiresAt,
		previous.FamilyExpiresAt, params.Now)
	if err := normalizeRefreshTokenWriteError(operationErr, err); err != nil {
		return RefreshTokenRotateResult{}, err
	}
	consumedAt := params.Now
	previous.ConsumedAt = &consumedAt
	result.Status = RefreshTokenRotateStatusSucceeded
	result.NewToken = &RefreshToken{ID: params.NewID, TokenHash: params.NewTokenHash,
		FamilyID: previous.FamilyID, ParentID: uint64Pointer(previous.ID), ClientID: previous.ClientID,
		Subject: previous.Subject, Scopes: sortedCopy(params.NewScopes), Resources: sortedCopy(params.NewResources),
		ExpiresAt: params.NewExpiresAt, FamilyExpiresAt: previous.FamilyExpiresAt, CreatedAt: params.Now}
	return result, nil
}

// RevokeByID revokes a refresh token by record ID.
//
// Returns true only when this call newly revoked the token.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) RevokeByID(ctx context.Context, executor Executor, id uint64, revokedAt time.Time) (bool, error) {
	operationErr := "failed to revoke oauth refresh token by id"
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
	result, err := executor.ExecContext(ctx, query, revokedAt.UTC(), id)
	if err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := requireRefreshTokenAtMostOneRow(result, operationErr); err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: failed to read affected rows: %w", operationErr, err)
	}
	return count == 1, nil
}

// RevokeFamily revokes all active refresh tokens in a family.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) RevokeFamily(ctx context.Context, executor Executor, familyID uint64, revokedAt time.Time) (int64, error) {
	operationErr := "failed to revoke oauth refresh token family"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return 0, err
	}
	if familyID == 0 {
		return 0, fmt.Errorf("%s: family_id=empty", operationErr)
	}
	if revokedAt.IsZero() {
		return 0, fmt.Errorf("%s: revoked_at=empty", operationErr)
	}
	return s.revokeFamily(ctx, executor, familyID, revokedAt.UTC(), operationErr)
}

// RevokeByClientIDAndSubject revokes refresh tokens for a client and subject.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) RevokeByClientIDAndSubject(ctx context.Context, executor Executor, params RefreshTokenRevokeSubjectParams) (int64, error) {
	operationErr := "failed to revoke oauth refresh tokens by client id and subject"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return 0, err
	}
	if err := params.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s=? AND %s IS NULL;",
		s.tableName, ColRevokedAt, ColClientID, ColSubject, ColRevokedAt)
	return executeRefreshTokenCount(ctx, executor, query, operationErr, params.RevokedAt.UTC(), params.ClientID, params.Subject)
}

// DeleteExpired deletes refresh token history whose family lifetime has ended.
//
// Version:
//   - 2026-08-18: Added.
func (s *RefreshTokenStore) DeleteExpired(ctx context.Context, executor Executor, params RefreshTokenDeleteExpiredParams) (int64, error) {
	operationErr := "failed to delete expired oauth refresh tokens"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return 0, err
	}
	if err := params.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s < ? ORDER BY %s, %s LIMIT ?;",
		s.tableName, ColFamilyExpiresAt, ColFamilyExpiresAt, ColID)
	return executeRefreshTokenCount(ctx, executor, query, operationErr, params.Before.UTC(), params.Limit)
}

// Validate validates refresh token insert parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p *RefreshTokenInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: refresh_token_insert_params=null")
	}
	if p.ID == 0 {
		return fmt.Errorf("invalid parameter: id=empty")
	}
	if err := validateRefreshTokenFields(p.TokenHash, p.ClientID, p.Subject, p.Scopes, p.Resources); err != nil {
		return err
	}
	return validateRefreshTokenTimes(p.CreatedAt, p.ExpiresAt, p.FamilyExpiresAt)
}

// Validate validates refresh token rotation parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p *RefreshTokenRotateParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: refresh_token_rotate_params=null")
	}
	if err := validateRefreshTokenHash("token_hash", p.TokenHash); err != nil {
		return err
	}
	if err := ValidateOAuthClientID(p.ClientID); err != nil {
		return err
	}
	if p.Now.IsZero() {
		return fmt.Errorf("invalid parameter: now=empty")
	}
	if p.NewID == 0 {
		return fmt.Errorf("invalid parameter: new_id=empty")
	}
	if err := validateRefreshTokenHash("new_token_hash", p.NewTokenHash); err != nil {
		return err
	}
	if p.TokenHash == p.NewTokenHash {
		return fmt.Errorf("invalid parameter: new_token_hash=invalid")
	}
	if err := validateStrings("new_scopes", p.NewScopes, maxClientValueCount, maxClientValueLength, true); err != nil {
		return err
	}
	if err := validateAuthorizationCodeResources(p.NewResources); err != nil {
		return err
	}
	if p.NewExpiresAt.IsZero() {
		return fmt.Errorf("invalid parameter: new_expires_at=empty")
	}
	if !p.NewExpiresAt.After(p.Now) {
		return fmt.Errorf("invalid parameter: new_expires_at=out_of_range")
	}
	return nil
}

// Validate validates subject-wide refresh token revocation parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p RefreshTokenRevokeSubjectParams) Validate() error {
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

// Validate validates expired refresh token deletion parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p RefreshTokenDeleteExpiredParams) Validate() error {
	if p.Before.IsZero() {
		return fmt.Errorf("invalid parameter: before=empty")
	}
	if p.Limit == 0 || p.Limit > maxRefreshTokenDeleteLimit {
		return fmt.Errorf("invalid parameter: limit=out_of_range max_value=%d", maxRefreshTokenDeleteLimit)
	}
	return nil
}

func (s *RefreshTokenStore) validateOperation(ctx context.Context, executor Executor, operationErr string) error {
	if s == nil {
		return fmt.Errorf("%s: refresh_token_store=null", operationErr)
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

func (s *RefreshTokenStore) selectClause() string {
	return fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s",
		ColID, ColTokenHash, ColFamilyID, ColParentID, ColClientID, ColSubject, ColScopes,
		ColResources, ColExpiresAt, ColFamilyExpiresAt, ColConsumedAt, ColRevokedAt,
		ColCreatedAt, s.tableName)
}

func (s *RefreshTokenStore) revokeFamily(ctx context.Context, executor Executor, familyID uint64, revokedAt time.Time, operationErr string) (int64, error) {
	query := fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s IS NULL;", s.tableName, ColRevokedAt, ColFamilyID, ColRevokedAt)
	return executeRefreshTokenCount(ctx, executor, query, operationErr, revokedAt, familyID)
}

func scanRefreshToken(row *sql.Row, operationErr string) (*RefreshToken, error) {
	result := &RefreshToken{}
	var scopes, resources []byte
	if err := row.Scan(&result.ID, &result.TokenHash, &result.FamilyID, &result.ParentID,
		&result.ClientID, &result.Subject, &scopes, &resources, &result.ExpiresAt,
		&result.FamilyExpiresAt, &result.ConsumedAt, &result.RevokedAt, &result.CreatedAt); err != nil {
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
	if err := validateRefreshToken(result); err != nil {
		return nil, fmt.Errorf("%s: invalid stored refresh token: %w", operationErr, err)
	}
	result.ExpiresAt = result.ExpiresAt.UTC()
	result.FamilyExpiresAt = result.FamilyExpiresAt.UTC()
	result.ConsumedAt = utcTimePointer(result.ConsumedAt)
	result.RevokedAt = utcTimePointer(result.RevokedAt)
	result.CreatedAt = result.CreatedAt.UTC()
	return result, nil
}

func validateRefreshToken(value *RefreshToken) error {
	if value == nil {
		return fmt.Errorf("refresh_token=null")
	}
	if value.ID == 0 {
		return fmt.Errorf("id=empty")
	}
	if value.FamilyID == 0 {
		return fmt.Errorf("family_id=empty")
	}
	if value.ParentID != nil && *value.ParentID == 0 {
		return fmt.Errorf("parent_id=empty")
	}
	if value.ParentID == nil && value.FamilyID != value.ID {
		return fmt.Errorf("family_id=invalid")
	}
	if err := validateRefreshTokenFields(value.TokenHash, value.ClientID, value.Subject, value.Scopes, value.Resources); err != nil {
		return err
	}
	if err := validateRefreshTokenTimes(value.CreatedAt, value.ExpiresAt, value.FamilyExpiresAt); err != nil {
		return err
	}
	if value.ConsumedAt != nil && value.ConsumedAt.IsZero() {
		return fmt.Errorf("consumed_at=empty")
	}
	if value.RevokedAt != nil && value.RevokedAt.IsZero() {
		return fmt.Errorf("revoked_at=empty")
	}
	return nil
}

func validateRefreshTokenFields(tokenHash, clientID, subject string, scopes, resources []string) error {
	if err := validateRefreshTokenHash("token_hash", tokenHash); err != nil {
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

func validateRefreshTokenHash(parameter, value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: %s=empty", parameter)
	}
	if len(value) > maxRefreshTokenHashLength {
		return fmt.Errorf("invalid parameter: %s=too_long actual_length=%d max_length=%d", parameter, len(value), maxRefreshTokenHashLength)
	}
	return validatePrintableASCII(parameter, value)
}

func validateRefreshTokenTimes(createdAt, expiresAt, familyExpiresAt time.Time) error {
	if createdAt.IsZero() {
		return fmt.Errorf("invalid parameter: created_at=empty")
	}
	if expiresAt.IsZero() {
		return fmt.Errorf("invalid parameter: expires_at=empty")
	}
	if familyExpiresAt.IsZero() {
		return fmt.Errorf("invalid parameter: family_expires_at=empty")
	}
	if !expiresAt.After(createdAt) {
		return fmt.Errorf("invalid parameter: expires_at=out_of_range")
	}
	if expiresAt.After(familyExpiresAt) {
		return fmt.Errorf("invalid parameter: family_expires_at=out_of_range")
	}
	return nil
}

func encodeRefreshTokenCollections(scopes, resources []string) ([]byte, []byte, error) {
	encodedScopes, err := encodeAuthorizationCodeStrings(scopes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode refresh token scopes: %w", err)
	}
	encodedResources, err := encodeAuthorizationCodeStrings(resources)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode refresh token resources: %w", err)
	}
	return encodedScopes, encodedResources, nil
}

func normalizeRefreshTokenWriteError(operationErr string, err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return fmt.Errorf("%s: %w: %w", operationErr, ErrDuplicateKey, err)
	}
	return fmt.Errorf("%s: %w", operationErr, err)
}

func requireRefreshTokenAffectedRow(result sql.Result, operationErr string) error {
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

func requireRefreshTokenAtMostOneRow(result sql.Result, operationErr string) error {
	if result == nil {
		return fmt.Errorf("%s: sql_result=null", operationErr)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: failed to read affected rows: %w", operationErr, err)
	}
	if count < 0 || count > 1 {
		return fmt.Errorf("%s: affected_rows=invalid actual_count=%d max_count=1", operationErr, count)
	}
	return nil
}

func executeRefreshTokenCount(ctx context.Context, executor Executor, query, operationErr string, args ...any) (int64, error) {
	result, err := executor.ExecContext(ctx, query, args...)
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

func uint64Pointer(value uint64) *uint64 {
	return &value
}
