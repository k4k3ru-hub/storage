package oauth

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruInternalSQLScan "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultAuthorizationRequestTableName = "oauth_authorization_requests"
	maxAuthorizationRequestHashLength    = 128
	maxAuthorizationRequestStateLength   = 2048
)

var authorizationRequestIDGenerator = &k4k3ruInternalGenerator.ID{}

type AuthorizationRequest struct {
	ID                  uint64
	RequestHash         string
	Status              AuthorizationRequestStatus
	ClientID            string
	Subject             *string
	RedirectURI         string
	State               string
	Scopes              []string
	Resources           []string
	CodeChallenge       string
	CodeChallengeMethod CodeChallengeMethod
	ExpiresAt           time.Time
	OTPRequestedAt      *time.Time
	AuthenticatedAt     *time.Time
	ApprovedAt          *time.Time
	DeniedAt            *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type AuthorizationRequestStatus uint8

const (
	AuthorizationRequestStatusPrepared AuthorizationRequestStatus = iota + 1
	AuthorizationRequestStatusOTPRequested
	AuthorizationRequestStatusAuthenticated
	AuthorizationRequestStatusApproved
	AuthorizationRequestStatusDenied
)

type AuthorizationRequestStore struct{ tableName string }

type AuthorizationRequestInsertParams struct {
	ID                  uint64
	RequestHash         string
	Status              AuthorizationRequestStatus
	ClientID            string
	Subject             *string
	RedirectURI         string
	State               string
	Scopes              []string
	Resources           []string
	CodeChallenge       string
	CodeChallengeMethod CodeChallengeMethod
	ExpiresAt           time.Time
	OTPRequestedAt      *time.Time
	AuthenticatedAt     *time.Time
	ApprovedAt          *time.Time
	DeniedAt            *time.Time
	CreatedAt           time.Time
}

// GenerateAuthorizationRequestID generates an internal authorization request ID.
//
// Returns:
//   - Generated ID.
//
// Version:
//   - 2026-08-20: Added.
func GenerateAuthorizationRequestID() uint64 { return authorizationRequestIDGenerator.Generate() }

// NewAuthorizationRequestStore creates an authorization request store.
//
// Parameters:
//   - tableName: Authorization request table name.
//
// Returns:
//   - Authorization request store.
//
// Version:
//   - 2026-08-20: Added.
func NewAuthorizationRequestStore(tableName string) (*AuthorizationRequestStore, error) {
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("failed to create oauth authorization request store: %w", err)
	}
	return &AuthorizationRequestStore{tableName: tableName}, nil
}

// CreateTable creates the authorization request table.
//
// Parameters:
//   - ctx: Operation context.
//   - executor: SQL executor.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) CreateTable(ctx context.Context, executor Executor) error {
	if err := s.validateOperation(ctx, executor, "failed to create oauth authorization request table"); err != nil {
		return err
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT UNSIGNED NOT NULL COMMENT 'ID',
		request_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Request token hash',
		status TINYINT UNSIGNED NOT NULL COMMENT 'Status',
		client_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'OAuth client ID',
		subject VARCHAR(255) NULL COMMENT 'Authenticated subject',
		redirect_uri VARCHAR(2048) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Redirect URI',
		state VARCHAR(2048) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Client state',
		scopes JSON NOT NULL COMMENT 'Scopes',
		resources JSON NOT NULL COMMENT 'Resources',
		code_challenge VARCHAR(43) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'PKCE code challenge',
		code_challenge_method VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'PKCE code challenge method',
		expires_at DATETIME(6) NOT NULL COMMENT 'Expires at',
		otp_requested_at DATETIME(6) NULL COMMENT 'OTP requested at',
		authenticated_at DATETIME(6) NULL COMMENT 'Authenticated at',
		approved_at DATETIME(6) NULL COMMENT 'Approved at',
		denied_at DATETIME(6) NULL COMMENT 'Denied at',
		created_at DATETIME(6) NOT NULL COMMENT 'Created at',
		updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'Updated at',
		PRIMARY KEY (id),
		UNIQUE KEY uq_oauth_authorization_request_hash (request_hash),
		KEY idx_oauth_authorization_request_client_subject (client_id, subject),
		KEY idx_oauth_authorization_request_status_expires (status, expires_at)
	) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;`, s.tableName)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create oauth authorization request table: %w", err)
	}
	return nil
}

// Insert inserts an authorization request.
//
// Parameters:
//   - ctx: Operation context.
//   - executor: SQL executor.
//   - params: Authorization request values.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) Insert(ctx context.Context, executor Executor, params *AuthorizationRequestInsertParams) error {
	const operation = "failed to insert oauth authorization request"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: authorization_request_insert_params=null", operation)
	}
	if params.ID == 0 {
		params.ID = GenerateAuthorizationRequestID()
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	} else {
		params.CreatedAt = params.CreatedAt.UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	scopes, resources, err := encodeAuthorizationRequestCollections(params.Scopes, params.Resources)
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	query := fmt.Sprintf("INSERT INTO %s (id, request_hash, status, client_id, subject, redirect_uri, state, scopes, resources, code_challenge, code_challenge_method, expires_at, otp_requested_at, authenticated_at, approved_at, denied_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", s.tableName)
	_, err = executor.ExecContext(ctx, query, params.ID, params.RequestHash, params.Status, params.ClientID, params.Subject, params.RedirectURI, params.State, scopes, resources, params.CodeChallenge, params.CodeChallengeMethod, params.ExpiresAt.UTC(), utcTimePointer(params.OTPRequestedAt), utcTimePointer(params.AuthenticatedAt), utcTimePointer(params.ApprovedAt), utcTimePointer(params.DeniedAt), params.CreatedAt)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operation, ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

// SelectByRequestHash selects an authorization request by request hash.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) SelectByRequestHash(ctx context.Context, executor Executor, requestHash string) (*AuthorizationRequest, error) {
	const operation = "failed to select oauth authorization request by request hash"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return nil, err
	}
	if err := validateAuthorizationRequestHash(requestHash); err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return scanAuthorizationRequest(executor.QueryRowContext(ctx, s.selectClause()+" WHERE request_hash = ? LIMIT 1;", requestHash), operation)
}

// SelectByRequestHashForUpdate selects and locks an authorization request.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) SelectByRequestHashForUpdate(ctx context.Context, tx *sql.Tx, requestHash string) (*AuthorizationRequest, error) {
	const operation = "failed to select oauth authorization request for update"
	if s == nil || s.tableName == "" {
		return nil, fmt.Errorf("%s: authorization_request_store=null", operation)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%s: context=null", operation)
	}
	if tx == nil {
		return nil, fmt.Errorf("%s: sql_tx=null", operation)
	}
	if err := validateAuthorizationRequestHash(requestHash); err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return scanAuthorizationRequest(tx.QueryRowContext(ctx, s.selectClause()+" WHERE request_hash = ? LIMIT 1 FOR UPDATE;", requestHash), operation)
}

// MarkOTPRequested marks a prepared authorization request as OTP requested.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) MarkOTPRequested(ctx context.Context, executor Executor, id uint64, requestedAt time.Time) error {
	return s.transition(ctx, executor, id, AuthorizationRequestStatusPrepared, AuthorizationRequestStatusOTPRequested, "otp_requested_at", requestedAt, "failed to mark oauth authorization request otp requested")
}

// MarkAuthenticated marks an OTP-requested authorization request as authenticated.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) MarkAuthenticated(ctx context.Context, executor Executor, id uint64, subject string, authenticatedAt time.Time) error {
	const operation = "failed to mark oauth authorization request authenticated"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	if id == 0 {
		return fmt.Errorf("%s: id=empty", operation)
	}
	if strings.TrimSpace(subject) == "" {
		return fmt.Errorf("%s: subject=empty", operation)
	}
	if utf8.RuneCountInString(subject) > maxAuthorizationCodeSubjectLength {
		return fmt.Errorf("%s: subject=too_long max_length=%d", operation, maxAuthorizationCodeSubjectLength)
	}
	if authenticatedAt.IsZero() {
		return fmt.Errorf("%s: authenticated_at=empty", operation)
	}
	query := fmt.Sprintf("UPDATE %s SET status=?, subject=?, authenticated_at=? WHERE id=? AND status=? AND expires_at>?;", s.tableName)
	result, err := executor.ExecContext(ctx, query, AuthorizationRequestStatusAuthenticated, subject, authenticatedAt.UTC(), id, AuthorizationRequestStatusOTPRequested, authenticatedAt.UTC())
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return requireAuthorizationRequestAffectedRow(result, operation)
}

// Approve marks an authenticated authorization request as approved.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) Approve(ctx context.Context, executor Executor, id uint64, approvedAt time.Time) error {
	return s.transition(ctx, executor, id, AuthorizationRequestStatusAuthenticated, AuthorizationRequestStatusApproved, "approved_at", approvedAt, "failed to approve oauth authorization request")
}

// Deny marks an active authorization request as denied.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStore) Deny(ctx context.Context, executor Executor, id uint64, deniedAt time.Time) error {
	const operation = "failed to deny oauth authorization request"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	if id == 0 {
		return fmt.Errorf("%s: id=empty", operation)
	}
	if deniedAt.IsZero() {
		return fmt.Errorf("%s: denied_at=empty", operation)
	}
	query := fmt.Sprintf("UPDATE %s SET status=?, denied_at=? WHERE id=? AND status IN (?, ?, ?) AND expires_at>?;", s.tableName)
	result, err := executor.ExecContext(ctx, query, AuthorizationRequestStatusDenied, deniedAt.UTC(), id, AuthorizationRequestStatusPrepared, AuthorizationRequestStatusOTPRequested, AuthorizationRequestStatusAuthenticated, deniedAt.UTC())
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return requireAuthorizationRequestAffectedRow(result, operation)
}

// Validate validates authorization request insert parameters.
//
// Version:
//   - 2026-08-20: Added.
func (p *AuthorizationRequestInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: authorization_request_insert_params=null")
	}
	if p.ID == 0 {
		return fmt.Errorf("invalid parameter: id=empty")
	}
	if err := validateAuthorizationRequestHash(p.RequestHash); err != nil {
		return err
	}
	if err := p.Status.Validate(); err != nil {
		return err
	}
	if err := ValidateOAuthClientID(p.ClientID); err != nil {
		return err
	}
	if p.Subject != nil && (strings.TrimSpace(*p.Subject) == "" || utf8.RuneCountInString(*p.Subject) > maxAuthorizationCodeSubjectLength) {
		return fmt.Errorf("invalid parameter: subject=invalid")
	}
	if err := validateAuthorizationCodeURI("redirect_uri", p.RedirectURI); err != nil {
		return err
	}
	if len(p.State) > maxAuthorizationRequestStateLength {
		return fmt.Errorf("invalid parameter: state=too_long actual_length=%d max_length=%d", len(p.State), maxAuthorizationRequestStateLength)
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
	if p.ExpiresAt.IsZero() || p.CreatedAt.IsZero() || !p.ExpiresAt.After(p.CreatedAt) {
		return fmt.Errorf("invalid parameter: expires_at=out_of_range")
	}
	return validateAuthorizationRequestState(p)
}

// IsValid reports whether the authorization request status is valid.
//
// Version:
//   - 2026-08-20: Added.
func (s AuthorizationRequestStatus) IsValid() bool {
	return s >= AuthorizationRequestStatusPrepared && s <= AuthorizationRequestStatusDenied
}

// Validate validates the authorization request status.
//
// Version:
//   - 2026-08-20: Added.
func (s AuthorizationRequestStatus) Validate() error {
	if !s.IsValid() {
		return fmt.Errorf("invalid parameter: authorization_request_status=invalid")
	}
	return nil
}

// Value returns the authorization request status as a SQL value.
//
// Version:
//   - 2026-08-20: Added.
func (s AuthorizationRequestStatus) Value() (driver.Value, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return int64(s), nil
}

// Scan scans an authorization request status.
//
// Version:
//   - 2026-08-20: Added.
func (s *AuthorizationRequestStatus) Scan(value any) error {
	if s == nil {
		return fmt.Errorf("failed to scan oauth authorization request status: authorization_request_status=null")
	}
	parsed, err := k4k3ruInternalSQLScan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan oauth authorization request status: %w", err)
	}
	result := AuthorizationRequestStatus(parsed)
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan oauth authorization request status: %w", err)
	}
	*s = result
	return nil
}

func (s *AuthorizationRequestStore) transition(ctx context.Context, executor Executor, id uint64, from, to AuthorizationRequestStatus, timestampColumn string, at time.Time, operation string) error {
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	if id == 0 {
		return fmt.Errorf("%s: id=empty", operation)
	}
	if at.IsZero() {
		return fmt.Errorf("%s: transition_at=empty", operation)
	}
	query := fmt.Sprintf("UPDATE %s SET status=?, %s=? WHERE id=? AND status=? AND expires_at>?;", s.tableName, timestampColumn)
	result, err := executor.ExecContext(ctx, query, to, at.UTC(), id, from, at.UTC())
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return requireAuthorizationRequestAffectedRow(result, operation)
}

func (s *AuthorizationRequestStore) validateOperation(ctx context.Context, executor Executor, operation string) error {
	if s == nil || s.tableName == "" {
		return fmt.Errorf("%s: authorization_request_store=null", operation)
	}
	if ctx == nil {
		return fmt.Errorf("%s: context=null", operation)
	}
	if executor == nil {
		return fmt.Errorf("%s: executor=null", operation)
	}
	return nil
}

func (s *AuthorizationRequestStore) selectClause() string {
	return fmt.Sprintf("SELECT id, request_hash, status, client_id, subject, redirect_uri, state, scopes, resources, code_challenge, code_challenge_method, expires_at, otp_requested_at, authenticated_at, approved_at, denied_at, created_at, updated_at FROM %s", s.tableName)
}

func scanAuthorizationRequest(row *sql.Row, operation string) (*AuthorizationRequest, error) {
	result := &AuthorizationRequest{}
	var scopes, resources []byte
	if err := row.Scan(&result.ID, &result.RequestHash, &result.Status, &result.ClientID, &result.Subject, &result.RedirectURI, &result.State, &scopes, &resources, &result.CodeChallenge, &result.CodeChallengeMethod, &result.ExpiresAt, &result.OTPRequestedAt, &result.AuthenticatedAt, &result.ApprovedAt, &result.DeniedAt, &result.CreatedAt, &result.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	if err := json.Unmarshal(scopes, &result.Scopes); err != nil {
		return nil, fmt.Errorf("%s: failed to decode scopes: %w", operation, err)
	}
	if err := json.Unmarshal(resources, &result.Resources); err != nil {
		return nil, fmt.Errorf("%s: failed to decode resources: %w", operation, err)
	}
	params := &AuthorizationRequestInsertParams{ID: result.ID, RequestHash: result.RequestHash, Status: result.Status, ClientID: result.ClientID, Subject: result.Subject, RedirectURI: result.RedirectURI, State: result.State, Scopes: result.Scopes, Resources: result.Resources, CodeChallenge: result.CodeChallenge, CodeChallengeMethod: result.CodeChallengeMethod, ExpiresAt: result.ExpiresAt, OTPRequestedAt: result.OTPRequestedAt, AuthenticatedAt: result.AuthenticatedAt, ApprovedAt: result.ApprovedAt, DeniedAt: result.DeniedAt, CreatedAt: result.CreatedAt}
	if err := params.Validate(); err != nil {
		return nil, fmt.Errorf("%s: invalid stored authorization request: %w", operation, err)
	}
	return result, nil
}

func validateAuthorizationRequestHash(value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: request_hash=empty")
	}
	if len(value) > maxAuthorizationRequestHashLength {
		return fmt.Errorf("invalid parameter: request_hash=too_long actual_length=%d max_length=%d", len(value), maxAuthorizationRequestHashLength)
	}
	return validatePrintableASCII("request_hash", value)
}

func validateAuthorizationRequestState(p *AuthorizationRequestInsertParams) error {
	present := func(value *time.Time) bool { return value != nil && !value.IsZero() }
	if p.ApprovedAt != nil && p.DeniedAt != nil {
		return fmt.Errorf("invalid parameter: authorization_request_decision=invalid")
	}
	switch p.Status {
	case AuthorizationRequestStatusPrepared:
		if p.Subject != nil || present(p.OTPRequestedAt) || present(p.AuthenticatedAt) || present(p.ApprovedAt) || present(p.DeniedAt) {
			return fmt.Errorf("invalid parameter: authorization_request_state=invalid")
		}
	case AuthorizationRequestStatusOTPRequested:
		if p.Subject != nil || !present(p.OTPRequestedAt) || present(p.AuthenticatedAt) || present(p.ApprovedAt) || present(p.DeniedAt) {
			return fmt.Errorf("invalid parameter: authorization_request_state=invalid")
		}
	case AuthorizationRequestStatusAuthenticated:
		if p.Subject == nil || !present(p.OTPRequestedAt) || !present(p.AuthenticatedAt) || present(p.ApprovedAt) || present(p.DeniedAt) {
			return fmt.Errorf("invalid parameter: authorization_request_state=invalid")
		}
	case AuthorizationRequestStatusApproved:
		if p.Subject == nil || !present(p.OTPRequestedAt) || !present(p.AuthenticatedAt) || !present(p.ApprovedAt) || present(p.DeniedAt) {
			return fmt.Errorf("invalid parameter: authorization_request_state=invalid")
		}
	case AuthorizationRequestStatusDenied:
		if !present(p.DeniedAt) || present(p.ApprovedAt) {
			return fmt.Errorf("invalid parameter: authorization_request_state=invalid")
		}
	}
	return nil
}

func encodeAuthorizationRequestCollections(scopes, resources []string) ([]byte, []byte, error) {
	scopesJSON, err := json.Marshal(sortedCopy(scopes))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode authorization request scopes: %w", err)
	}
	resourcesJSON, err := json.Marshal(sortedCopy(resources))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode authorization request resources: %w", err)
	}
	return scopesJSON, resourcesJSON, nil
}

func requireAuthorizationRequestAffectedRow(result sql.Result, operation string) error {
	if result == nil {
		return fmt.Errorf("%s: sql_result=null", operation)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: failed to read affected rows: %w", operation, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: authorization request transition rejected: affected_rows=%d", operation, count)
	}
	return nil
}
