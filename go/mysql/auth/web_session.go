package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const DefaultWebSessionTableName = "auth_web_sessions"

type WebSession struct {
	ID                uint64
	SessionTokenHash  string
	Subject           string
	CreatedAt         time.Time
	LastActivityAt    time.Time
	ExpiresAt         time.Time
	AbsoluteExpiresAt time.Time
	RevokedAt         *time.Time
}

type WebSessionStore struct {
	tableName   string
	idGenerator k4k3ruInternalGenerator.ID
}

type WebSessionInsertParams struct {
	ID                uint64
	SessionTokenHash  string
	Subject           string
	CreatedAt         time.Time
	LastActivityAt    time.Time
	ExpiresAt         time.Time
	AbsoluteExpiresAt time.Time
}

type WebSessionSelectActiveParams struct {
	SessionTokenHash string
	Now              time.Time
}

type WebSessionTouchParams struct {
	SessionTokenHash string
	Now              time.Time
	IdleTimeout      time.Duration
}

// NewWebSessionStore creates a store for cookie-identified web sessions.
//
// Parameters:
//   - tableName: Application-owned table name, such as console_web_sessions.
//
// Version:
//   - 2026-09-07: Added.
func NewWebSessionStore(tableName string) (*WebSessionStore, error) {
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("failed to create web session store: %w", err)
	}
	return &WebSessionStore{tableName: tableName}, nil
}

// CreateTable creates the web session table without changing an existing schema.
//
// Version:
//   - 2026-09-07: Added.
func (s *WebSessionStore) CreateTable(ctx context.Context, executor Executor) error {
	operationErr := "failed to create web session table"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT UNSIGNED NOT NULL COMMENT 'Session record ID',
		session_token_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'SHA-256 session token hash in lowercase hex',
		subject VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'Authenticated subject',
		created_at DATETIME(6) NOT NULL COMMENT 'Session created at',
		last_activity_at DATETIME(6) NOT NULL COMMENT 'Last qualifying activity at',
		expires_at DATETIME(6) NOT NULL COMMENT 'Effective session expiry',
		absolute_expires_at DATETIME(6) NOT NULL COMMENT 'Maximum session expiry',
		revoked_at DATETIME(6) NULL COMMENT 'Session revoked at',
		PRIMARY KEY (id),
		UNIQUE KEY uq_web_session_token_hash (session_token_hash),
		KEY idx_web_session_subject (subject),
		KEY idx_web_session_expires_at (expires_at),
		CHECK (last_activity_at >= created_at),
		CHECK (expires_at > last_activity_at AND expires_at <= absolute_expires_at),
		CHECK (revoked_at IS NULL OR revoked_at >= created_at)
	) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`, s.tableName)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Insert inserts a web session, generating a missing ID without mutating params.
//
// Parameters:
//   - params: Explicit UTC-compatible timestamps and a token hash; no raw token.
//
// Version:
//   - 2026-09-07: Added.
func (s *WebSessionStore) Insert(ctx context.Context, executor Executor, params *WebSessionInsertParams) error {
	operationErr := "failed to insert web session"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	p := *params
	if p.ID == 0 {
		p.ID = s.idGenerator.Generate()
	}
	query := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?);", s.tableName,
		ColID, ColSessionTokenHash, ColSubject, ColCreatedAt, ColLastActivityAt, ColExpiresAt, ColAbsoluteExpiresAt)
	_, err := executor.ExecContext(ctx, query, p.ID, p.SessionTokenHash, p.Subject, p.CreatedAt.UTC(), p.LastActivityAt.UTC(), p.ExpiresAt.UTC(), p.AbsoluteExpiresAt.UTC())
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// SelectActiveByTokenHash selects a session without renewing its lifetime.
//
// Returns:
//   - Session, or nil for an unknown, expired, revoked, or not-yet-created session.
//
// Version:
//   - 2026-09-07: Added.
func (s *WebSessionStore) SelectActiveByTokenHash(ctx context.Context, executor Executor, params WebSessionSelectActiveParams) (*WebSession, error) {
	operationErr := "failed to select active web session"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := params.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s FROM %s WHERE session_token_hash = ? AND revoked_at IS NULL AND created_at <= ? AND expires_at > ? AND absolute_expires_at > ? LIMIT 1;",
		ColID, ColSessionTokenHash, ColSubject, ColCreatedAt, ColLastActivityAt, ColExpiresAt, ColAbsoluteExpiresAt, ColRevokedAt, s.tableName)
	row := executor.QueryRowContext(ctx, query, params.SessionTokenHash, params.Now.UTC(), params.Now.UTC(), params.Now.UTC())
	if row == nil {
		return nil, fmt.Errorf("%s: sql_row=null", operationErr)
	}
	var result WebSession
	if err := row.Scan(&result.ID, &result.SessionTokenHash, &result.Subject, &result.CreatedAt, &result.LastActivityAt, &result.ExpiresAt, &result.AbsoluteExpiresAt, &result.RevokedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	p := WebSessionInsertParams{ID: result.ID, SessionTokenHash: result.SessionTokenHash, Subject: result.Subject, CreatedAt: result.CreatedAt, LastActivityAt: result.LastActivityAt, ExpiresAt: result.ExpiresAt, AbsoluteExpiresAt: result.AbsoluteExpiresAt}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	if result.ID == 0 {
		return nil, fmt.Errorf("%s: id=empty", operationErr)
	}
	result.CreatedAt = result.CreatedAt.UTC()
	result.LastActivityAt = result.LastActivityAt.UTC()
	result.ExpiresAt = result.ExpiresAt.UTC()
	result.AbsoluteExpiresAt = result.AbsoluteExpiresAt.UTC()
	return &result, nil
}

// Touch advances qualifying activity and idle expiry, capped by absolute expiry.
//
// The conditional update never revives expired or revoked sessions. Older or
// equal activity timestamps do not overwrite concurrent updates. Callers must
// authenticate separately; false does not distinguish an unchanged session from
// an invalid session. Background polling must not call this method.
//
// Returns:
//   - Whether this call advanced the session activity.
//
// Version:
//   - 2026-09-07: Added.
func (s *WebSessionStore) Touch(ctx context.Context, executor Executor, params WebSessionTouchParams) (bool, error) {
	operationErr := "failed to update web session activity"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return false, err
	}
	if err := params.Validate(); err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("UPDATE %s SET last_activity_at = ?, expires_at = LEAST(absolute_expires_at, GREATEST(expires_at, ?)) WHERE session_token_hash = ? AND revoked_at IS NULL AND created_at <= ? AND expires_at > ? AND absolute_expires_at > ? AND last_activity_at < ?;", s.tableName)
	now := params.Now.UTC()
	return executeWebSessionUpdate(ctx, executor, query, operationErr, now, now.Add(params.IdleTimeout), params.SessionTokenHash, now, now, now, now)
}

// RevokeByTokenHash idempotently revokes a session, including an expired session.
//
// Returns:
//   - Whether this call newly revoked the session.
//
// Version:
//   - 2026-09-07: Added.
func (s *WebSessionStore) RevokeByTokenHash(ctx context.Context, executor Executor, tokenHash string, revokedAt time.Time) (bool, error) {
	operationErr := "failed to revoke web session"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return false, err
	}
	if err := validateWebSessionTokenHash(tokenHash); err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := validateWebSessionTime("revoked_at", revokedAt); err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("UPDATE %s SET revoked_at = ? WHERE session_token_hash = ? AND revoked_at IS NULL AND created_at <= ?;", s.tableName)
	return executeWebSessionUpdate(ctx, executor, query, operationErr, revokedAt.UTC(), tokenHash, revokedAt.UTC())
}

// Validate validates web session insertion parameters; a zero ID requests generation.
//
// Version:
//   - 2026-09-07: Added.
func (p *WebSessionInsertParams) Validate() error {
	op := "failed to validate web session insert parameters"
	if p == nil {
		return fmt.Errorf("%s: params=null", op)
	}
	if err := validateWebSessionTokenHash(p.SessionTokenHash); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if strings.TrimSpace(p.Subject) == "" {
		return fmt.Errorf("%s: subject=empty", op)
	}
	if !utf8.ValidString(p.Subject) {
		return fmt.Errorf("%s: subject=invalid", op)
	}
	if utf8.RuneCountInString(p.Subject) > 255 {
		return fmt.Errorf("%s: subject=too_long max_length=255", op)
	}
	for _, field := range []struct {
		name  string
		value time.Time
	}{{"created_at", p.CreatedAt}, {"last_activity_at", p.LastActivityAt}, {"expires_at", p.ExpiresAt}, {"absolute_expires_at", p.AbsoluteExpiresAt}} {
		if err := validateWebSessionTime(field.name, field.value); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}
	if p.LastActivityAt.Before(p.CreatedAt) {
		return fmt.Errorf("%s: last_activity_at=out_of_range", op)
	}
	if !p.ExpiresAt.After(p.LastActivityAt) || p.ExpiresAt.After(p.AbsoluteExpiresAt) {
		return fmt.Errorf("%s: expires_at=out_of_range", op)
	}
	return nil
}

// Validate validates an active web session lookup.
//
// Version:
//   - 2026-09-07: Added.
func (p WebSessionSelectActiveParams) Validate() error {
	if err := validateWebSessionTokenHash(p.SessionTokenHash); err != nil {
		return fmt.Errorf("failed to validate web session lookup: %w", err)
	}
	if err := validateWebSessionTime("now", p.Now); err != nil {
		return fmt.Errorf("failed to validate web session lookup: %w", err)
	}
	return nil
}

// Validate validates a web session activity update.
//
// Version:
//   - 2026-09-07: Added.
func (p WebSessionTouchParams) Validate() error {
	if err := (WebSessionSelectActiveParams{SessionTokenHash: p.SessionTokenHash, Now: p.Now}).Validate(); err != nil {
		return fmt.Errorf("failed to validate web session activity: %w", err)
	}
	if p.IdleTimeout < time.Microsecond || p.IdleTimeout%time.Microsecond != 0 {
		return fmt.Errorf("failed to validate web session activity: idle_timeout=out_of_range")
	}
	if err := validateWebSessionTime("expires_at", p.Now.Add(p.IdleTimeout)); err != nil {
		return fmt.Errorf("failed to validate web session activity: %w", err)
	}
	return nil
}

func validateWebSessionTokenHash(value string) error {
	if value == "" {
		return fmt.Errorf("failed to validate web session token hash: session_token_hash=empty")
	}
	if len(value) != 64 {
		return fmt.Errorf("failed to validate web session token hash: session_token_hash=invalid")
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return fmt.Errorf("failed to validate web session token hash: session_token_hash=invalid")
		}
	}
	return nil
}

func validateWebSessionTime(name string, value time.Time) error {
	if value.IsZero() {
		return fmt.Errorf("failed to validate web session timestamp: %s=empty", name)
	}
	if value.UTC().Year() < 1000 || value.UTC().Year() > 9999 || value.Nanosecond()%1000 != 0 {
		return fmt.Errorf("failed to validate web session timestamp: %s=out_of_range", name)
	}
	return nil
}

func (s *WebSessionStore) validateOperation(ctx context.Context, executor Executor, operationErr string) error {
	if s == nil || s.tableName == "" {
		return fmt.Errorf("%s: web_session_store=null", operationErr)
	}
	if ctx == nil {
		return fmt.Errorf("%s: context=null", operationErr)
	}
	if executor == nil {
		return fmt.Errorf("%s: executor=null", operationErr)
	}
	return nil
}

func executeWebSessionUpdate(ctx context.Context, executor Executor, query, operationErr string, args ...any) (bool, error) {
	result, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	if result == nil {
		return false, fmt.Errorf("%s: sql_result=null", operationErr)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: %w", operationErr, err)
	}
	if count < 0 || count > 1 {
		return false, fmt.Errorf("%s: affected_rows=out_of_range", operationErr)
	}
	return count == 1, nil
}
