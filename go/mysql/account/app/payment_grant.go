package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	api "github.com/k4k3ru-hub/storage/go/api"
	validator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const DefaultPaymentGrantTableName = "account_app_payment_grants"

type PaymentGrantKey struct {
	Source     string
	IntentKind string
	IntentID   uint64
}
type PaymentGrant struct {
	PaymentGrantKey
	AccountID     uint64
	CreditTicks   uint64
	ExpiresInDays uint32
	ExpiresAt     *time.Time
	CreditID      uint64
	OperationID   uint64
	CreatedAt     time.Time
}
type PaymentGrantStore struct{ tableName string }

// NewPaymentGrantStore creates a store for immutable payment grant receipts.
//
// Version:
//   - 2026-09-10: Added.
func NewPaymentGrantStore(tableName string) (*PaymentGrantStore, error) {
	if err := validator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("failed to create payment grant store: %w", err)
	}
	return &PaymentGrantStore{tableName: tableName}, nil
}

// Validate validates the payment identity used for deduplication.
//
// Version:
//   - 2026-09-10: Added.
func (k PaymentGrantKey) Validate() error {
	for _, field := range []struct {
		name, value string
		limit       int
	}{{"source", k.Source, 32}, {"intent_kind", k.IntentKind, 16}} {
		if field.value == "" {
			return fmt.Errorf("failed to validate payment grant key: %s=empty", field.name)
		}
		if len(field.value) > field.limit {
			return fmt.Errorf("failed to validate payment grant key: %s=too_long", field.name)
		}
		if strings.Trim(field.value, "abcdefghijklmnopqrstuvwxyz0123456789-_") != "" {
			return fmt.Errorf("failed to validate payment grant key: %s=invalid", field.name)
		}
	}
	if k.IntentID == 0 {
		return fmt.Errorf("failed to validate payment grant key: intent_id=empty")
	}
	return nil
}

func (s *PaymentGrantStore) validate(ctx context.Context, db api.Executor) error {
	if s == nil {
		return fmt.Errorf("failed to validate payment grant store: store=null")
	}
	if ctx == nil {
		return fmt.Errorf("failed to validate payment grant store: context=null")
	}
	if db == nil {
		return fmt.Errorf("failed to validate payment grant store: executor=null")
	}
	if err := validator.ValidateSQLIdentifier(s.tableName, "table_name"); err != nil {
		return fmt.Errorf("failed to validate payment grant store: %w", err)
	}
	return nil
}

// CreateTable creates a receipt table; it does not backfill previous grants.
//
// Version:
//   - 2026-09-10: Added.
func (s *PaymentGrantStore) CreateTable(ctx context.Context, db api.Executor) error {
	if err := s.validate(ctx, db); err != nil {
		return err
	}
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 source VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 intent_kind VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 intent_id BIGINT UNSIGNED NOT NULL,
 account_id BIGINT UNSIGNED NOT NULL,
 credit_ticks BIGINT UNSIGNED NOT NULL,
 expires_in_days INT UNSIGNED NOT NULL,
 expires_at DATETIME(6) NULL,
 credit_id BIGINT UNSIGNED NOT NULL,
 operation_id BIGINT UNSIGNED NOT NULL,
 created_at DATETIME(6) NOT NULL,
 PRIMARY KEY (source,intent_kind,intent_id),
 UNIQUE KEY uk_payment_grant_credit (credit_id),
 KEY idx_payment_grant_account (account_id)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, s.tableName)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("failed to create payment grant table: %w", err)
	}
	return nil
}

// Insert inserts a receipt in the caller's credit transaction. Duplicate keys
// wrap api.ErrDuplicateKey; callers must compare the existing grant terms.
//
// Version:
//   - 2026-09-10: Added.
func (s *PaymentGrantStore) Insert(ctx context.Context, db api.Executor, p PaymentGrant) error {
	if err := s.validate(ctx, db); err != nil {
		return err
	}
	if err := p.PaymentGrantKey.Validate(); err != nil {
		return fmt.Errorf("failed to insert payment grant: %w", err)
	}
	if p.AccountID == 0 || p.CreditID == 0 || p.OperationID == 0 || p.CreditTicks == 0 {
		return fmt.Errorf("failed to insert payment grant: required_id_or_ticks=empty")
	}
	if p.CreatedAt.IsZero() {
		return fmt.Errorf("failed to insert payment grant: created_at=empty")
	}
	if (p.ExpiresInDays == 0) != (p.ExpiresAt == nil) {
		return fmt.Errorf("failed to insert payment grant: expiry=invalid")
	}
	q := fmt.Sprintf("INSERT INTO %s (source,intent_kind,intent_id,account_id,credit_ticks,expires_in_days,expires_at,credit_id,operation_id,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)", s.tableName)
	_, err := db.ExecContext(ctx, q, p.Source, p.IntentKind, p.IntentID, p.AccountID, p.CreditTicks, p.ExpiresInDays, p.ExpiresAt, p.CreditID, p.OperationID, p.CreatedAt)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("failed to insert payment grant: %w: %w", api.ErrDuplicateKey, err)
		}
		return fmt.Errorf("failed to insert payment grant: %w", err)
	}
	return nil
}

// Select returns a completed receipt, or nil when the payment has no receipt.
// Read outside a rolled-back transaction after a duplicate INSERT so that the
// winning transaction's committed receipt is visible.
//
// Version:
//   - 2026-09-10: Added.
func (s *PaymentGrantStore) Select(ctx context.Context, db api.Executor, key PaymentGrantKey) (*PaymentGrant, error) {
	if err := s.validate(ctx, db); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("failed to select payment grant: %w", err)
	}
	p := &PaymentGrant{PaymentGrantKey: key}
	q := fmt.Sprintf("SELECT account_id,credit_ticks,expires_in_days,expires_at,credit_id,operation_id,created_at FROM %s WHERE source=? AND intent_kind=? AND intent_id=?", s.tableName)
	err := db.QueryRowContext(ctx, q, key.Source, key.IntentKind, key.IntentID).Scan(&p.AccountID, &p.CreditTicks, &p.ExpiresInDays, &p.ExpiresAt, &p.CreditID, &p.OperationID, &p.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to select payment grant: %w", err)
	}
	return p, nil
}
