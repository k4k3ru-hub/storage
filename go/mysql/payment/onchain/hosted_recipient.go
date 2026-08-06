//
// hosted_recipient.go
//
package onchain

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	k4k3ruAPI "github.com/k4k3ru-hub/storage/go/api"
	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruInternalSQLScan "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultHostedRecipientTableName = "payment_onchain_hosted_recipients"
)

var hostedRecipientIDGenerator = &k4k3ruInternalGenerator.ID{}

type HostedRecipient struct {
	ID                  uint64
	AccountID           uint64
	Status              HostedRecipientStatus
	ChainFamily         string
	Address             string
	EncryptedPrivateKey string
	SecretProviderRef   string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type HostedRecipientStatus uint8

const (
	HostedRecipientStatusActive HostedRecipientStatus = iota + 1
	HostedRecipientStatusDisabled
	HostedRecipientStatusArchived
)

type HostedRecipientStore struct {
	tableName string
}

type HostedRecipientInsertParams struct {
	ID                  uint64
	AccountID           uint64
	Status              HostedRecipientStatus
	ChainFamily         string
	Address             string
	EncryptedPrivateKey string
	SecretProviderRef   string
	CreatedAt           time.Time
	Ignore              bool
}

type HostedRecipientUpdateParams struct {
	Status              *HostedRecipientStatus
	EncryptedPrivateKey *string
	SecretProviderRef   *string
}

//
// GenerateHostedRecipientID generates a hosted recipient ID.
//
// Returns:
//   - Generated hosted recipient ID.
//
// Version:
//   - 2026-08-06: Added.
//
func GenerateHostedRecipientID() uint64 {
	return hostedRecipientIDGenerator.Generate()
}

//
// NewHostedRecipientStore creates a hosted recipient store.
//
// Parameters:
//   - tableName: Hosted recipient table name.
//
// Returns:
//   - Hosted recipient store.
//
// Version:
//   - 2026-08-06: Added.
//
func NewHostedRecipientStore(tableName string) (*HostedRecipientStore, error) {
	operationErr := "failed to create payment onchain hosted recipient store"

	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	return &HostedRecipientStore{tableName: tableName}, nil
}

//
// ValidateHostedRecipientID validates a hosted recipient ID.
//
// Version:
//   - 2026-08-06: Added.
//
func ValidateHostedRecipientID(id uint64) error {
	if id == 0 {
		return fmt.Errorf("invalid parameter: id=0")
	}
	return nil
}

//
// ValidateHostedRecipientAccountID validates a hosted recipient account ID.
//
// Version:
//   - 2026-08-06: Added.
//
func ValidateHostedRecipientAccountID(accountID uint64) error {
	if accountID == 0 {
		return fmt.Errorf("invalid parameter: account_id=0")
	}
	return nil
}

//
// ValidateHostedRecipientChainFamily validates a hosted recipient chain family.
//
// Version:
//   - 2026-08-06: Added.
//
func ValidateHostedRecipientChainFamily(chainFamily string) error {
	chainFamily = strings.TrimSpace(chainFamily)
	if chainFamily == "" {
		return fmt.Errorf("invalid parameter: chain_family=empty")
	}
	if len(chainFamily) > 16 {
		return fmt.Errorf("invalid parameter: chain_family=too_long")
	}
	return nil
}

//
// ValidateHostedRecipientAddress validates a hosted recipient address.
//
// Version:
//   - 2026-08-06: Added.
//
func ValidateHostedRecipientAddress(address string) error {
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("invalid parameter: address=empty")
	}
	if len(address) > 255 {
		return fmt.Errorf("invalid parameter: address=too_long")
	}
	return nil
}

//
// ValidateHostedRecipientEncryptedPrivateKey validates encrypted private key data.
//
// Version:
//   - 2026-08-06: Added.
//
func ValidateHostedRecipientEncryptedPrivateKey(value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: encrypted_private_key=empty")
	}
	if len(value) > 4096 {
		return fmt.Errorf("invalid parameter: encrypted_private_key=too_long")
	}
	return nil
}

//
// ValidateHostedRecipientSecretProviderRef validates a secret provider reference.
//
// Version:
//   - 2026-08-06: Added.
//
func ValidateHostedRecipientSecretProviderRef(value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: secret_provider_ref=empty")
	}
	if len(value) > 128 {
		return fmt.Errorf("invalid parameter: secret_provider_ref=too_long")
	}
	return nil
}

//
// CreateTable creates the hosted recipient table.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStore) CreateTable(ctx context.Context, executor k4k3ruAPI.Executor) error {
	operationErr := "failed to create payment onchain hosted recipient table"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			%s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
			%s BIGINT UNSIGNED NOT NULL COMMENT 'Account ID',
			%s TINYINT UNSIGNED NOT NULL COMMENT 'Status',
			%s VARCHAR(16) NOT NULL COMMENT 'Chain family',
			%s VARCHAR(255) NOT NULL COMMENT 'Address',
			%s TEXT NOT NULL COMMENT 'Encrypted private key',
			%s VARCHAR(128) NOT NULL COMMENT 'Secret provider reference',
			%s DATETIME(6) NOT NULL COMMENT 'Created at',
			%s DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'Updated at',
			PRIMARY KEY (%s),
			UNIQUE KEY uq_hosted_recipient_account_chain (%s, %s),
			UNIQUE KEY uq_hosted_recipient_chain_address (%s, %s),
			KEY idx_hosted_recipient_status (%s)
		) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`,
		s.tableName,
		ColID, ColAccountID, ColStatus, ColChainFamily, ColAddress,
		ColEncryptedPrivateKey, ColSecretProviderRef, ColCreatedAt, ColUpdatedAt,
		ColID,
		ColAccountID, ColChainFamily,
		ColChainFamily, ColAddress,
		ColStatus,
	)

	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

//
// Insert inserts a hosted recipient.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStore) Insert(ctx context.Context, executor k4k3ruAPI.Executor, params *HostedRecipientInsertParams) error {
	operationErr := "failed to insert payment onchain hosted recipient"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: invalid parameter: hosted_recipient_insert_params=null", operationErr)
	}
	if params.ID == 0 {
		params.ID = GenerateHostedRecipientID()
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}

	prefix := "INSERT"
	if params.Ignore {
		prefix = "INSERT IGNORE"
	}
	query := fmt.Sprintf(
		"%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
		prefix, s.tableName, ColID, ColAccountID, ColStatus, ColChainFamily, ColAddress,
		ColEncryptedPrivateKey, ColSecretProviderRef, ColCreatedAt,
	)
	_, err := executor.ExecContext(ctx, query,
		params.ID, params.AccountID, params.Status, params.ChainFamily, params.Address,
		params.EncryptedPrivateKey, params.SecretProviderRef, params.CreatedAt,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, k4k3ruAPI.ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

//
// SelectByID selects a hosted recipient by ID.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStore) SelectByID(ctx context.Context, executor k4k3ruAPI.Executor, id uint64) (*HostedRecipient, error) {
	operationErr := "failed to select payment onchain hosted recipient by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := ValidateHostedRecipientID(id); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColID)
	return scanHostedRecipient(executor.QueryRowContext(ctx, query, id), operationErr)
}

//
// SelectByAccountIDAndChainFamily selects a hosted recipient by account ID and chain family.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStore) SelectByAccountIDAndChainFamily(ctx context.Context, executor k4k3ruAPI.Executor, accountID uint64, chainFamily string) (*HostedRecipient, error) {
	operationErr := "failed to select payment onchain hosted recipient by account id and chain family"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := validateHostedRecipientLookup(accountID, chainFamily); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? AND %s = ? LIMIT 1;", ColAccountID, ColChainFamily)
	return scanHostedRecipient(executor.QueryRowContext(ctx, query, accountID, chainFamily), operationErr)
}

//
// SelectForUpdateByAccountIDAndChainFamily selects and locks a hosted recipient.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStore) SelectForUpdateByAccountIDAndChainFamily(ctx context.Context, tx *sql.Tx, accountID uint64, chainFamily string) (*HostedRecipient, error) {
	operationErr := "failed to select payment onchain hosted recipient for update by account id and chain family"
	if s == nil || s.tableName == "" {
		return nil, fmt.Errorf("%s: invalid parameter: hosted_recipient_store=null_or_empty", operationErr)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%s: invalid parameter: context=null", operationErr)
	}
	if tx == nil {
		return nil, fmt.Errorf("%s: invalid parameter: sql_tx=null", operationErr)
	}
	if err := validateHostedRecipientLookup(accountID, chainFamily); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? AND %s = ? LIMIT 1 FOR UPDATE;", ColAccountID, ColChainFamily)
	return scanHostedRecipient(tx.QueryRowContext(ctx, query, accountID, chainFamily), operationErr)
}

//
// DeleteByID deletes a hosted recipient by ID.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStore) DeleteByID(ctx context.Context, executor k4k3ruAPI.Executor, id uint64) error {
	operationErr := "failed to delete payment onchain hosted recipient by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if err := ValidateHostedRecipientID(id); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", s.tableName, ColID)
	if _, err := executor.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

//
// UpdateByID updates a hosted recipient by ID.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStore) UpdateByID(ctx context.Context, executor k4k3ruAPI.Executor, params HostedRecipientUpdateParams, id uint64) error {
	operationErr := "failed to update payment onchain hosted recipient by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if err := ValidateHostedRecipientID(id); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	assignments, args := params.BuildAssignments()
	if len(assignments) == 0 {
		return fmt.Errorf("%s: invalid parameter: assignments=empty", operationErr)
	}
	args = append(args, id)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?;", s.tableName, strings.Join(assignments, ", "), ColID)
	if _, err := executor.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

//
// IsValid reports whether the hosted recipient status is valid.
//
// Version:
//   - 2026-08-06: Added.
//
func (s HostedRecipientStatus) IsValid() bool {
	switch s {
	case HostedRecipientStatusActive, HostedRecipientStatusDisabled, HostedRecipientStatusArchived:
		return true
	default:
		return false
	}
}

//
// Validate validates the hosted recipient status.
//
// Version:
//   - 2026-08-06: Added.
//
func (s HostedRecipientStatus) Validate() error {
	if !s.IsValid() {
		return fmt.Errorf("invalid parameter: hosted_recipient_status=%d", s)
	}
	return nil
}

//
// Value returns the hosted recipient status as a driver.Value.
//
// Version:
//   - 2026-08-06: Added.
//
func (s HostedRecipientStatus) Value() (driver.Value, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return int64(s), nil
}

//
// Scan scans a hosted recipient status.
//
// Version:
//   - 2026-08-06: Added.
//
func (s *HostedRecipientStatus) Scan(value any) error {
	if s == nil {
		return fmt.Errorf("failed to scan hosted recipient status: invalid parameter: hosted_recipient_status=null")
	}
	v, err := k4k3ruInternalSQLScan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan hosted recipient status: %w", err)
	}
	result := HostedRecipientStatus(v)
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan hosted recipient status: %w", err)
	}
	*s = result
	return nil
}

//
// Validate validates hosted recipient insert parameters.
//
// Version:
//   - 2026-08-06: Added.
//
func (p *HostedRecipientInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: hosted_recipient_insert_params=null")
	}
	if err := ValidateHostedRecipientID(p.ID); err != nil {
		return err
	}
	if err := ValidateHostedRecipientAccountID(p.AccountID); err != nil {
		return err
	}
	if err := p.Status.Validate(); err != nil {
		return err
	}
	if err := ValidateHostedRecipientChainFamily(p.ChainFamily); err != nil {
		return err
	}
	if err := ValidateHostedRecipientAddress(p.Address); err != nil {
		return err
	}
	if err := ValidateHostedRecipientEncryptedPrivateKey(p.EncryptedPrivateKey); err != nil {
		return err
	}
	return ValidateHostedRecipientSecretProviderRef(p.SecretProviderRef)
}

//
// BuildAssignments builds hosted recipient UPDATE assignments and arguments.
//
// Version:
//   - 2026-08-06: Added.
//
func (p HostedRecipientUpdateParams) BuildAssignments() ([]string, []any) {
	assignments := make([]string, 0, 3)
	args := make([]any, 0, 3)
	if p.Status != nil {
		assignments = append(assignments, ColStatus+"=?")
		args = append(args, *p.Status)
	}
	if p.EncryptedPrivateKey != nil {
		assignments = append(assignments, ColEncryptedPrivateKey+"=?")
		args = append(args, *p.EncryptedPrivateKey)
	}
	if p.SecretProviderRef != nil {
		assignments = append(assignments, ColSecretProviderRef+"=?")
		args = append(args, *p.SecretProviderRef)
	}
	return assignments, args
}

//
// Validate validates hosted recipient update parameters.
//
// Version:
//   - 2026-08-06: Added.
//
func (p HostedRecipientUpdateParams) Validate() error {
	if p.Status != nil {
		if err := p.Status.Validate(); err != nil {
			return err
		}
	}
	if p.EncryptedPrivateKey != nil {
		if err := ValidateHostedRecipientEncryptedPrivateKey(*p.EncryptedPrivateKey); err != nil {
			return err
		}
	}
	if p.SecretProviderRef != nil {
		if err := ValidateHostedRecipientSecretProviderRef(*p.SecretProviderRef); err != nil {
			return err
		}
	}
	return nil
}

func (s *HostedRecipientStore) validateOperation(ctx context.Context, executor k4k3ruAPI.Executor, operationErr string) error {
	if s == nil {
		return fmt.Errorf("%s: invalid parameter: hosted_recipient_store=null", operationErr)
	}
	if s.tableName == "" {
		return fmt.Errorf("%s: invalid parameter: table_name=empty", operationErr)
	}
	if ctx == nil {
		return fmt.Errorf("%s: invalid parameter: context=null", operationErr)
	}
	if executor == nil {
		return fmt.Errorf("%s: invalid parameter: executor=null", operationErr)
	}
	return nil
}

func (s *HostedRecipientStore) selectClause() string {
	return fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s",
		ColID, ColAccountID, ColStatus, ColChainFamily, ColAddress, ColEncryptedPrivateKey,
		ColSecretProviderRef, ColCreatedAt, ColUpdatedAt, s.tableName,
	)
}

func validateHostedRecipientLookup(accountID uint64, chainFamily string) error {
	if err := ValidateHostedRecipientAccountID(accountID); err != nil {
		return err
	}
	return ValidateHostedRecipientChainFamily(chainFamily)
}

func scanHostedRecipient(row *sql.Row, operationErr string) (*HostedRecipient, error) {
	result := &HostedRecipient{}
	if err := row.Scan(
		&result.ID, &result.AccountID, &result.Status, &result.ChainFamily, &result.Address,
		&result.EncryptedPrivateKey, &result.SecretProviderRef, &result.CreatedAt, &result.UpdatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return result, nil
}
