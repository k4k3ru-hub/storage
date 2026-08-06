//
// usage_credit_event.go
//
package app

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	k4k3ruAPI "github.com/k4k3ru-hub/storage/go/api"
	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruInternalSQLScan "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultUsageCreditEventTableName = "account_app_usage_credit_events"
)

var (
	usageCreditEventIDGenerator          = &k4k3ruInternalGenerator.ID{}
	usageCreditEventOperationIDGenerator = &k4k3ruInternalGenerator.ID{}
)

type UsageCreditEvent struct {
	ID          uint64
	OperationID uint64
	AccountID   uint64
	CreditID    uint64
	Type        UsageCreditEventType
	DeltaTicks  int64
	Description *string
	MetaData    *string
	CreatedAt   time.Time
}

type UsageCreditEventType uint8

const (
	UsageCreditEventTypeGranted UsageCreditEventType = iota + 1
	UsageCreditEventTypeAPIRequestConsumed
	UsageCreditEventTypeSubscriptionConsumed
	UsageCreditEventTypeRefunded
	UsageCreditEventTypeExpired
	UsageCreditEventTypeAdjusted
)

type UsageCreditEventStore struct {
	tableName       string
	creditTableName string
}

type UsageCreditEventInsertParams struct {
	ID          uint64
	OperationID uint64
	AccountID   uint64
	CreditID    uint64
	Type        UsageCreditEventType
	DeltaTicks  int64
	Description *string
	MetaData    *string
	CreatedAt   time.Time
	Ignore      bool
}

//
// GenerateUsageCreditEventID generates a usage credit event ID.
//
// Version:
//   - 2026-07-26: Added.
//
func GenerateUsageCreditEventID() uint64 {
	return usageCreditEventIDGenerator.Generate()
}

//
// GenerateUsageCreditEventOperationID generates a usage credit event operation ID.
//
// Version:
//   - 2026-07-26: Added.
//
func GenerateUsageCreditEventOperationID() uint64 {
	return usageCreditEventOperationIDGenerator.Generate()
}

//
// NewUsageCreditEventStore creates a usage credit event store.
//
// Version:
//   - 2026-07-26: Added.
//
func NewUsageCreditEventStore(tableName, creditTableName string) (*UsageCreditEventStore, error) {
	operationErr := "failed to create account app usage credit event store"

	// Guard.
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	creditTableName = strings.TrimSpace(creditTableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(creditTableName, "credit_table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	return &UsageCreditEventStore{
		tableName:       tableName,
		creditTableName: creditTableName,
	}, nil
}

//
// ValidateUsageCreditEventID validates a usage credit event ID.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditEventID(id uint64) error {
	if id == 0 {
		return fmt.Errorf("invalid parameter: id=0")
	}
	return nil
}

//
// ValidateID validates the usage credit event ID.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCreditEvent) ValidateID() error {
	if e == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event=null")
	}
	return ValidateUsageCreditEventID(e.ID)
}

//
// ValidateUsageCreditEventOperationID validates a usage credit event operation ID.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditEventOperationID(operationID uint64) error {
	if operationID == 0 {
		return fmt.Errorf("invalid parameter: operation_id=0")
	}
	return nil
}

//
// ValidateOperationID validates the usage credit event operation ID.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCreditEvent) ValidateOperationID() error {
	if e == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event=null")
	}
	return ValidateUsageCreditEventOperationID(e.OperationID)
}

//
// ValidateUsageCreditEventAccountID validates a usage credit event account ID.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditEventAccountID(accountID uint64) error {
	if accountID == 0 {
		return fmt.Errorf("invalid parameter: account_id=0")
	}
	return nil
}

//
// ValidateAccountID validates the usage credit event account ID.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCreditEvent) ValidateAccountID() error {
	if e == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event=null")
	}
	return ValidateUsageCreditEventAccountID(e.AccountID)
}

//
// ValidateUsageCreditEventCreditID validates a usage credit event credit ID.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditEventCreditID(creditID uint64) error {
	if creditID == 0 {
		return fmt.Errorf("invalid parameter: credit_id=0")
	}
	return nil
}

//
// ValidateCreditID validates the usage credit event credit ID.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCreditEvent) ValidateCreditID() error {
	if e == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event=null")
	}
	return ValidateUsageCreditEventCreditID(e.CreditID)
}

//
// ValidateUsageCreditEventType validates a usage credit event type.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditEventType(t UsageCreditEventType) error {
	if err := t.Validate(); err != nil {
		return err
	}
	return nil
}

//
// ValidateType validates the usage credit event type.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCreditEvent) ValidateType() error {
	if e == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event=null")
	}
	return ValidateUsageCreditEventType(e.Type)
}

//
// ValidateUsageCreditEventDescription validates a usage credit event description.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditEventDescription(description *string) error {
	if description == nil {
		return nil
	}
	if utf8.RuneCountInString(*description) > 255 {
		return fmt.Errorf("invalid parameter: description=too_long")
	}
	return nil
}

//
// ValidateDescription validates the usage credit event description.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCreditEvent) ValidateDescription() error {
	if e == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event=null")
	}
	return ValidateUsageCreditEventDescription(e.Description)
}

//
// ValidateUsageCreditEventMetaData validates usage credit event metadata.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditEventMetaData(metaData *string) error {
	if metaData == nil {
		return nil
	}
	if len([]byte(*metaData)) > 4096 {
		return fmt.Errorf("invalid parameter: meta_data=too_long")
	}
	return nil
}

//
// ValidateMetaData validates the usage credit event metadata.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCreditEvent) ValidateMetaData() error {
	if e == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event=null")
	}
	return ValidateUsageCreditEventMetaData(e.MetaData)
}

//
// CreateTable creates the usage credit event table.
//
// Version:
//   - 2026-07-26: Added.
//
func (s *UsageCreditEventStore) CreateTable(ctx context.Context, executor k4k3ruAPI.Executor) error {
	operationErr := "failed to create usage credit event table"

	// Guard.
	if s == nil {
		return fmt.Errorf("%s: invalid parameter: usage_credit_event_store=null", operationErr)
	}
	if s.tableName == "" {
		return fmt.Errorf("%s: invalid parameter: table_name=empty", operationErr)
	}
	if s.creditTableName == "" {
		return fmt.Errorf("%s: invalid parameter: credit_table_name=empty", operationErr)
	}
	if ctx == nil {
		return fmt.Errorf("%s: invalid parameter: context=null", operationErr)
	}
	if executor == nil {
		return fmt.Errorf("%s: invalid parameter: executor=null", operationErr)
	}
	// Generate CREATE TABLE query.
	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
            %s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
            %s BIGINT UNSIGNED NOT NULL COMMENT 'Operation ID',
            %s BIGINT UNSIGNED NOT NULL COMMENT 'Account ID',
            %s BIGINT UNSIGNED NOT NULL COMMENT 'Credit ID',
            %s TINYINT UNSIGNED NOT NULL COMMENT 'Type',
            %s BIGINT NOT NULL COMMENT 'Balance delta ticks',
            %s VARCHAR(255) NULL COMMENT 'Description',
            %s TEXT NULL COMMENT 'Meta data',
            %s DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created at',
            PRIMARY KEY (%s),
            KEY idx_%s_account_operation (%s, %s),
            KEY idx_%s_account_credit (%s, %s),
            KEY idx_%s_credit_account (%s, %s),
            CONSTRAINT fk_%s_credit_id FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE CASCADE ON UPDATE RESTRICT
        ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`,
		s.tableName,
		ColID,
		ColOperationID,
		ColAccountID,
		ColCreditID,
		ColType,
		ColDeltaTicks,
		ColDescription,
		ColMetaData,
		ColCreatedAt,
		ColID,
		s.tableName, ColAccountID, ColOperationID,
		s.tableName, ColAccountID, ColCreditID,
		s.tableName, ColCreditID, ColAccountID,
		s.tableName, ColCreditID, s.creditTableName, ColID,
	)

	// Execute query.
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}

	return nil
}

//
// DeleteByID deletes a usage credit event by ID.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageCreditEventStore) DeleteByID(ctx context.Context, executor k4k3ruAPI.Executor, id uint64) error {
	operationErr := "failed to delete usage credit event by id"

	// Guard.
	if s == nil {
		return fmt.Errorf("%s: invalid parameter: usage_credit_event_store=null", operationErr)
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
	if id == 0 {
		return fmt.Errorf("%s: invalid parameter: id=0", operationErr)
	}

	// Generate DELETE query.
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", s.tableName, ColID)

	// Execute query.
	if _, err := executor.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}

	return nil
}

//
// Insert inserts a usage credit event.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageCreditEventStore) Insert(ctx context.Context, executor k4k3ruAPI.Executor, params *UsageCreditEventInsertParams) error {
	operationErr := "failed to insert usage credit event"

	// Guard.
	if s == nil {
		return fmt.Errorf("%s: invalid parameter: usage_credit_event_store=null", operationErr)
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
	if params == nil {
		return fmt.Errorf("%s: invalid parameter: usage_credit_event_insert_params=null", operationErr)
	}

	// Apply defaults.
	if params.ID == 0 {
		params.ID = GenerateUsageCreditEventID()
	}
	if params.OperationID == 0 {
		params.OperationID = GenerateUsageCreditEventOperationID()
	}
	now := time.Now().UTC()
	if params.CreatedAt.IsZero() {
		params.CreatedAt = now
	}

	// Validate params.
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}

	// Generate INSERT query.
	queryPrefix := "INSERT"
	if params.Ignore {
		queryPrefix = "INSERT IGNORE"
	}

	query := fmt.Sprintf(
		"%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
		queryPrefix,
		s.tableName,
		ColID,
		ColOperationID,
		ColAccountID,
		ColCreditID,
		ColType,
		ColDeltaTicks,
		ColDescription,
		ColMetaData,
		ColCreatedAt,
	)

	// Execute query.
	if _, err := executor.ExecContext(
		ctx,
		query,
		params.ID,
		params.OperationID,
		params.AccountID,
		params.CreditID,
		params.Type,
		params.DeltaTicks,
		params.Description,
		params.MetaData,
		params.CreatedAt,
	); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, k4k3ruAPI.ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}

	return nil
}

//
// SelectByAccountIDAndOperationID selects usage credit events by account ID and operation ID.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageCreditEventStore) SelectByAccountIDAndOperationID(ctx context.Context, executor k4k3ruAPI.Executor, accountID, operationID uint64) ([]*UsageCreditEvent, error) {
	operationErr := "failed to select usage credit event by account id and operation id"

	// Guard.
	if s == nil {
		return nil, fmt.Errorf("%s: invalid parameter: usage_credit_event_store=null", operationErr)
	}
	if s.tableName == "" {
		return nil, fmt.Errorf("%s: invalid parameter: table_name=empty", operationErr)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%s: invalid parameter: context=null", operationErr)
	}
	if executor == nil {
		return nil, fmt.Errorf("%s: invalid parameter: executor=null", operationErr)
	}

	// Validate.
	if err := ValidateUsageCreditEventAccountID(accountID); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := ValidateUsageCreditEventOperationID(operationID); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	// Generate SELECT query.
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? AND %s = ?;", s.tableName, ColAccountID, ColOperationID)

	// Execute query.
	rows, err := executor.QueryContext(ctx, query, accountID, operationID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	defer rows.Close()

	// Scan.
	var result []*UsageCreditEvent
	for rows.Next() {
		row := &UsageCreditEvent{}
		if err := rows.Scan(
			&row.ID,
			&row.OperationID,
			&row.AccountID,
			&row.CreditID,
			&row.Type,
			&row.DeltaTicks,
			&row.Description,
			&row.MetaData,
			&row.CreatedAt,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("%s: %w", operationErr, err)
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	return result, nil
}

//
// SelectByAccountIDAndCreditID selects usage credit events by account ID and credit ID.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageCreditEventStore) SelectByAccountIDAndCreditID(ctx context.Context, executor k4k3ruAPI.Executor, accountID, creditID uint64) ([]*UsageCreditEvent, error) {
	operationErr := "failed to select usage credit event by account id and credit id"

	// Guard.
	if s == nil {
		return nil, fmt.Errorf("%s: invalid parameter: usage_credit_event_store=null", operationErr)
	}
	if s.tableName == "" {
		return nil, fmt.Errorf("%s: invalid parameter: table_name=empty", operationErr)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%s: invalid parameter: context=null", operationErr)
	}
	if executor == nil {
		return nil, fmt.Errorf("%s: invalid parameter: executor=null", operationErr)
	}

	// Validate.
	if err := ValidateUsageCreditEventAccountID(accountID); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := ValidateUsageCreditEventCreditID(creditID); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	// Generate SELECT query.
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? AND %s = ?;", s.tableName, ColAccountID, ColCreditID)

	// Execute query.
	rows, err := executor.QueryContext(ctx, query, accountID, creditID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	defer rows.Close()

	// Scan.
	var result []*UsageCreditEvent
	for rows.Next() {
		row := &UsageCreditEvent{}
		if err := rows.Scan(
			&row.ID,
			&row.OperationID,
			&row.AccountID,
			&row.CreditID,
			&row.Type,
			&row.DeltaTicks,
			&row.Description,
			&row.MetaData,
			&row.CreatedAt,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("%s: %w", operationErr, err)
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	return result, nil
}

//
// IsValid reports whether the usage credit event type is valid.
//
// Version:
//   - 2026-07-26: Added.
//
func (t UsageCreditEventType) IsValid() bool {
	switch t {
	case UsageCreditEventTypeGranted,
		UsageCreditEventTypeAPIRequestConsumed,
		UsageCreditEventTypeSubscriptionConsumed,
		UsageCreditEventTypeRefunded,
		UsageCreditEventTypeExpired,
		UsageCreditEventTypeAdjusted:
		return true
	default:
		return false
	}
}

//
// String returns the usage credit event type as a string.
//
// Version:
//   - 2026-07-26: Added.
//
func (t UsageCreditEventType) String() string {
	switch t {
	case UsageCreditEventTypeGranted:
		return "granted"
	case UsageCreditEventTypeAPIRequestConsumed:
		return "api-request-consumed"
	case UsageCreditEventTypeSubscriptionConsumed:
		return "subscription-consumed"
	case UsageCreditEventTypeRefunded:
		return "refunded"
	case UsageCreditEventTypeExpired:
		return "expired"
	case UsageCreditEventTypeAdjusted:
		return "adjusted"
	default:
		return ""
	}
}

//
// Validate validates the usage credit event type.
//
// Version:
//   - 2026-07-26: Added.
//
func (t UsageCreditEventType) Validate() error {
	if !t.IsValid() {
		return fmt.Errorf("invalid parameter: usage_credit_event_type=%d", t)
	}
	return nil
}

//
// Value returns the usage credit event type as a driver.Value.
//
// Version:
//   - 2026-07-26: Added.
//
func (t UsageCreditEventType) Value() (driver.Value, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}

	return int64(t), nil
}

//
// Scan scans a usage credit event type.
//
// Version:
//   - 2026-07-26: Added.
//
func (t *UsageCreditEventType) Scan(value any) error {
	if t == nil {
		return fmt.Errorf("failed to scan usage credit event type: invalid parameter: usage_credit_event_type=null")
	}

	v, err := k4k3ruInternalSQLScan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan usage credit event type: %w", err)
	}

	result := UsageCreditEventType(v)
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan usage credit event type: %w", err)
	}

	*t = result

	return nil
}

//
// Validate validates usage credit event insert parameters.
//
// Version:
//   - 2026-07-27: Added.
//
func (p *UsageCreditEventInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: usage_credit_event_insert_params=null")
	}
	if err := ValidateUsageCreditEventID(p.ID); err != nil {
		return err
	}
	if err := ValidateUsageCreditEventOperationID(p.OperationID); err != nil {
		return err
	}
	if err := ValidateUsageCreditEventAccountID(p.AccountID); err != nil {
		return err
	}
	if err := ValidateUsageCreditEventCreditID(p.CreditID); err != nil {
		return err
	}
	if err := ValidateUsageCreditEventType(p.Type); err != nil {
		return err
	}
	if err := ValidateUsageCreditEventDescription(p.Description); err != nil {
		return err
	}
	if err := ValidateUsageCreditEventMetaData(p.MetaData); err != nil {
		return err
	}
	return nil
}
