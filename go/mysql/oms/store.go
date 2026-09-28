package oms

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	api "github.com/k4k3ru-hub/storage/go/api"
	validator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultOrderTableName        = "oms_orders"
	DefaultOnchainEventTableName = "oms_order_execution_onchain_events"
	DefaultFeeTableName          = "oms_order_execution_fees"
	DefaultExecutionTableName    = "oms_order_executions"
	DefaultPnLTableName          = "oms_pnl"
)

//go:embed schema.proposed.sql
var reviewedSchema string

type Store struct{ orderTable, executionTable, onchainEventTable, feeTable, pnlTable string }

// NewStore composes an OMS store with explicit table names and no database ownership.
//
// Version:
//   - 2026-09-28: Preserve four-table composition; opt into PnL with NewStoreWithPnL.
//   - 2026-09-26: Separate execution snapshots and onchain event history.
func NewStore(orderTable, executionTable, onchainEventTable, feeTable string) (*Store, error) {
	names := []string{orderTable, executionTable, onchainEventTable, feeTable}
	seen := map[string]bool{}
	for _, name := range names {
		if err := validator.ValidateSQLIdentifier(name, "table_name"); err != nil {
			return nil, fmt.Errorf("failed to create oms store: %w", errors.Join(ErrInvalidParameter, err))
		}
		if len(name) > 64 {
			return nil, fmt.Errorf("failed to create oms store: %w", invalid("table_name", "too_long"))
		}
		if seen[strings.ToLower(name)] {
			return nil, fmt.Errorf("failed to create oms store: %w", invalid("table_name", "invalid"))
		}
		seen[strings.ToLower(name)] = true
	}
	return &Store{orderTable: orderTable, executionTable: executionTable, onchainEventTable: onchainEventTable, feeTable: feeTable}, nil
}

// NewStoreWithPnL composes an OMS store with a PnL checkpoint table and atomic source invalidation.
// All writers to these OMS tables must use this composition before publishing checkpoints.
//
// Version:
//   - 2026-09-28: Added.
func NewStoreWithPnL(orderTable, executionTable, onchainEventTable, feeTable, pnlTable string) (*Store, error) {
	s, err := NewStore(orderTable, executionTable, onchainEventTable, feeTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create oms pnl store: %w", err)
	}
	// Reuse the identifier, length and duplicate-name validation of the existing constructor.
	if _, err := NewStore(pnlTable, orderTable, executionTable, onchainEventTable); err != nil {
		return nil, fmt.Errorf("failed to create oms pnl store: %w", err)
	}
	if strings.EqualFold(pnlTable, feeTable) {
		return nil, fmt.Errorf("failed to create oms pnl store: %w", invalid("pnl_table", "invalid"))
	}
	s.pnlTable = pnlTable
	return s, nil
}

// NewDefaultStore composes a store using the reviewed OMS table names.
//
// Version:
//   - 2026-09-26: Separate execution snapshots and onchain event history.
func NewDefaultStore() (*Store, error) {
	return NewStore(DefaultOrderTableName, DefaultExecutionTableName, DefaultOnchainEventTableName, DefaultFeeTableName)
}

// CreateTables applies the reviewed initial DDL using an application-owned executor.
// MySQL DDL is not transactional; invoke this only from explicit migration code.
//
// Version:
//   - 2026-09-28: Include PnL only when explicitly composed with NewStoreWithPnL.
//   - 2026-09-26: Separate execution snapshots and onchain event history.
func (s *Store) CreateTables(ctx context.Context, executor api.Executor) error {
	if err := s.guard(ctx, executor); err != nil {
		return fmt.Errorf("failed to create oms tables: %w", err)
	}
	names := map[string]string{DefaultOrderTableName: s.orderTable, DefaultOnchainEventTableName: s.onchainEventTable, DefaultFeeTableName: s.feeTable, DefaultExecutionTableName: s.executionTable, DefaultPnLTableName: s.pnlTable}
	definition := reviewedSchema
	if s.pnlTable == "" {
		definition = strings.SplitN(definition, "CREATE TABLE oms_pnl (", 2)[0]
	}
	// Replace whole identifiers, never substrings of constraint names or supplied names.
	identifier := regexp.MustCompile(`\b(?:oms_orders|oms_order_execution_onchain_events|oms_order_execution_fees|oms_order_executions|oms_pnl)\b`)
	schema := identifier.ReplaceAllStringFunc(definition, func(name string) string { return quoted(names[name]) })
	if s.orderTable != DefaultOrderTableName || s.onchainEventTable != DefaultOnchainEventTableName || s.feeTable != DefaultFeeTableName || s.executionTable != DefaultExecutionTableName || (s.pnlTable != "" && s.pnlTable != DefaultPnLTableName) {
		hash := sha256.Sum256([]byte(s.orderTable + "/" + s.onchainEventTable + "/" + s.feeTable + "/" + s.executionTable + "/" + s.pnlTable))
		constraint := regexp.MustCompile(`CONSTRAINT ([a-zA-Z0-9_]+)`)
		schema = constraint.ReplaceAllStringFunc(schema, func(value string) string { return value + fmt.Sprintf("_%x", hash[:4]) })
	}
	// Statements in the embedded schema end at a line-final semicolon.
	// Preserve semicolons inside column COMMENT literals.
	lines := []string{}
	for _, line := range strings.Split(schema, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	for _, statement := range regexp.MustCompile(`(?m);[\t ]*$`).Split(strings.Join(lines, "\n"), -1) {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := executor.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("failed to create oms tables: %w", err)
		}
	}
	return nil
}
func quoted(name string) string { return "`" + name + "`" }
func (s *Store) guard(ctx context.Context, executor api.Executor) error {
	if s == nil || s.orderTable == "" || s.onchainEventTable == "" || s.feeTable == "" || s.executionTable == "" || ctx == nil || executor == nil {
		return invalid("dependency", "null")
	}
	value := reflect.ValueOf(executor)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return invalid("executor", "null")
	}
	return ctx.Err()
}
func writeError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var my *mysql.MySQLError
	if errors.As(err, &my) && my.Number == 1062 {
		err = errors.Join(ErrDuplicate, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
func utc(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }
func optionalTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := utc(*t)
	return &v
}
func created(t time.Time) time.Time {
	if t.IsZero() {
		t = time.Now()
	}
	return utc(t)
}
func sameString(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
func sameTime(a, b *time.Time) bool {
	return a == nil && b == nil || a != nil && b != nil && utc(*a).Equal(utc(*b))
}
func sameUint(a, b *uint64) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
func orderStateEqual(a, b OrderState) bool {
	return a.LastEventSequence == b.LastEventSequence && a.Status == b.Status && sameString(a.Quantity, b.Quantity) && a.FilledQuantity == b.FilledQuantity && sameString(a.FilledCounterQuantity, b.FilledCounterQuantity) && sameTime(a.CompletedAt, b.CompletedAt)
}
func identity(accountID, orderID uint64) error {
	if accountID == 0 || orderID == 0 {
		return invalid("identity", "empty")
	}
	return nil
}

type scanner interface{ Scan(...any) error }

// SelectOrderForUpdate locks an owned order in the caller's transaction.
//
// Version:
//   - 2026-09-26: Separate execution snapshots and onchain event history.
func (s *Store) SelectOrderForUpdate(ctx context.Context, tx *sql.Tx, accountID, orderID uint64) (*Order, error) {
	return s.selectOrder(ctx, tx, accountID, orderID, true)
}

// SelectOrder retrieves an owned order and preserves sql.ErrNoRows for missing records.
//
// Version:
//   - 2026-09-26: Separate execution snapshots and onchain event history.
func (s *Store) SelectOrder(ctx context.Context, executor api.Executor, accountID, orderID uint64) (*Order, error) {
	return s.selectOrder(ctx, executor, accountID, orderID, false)
}
func (s *Store) selectOrder(ctx context.Context, executor api.Executor, accountID, orderID uint64, lock bool) (*Order, error) {
	const op = "failed to select oms order"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	query := "SELECT " + orderColumns + " FROM " + quoted(s.orderTable) + " WHERE account_id=? AND id=?"
	if lock {
		query += " FOR UPDATE"
	}
	value, err := scanOrder(executor.QueryRowContext(ctx, query, accountID, orderID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}

// SelectOrderByIdempotencyKey finds a previously created order after a duplicate or lost response.
//
// Version:
//   - 2026-09-26: Separate execution snapshots and onchain event history.
func (s *Store) SelectOrderByIdempotencyKey(ctx context.Context, executor api.Executor, accountID uint64, key []byte) (*Order, error) {
	const op = "failed to select oms order by idempotency key"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if accountID == 0 {
		return nil, fmt.Errorf("%s: %w", op, invalid("account_id", "empty"))
	}
	if err := binaryKey("idempotency_key", key); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	value, err := scanOrder(executor.QueryRowContext(ctx, "SELECT "+orderColumns+" FROM "+quoted(s.orderTable)+" WHERE account_id=? AND idempotency_key=?", accountID, key))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}
