package oms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	api "github.com/k4k3ru-hub/storage/go/api"
)

// InsertExecution records one execution stage or individual fill under a locked owned order.
// It never changes the order state or sums fills. The caller must roll back on error.
//
// Version:
//   - 2026-09-20: Added.
func (s *Store) InsertExecution(ctx context.Context, tx *sql.Tx, accountID uint64, value Execution) (uint64, error) {
	const op = "failed to insert oms execution"
	if err := s.guard(ctx, tx); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	if value.ID == 0 {
		value.ID = GenerateExecutionID()
	}
	value.CreatedAt = created(value.CreatedAt)
	value.UpdatedAt = value.CreatedAt
	value.OccurredAt = utc(value.OccurredAt)
	value.ExpiresAt = optionalTime(value.ExpiresAt)
	if err := value.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	if _, err := s.SelectOrderForUpdate(ctx, tx, accountID, value.OrderID); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+quoted(s.executionTable)+" ("+executionColumns+") VALUES ("+executionPlaceholders+")", executionArgs(value)...)
	if err != nil {
		return 0, writeError(op, err)
	}
	return value.ID, nil
}

// SelectExecutionByKey retrieves one owned execution record by its stable order-scoped key.
//
// Version:
//   - 2026-09-20: Added.
func (s *Store) SelectExecutionByKey(ctx context.Context, executor api.Executor, accountID, orderID uint64, key []byte) (*Execution, error) {
	return s.selectExecution(ctx, executor, accountID, orderID, key, false)
}

// SelectExecutionForUpdate locks the parent before the record for consistent write ordering.
//
// Version:
//   - 2026-09-20: Added.
func (s *Store) SelectExecutionForUpdate(ctx context.Context, tx *sql.Tx, accountID, orderID uint64, key []byte) (*Execution, error) {
	if _, err := s.SelectOrderForUpdate(ctx, tx, accountID, orderID); err != nil {
		return nil, fmt.Errorf("failed to lock oms execution: %w", err)
	}
	return s.selectExecution(ctx, tx, accountID, orderID, key, true)
}
func (s *Store) selectExecution(ctx context.Context, executor api.Executor, accountID, orderID uint64, key []byte, lock bool) (*Execution, error) {
	const op = "failed to select oms execution"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := binaryKey("record_key", key); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	columns := "e." + strings.ReplaceAll(executionColumns, ",", ",e.")
	query := "SELECT " + columns + " FROM " + quoted(s.executionTable) + " e JOIN " + quoted(s.orderTable) + " o ON o.id=e.order_id WHERE o.account_id=? AND o.id=? AND e.record_key=?"
	if lock {
		query += " FOR UPDATE"
	}
	value, err := scanExecution(executor.QueryRowContext(ctx, query, accountID, orderID, key))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}

// ListExecutions reads an ID-ordered page scoped to an owned order.
// Use the last returned ID as afterID; limit must be between 1 and 200.
//
// Version:
//   - 2026-09-20: Added.
func (s *Store) ListExecutions(ctx context.Context, executor api.Executor, accountID, orderID, afterID uint64, limit uint32) (result []Execution, returnErr error) {
	const op = "failed to list oms executions"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if limit == 0 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	if _, err := s.SelectOrder(ctx, executor, accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	columns := "e." + strings.ReplaceAll(executionColumns, ",", ",e.")
	rows, err := executor.QueryContext(ctx, "SELECT "+columns+" FROM "+quoted(s.executionTable)+" e JOIN "+quoted(s.orderTable)+" o ON o.id=e.order_id WHERE o.account_id=? AND o.id=? AND e.id>? ORDER BY e.id LIMIT ?", accountID, orderID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			returnErr = joinReadError(returnErr, op, err)
		}
	}()
	result = make([]Execution, 0)
	for rows.Next() {
		value, err := scanExecution(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		result = append(result, *value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return result, nil
}

// UpdateExecutionState stores a caller-decided correction only if the locked record matches expected.
// Identity and stage fields remain immutable. Order aggregation belongs to the caller's transaction.
//
// Version:
//   - 2026-09-20: Added.
func (s *Store) UpdateExecutionState(ctx context.Context, tx *sql.Tx, accountID, orderID uint64, key []byte, expected, next ExecutionState) error {
	const op = "failed to update oms execution state"
	current, err := s.SelectExecutionForUpdate(ctx, tx, accountID, orderID, key)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if !executionStateEqual(current.ExecutionState, expected) {
		return fmt.Errorf("%s: %w", op, ErrConflict)
	}
	current.ExecutionState = next
	if err := current.Validate(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+quoted(s.executionTable)+" SET status=?,execution_id=?,quantity=?,counter_quantity=?,source_version=?,occurred_at=?,expires_at=? WHERE order_id=? AND id=?", next.Status, next.ExecutionID, next.Quantity, next.CounterQuantity, next.SourceVersion, utc(next.OccurredAt), optionalTime(next.ExpiresAt), orderID, current.ID)
	return writeError(op, err)
}

func joinReadError(previous error, operation string, err error) error {
	return errors.Join(previous, fmt.Errorf("%s: %w", operation, err))
}
