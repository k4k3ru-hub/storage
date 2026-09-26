package oms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	api "github.com/k4k3ru-hub/storage/go/api"
	"strings"
)

// AppendExecution appends a fact, evidence and fees and rebuilds the locked order snapshot.
// It allocates sequence and zero IDs. An identical key/content returns the original record.
// The caller owns commit and must roll back the entire transaction on any error.
// No RPC or network operation may be performed while holding this transaction.
//
// Version:
//   - 2026-09-26: Replace execution mutation with atomic history append.
func (s *Store) AppendExecution(ctx context.Context, tx *sql.Tx, accountID uint64, record ExecutionRecord) (*AppendResult, error) {
	const op = "failed to append oms execution"
	order, err := s.SelectOrderForUpdate(ctx, tx, accountID, record.Execution.OrderID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	history, err := s.loadHistory(ctx, tx, order.ID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	facts := make([]Execution, len(history))
	for i, r := range history {
		facts[i] = r.Execution
	}
	before, err := ReplayOrder(*order, facts)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if !orderStateEqual(order.OrderState, before.State) {
		return nil, fmt.Errorf("%s: %w: snapshot=invalid", op, ErrConflict)
	}
	var existing *ExecutionRecord
	for i := range history {
		if string(history[i].Execution.RecordKey) == string(record.Execution.RecordKey) {
			existing = &history[i]
			break
		}
	}
	normalized, err := normalizeRecord(record, order.LastExecutionSequence+1, existing)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if existing != nil {
		equal, err := recordsEqual(normalized, *existing)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		if !equal {
			return nil, fmt.Errorf("%s: %w: record_key=invalid", op, ErrConflict)
		}
		return &AppendResult{ExecutionID: existing.Execution.ID, Sequence: existing.Execution.Sequence, Duplicate: true, State: order.OrderState}, nil
	}
	facts = append(facts, normalized.Execution)
	next, err := ReplayOrder(*order, facts)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := validateChildren(normalized, history); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if _, err := tx.ExecContext(ctx, insertSQL(s.executionTable, executionColumns), executionArgs(normalized.Execution)...); err != nil {
		return nil, writeError(op, err)
	}
	if d := normalized.Onchain; d != nil {
		if _, err := tx.ExecContext(ctx, insertSQL(s.onchainDetailTable, onchainDetailColumns), onchainDetailArgs(*d)...); err != nil {
			return nil, writeError(op, err)
		}
	}
	for _, f := range normalized.Fees {
		if _, err := tx.ExecContext(ctx, insertSQL(s.feeTable, executionFeeColumns), executionFeeArgs(f)...); err != nil {
			return nil, writeError(op, err)
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+quoted(s.orderTable)+" SET status=?,filled_quantity=?,completed_at=?,last_execution_sequence=? WHERE account_id=? AND id=?", next.State.Status, next.State.FilledQuantity, optionalTime(next.State.CompletedAt), next.State.LastExecutionSequence, accountID, order.ID)
	if err != nil {
		return nil, writeError(op, err)
	}
	return &AppendResult{ExecutionID: normalized.Execution.ID, Sequence: normalized.Execution.Sequence, State: next.State}, nil
}

// SelectExecutionByKey retrieves an owned fact by its stable order-local key.
//
// Version:
//   - 2026-09-26: Read immutable facts from the new schema.
func (s *Store) SelectExecutionByKey(ctx context.Context, executor api.Executor, accountID, orderID uint64, key []byte) (*Execution, error) {
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
	value, err := scanExecution(executor.QueryRowContext(ctx, "SELECT "+prefixed("e", executionColumns)+" FROM "+quoted(s.executionTable)+" e JOIN "+quoted(s.orderTable)+" o ON o.id=e.order_id WHERE o.account_id=? AND o.id=? AND e.record_key=?", accountID, orderID, key))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}

// ListExecutions lists immutable facts ordered by sequence, after an exclusive sequence cursor.
// Limits range from 1 through 200; use a transaction for a consistent multi-page view.
//
// Version:
//   - 2026-09-26: Replace ID pagination with order-local sequence pagination.
func (s *Store) ListExecutions(ctx context.Context, executor api.Executor, accountID, orderID, afterSequence uint64, limit int) ([]Execution, error) {
	const op = "failed to list oms executions"
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	if _, err := s.SelectOrder(ctx, executor, accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	rows, err := executor.QueryContext(ctx, "SELECT "+executionColumns+" FROM "+quoted(s.executionTable)+" WHERE order_id=? AND sequence>? ORDER BY sequence LIMIT ?", orderID, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanExecution)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

// SelectOnchainDetail retrieves owned evidence without loading the recovery payload.
//
// Version:
//   - 2026-09-26: Added.
func (s *Store) SelectOnchainDetail(ctx context.Context, executor api.Executor, accountID, orderID, recordID uint64) (*OnchainDetail, error) {
	const op = "failed to select oms onchain detail"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	columns := strings.Replace(prefixed("d", onchainDetailColumns), "d.tx_payload", "NULL", 1)
	value, err := scanOnchainDetail(executor.QueryRowContext(ctx, "SELECT "+columns+" FROM "+quoted(s.onchainDetailTable)+" d JOIN "+quoted(s.orderTable)+" o ON o.id=d.order_id WHERE o.account_id=? AND o.id=? AND d.execution_record_id=?", accountID, orderID, recordID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}

// SelectSubmissionPayload retrieves owned recovery material for an accepted submission only.
// Keep the returned bytes inside the recovery worker; never include them in API responses or logs.
//
// Version:
//   - 2026-09-26: Added.
func (s *Store) SelectSubmissionPayload(ctx context.Context, executor api.Executor, accountID, orderID, recordID uint64) ([]byte, error) {
	const op = "failed to select oms submission payload"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	var payload []byte
	err := executor.QueryRowContext(ctx, "SELECT d.tx_payload FROM "+quoted(s.onchainDetailTable)+" d JOIN "+quoted(s.orderTable)+" o ON o.id=d.order_id WHERE o.account_id=? AND o.id=? AND d.execution_record_id=? AND d.exec_type='submission_accepted'", accountID, orderID, recordID).Scan(&payload)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return payload, nil
}

// ListExecutionFees lists fee components owned by one execution history record.
// Original costs and signed adjustments are returned unchanged; no currency conversion is inferred.
//
// Version:
//   - 2026-09-26: Added.
func (s *Store) ListExecutionFees(ctx context.Context, executor api.Executor, accountID, orderID, recordID uint64) ([]ExecutionFee, error) {
	const op = "failed to list oms execution fees"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	var id uint64
	err := executor.QueryRowContext(ctx, "SELECT e.id FROM "+quoted(s.executionTable)+" e JOIN "+quoted(s.orderTable)+" o ON o.id=e.order_id WHERE o.account_id=? AND o.id=? AND e.id=?", accountID, orderID, recordID).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	rows, err := executor.QueryContext(ctx, "SELECT "+executionFeeColumns+" FROM "+quoted(s.feeTable)+" WHERE order_id=? AND execution_record_id=? ORDER BY id", orderID, recordID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanExecutionFee)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}
func prefixed(alias, columns string) string {
	return alias + "." + strings.ReplaceAll(columns, ",", ","+alias+".")
}
func readRows[T any](rows *sql.Rows, scan func(scanner) (*T, error)) (values []T, err error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	for rows.Next() {
		v, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		values = append(values, *v)
	}
	return values, rows.Err()
}
func (s *Store) loadHistory(ctx context.Context, tx *sql.Tx, orderID uint64) ([]ExecutionRecord, error) {
	// Current reads are required even if the caller established an earlier REPEATABLE READ snapshot.
	rows, err := tx.QueryContext(ctx, "SELECT "+executionColumns+" FROM "+quoted(s.executionTable)+" WHERE order_id=? ORDER BY sequence FOR UPDATE", orderID)
	if err != nil {
		return nil, err
	}
	facts, err := readRows(rows, scanExecution)
	if err != nil {
		return nil, err
	}
	history := make([]ExecutionRecord, len(facts))
	positions := map[uint64]int{}
	for i, e := range facts {
		history[i].Execution = e
		positions[e.ID] = i
	}
	rows, err = tx.QueryContext(ctx, "SELECT "+onchainDetailColumns+" FROM "+quoted(s.onchainDetailTable)+" WHERE order_id=? FOR UPDATE", orderID)
	if err != nil {
		return nil, err
	}
	details, err := readRows(rows, scanOnchainDetail)
	if err != nil {
		return nil, err
	}
	for _, d := range details {
		pos, ok := positions[d.ExecutionRecordID]
		if !ok {
			return nil, ErrConflict
		}
		history[pos].Onchain = &d
	}
	rows, err = tx.QueryContext(ctx, "SELECT "+executionFeeColumns+" FROM "+quoted(s.feeTable)+" WHERE order_id=? ORDER BY id FOR UPDATE", orderID)
	if err != nil {
		return nil, err
	}
	fees, err := readRows(rows, scanExecutionFee)
	if err != nil {
		return nil, err
	}
	for _, f := range fees {
		pos, ok := positions[f.ExecutionRecordID]
		if !ok {
			return nil, ErrConflict
		}
		history[pos].Fees = append(history[pos].Fees, f)
	}
	return history, nil
}
