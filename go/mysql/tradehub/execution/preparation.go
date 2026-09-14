package execution

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ReservePreparation inserts a reservation or exclusively locks the existing key.
// The no-op duplicate update avoids shared-lock upgrades between concurrent retries.
//
// Version:
//   - 2026-09-14: Added.
func (s *Store) ReservePreparation(ctx context.Context, tx *sql.Tx, params ExecutionInsertParams) error {
	const operation = "failed to reserve execution preparation"
	if s == nil || ctx == nil || tx == nil {
		return fmt.Errorf("%s: dependency=null", operation)
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if params.AccountID == nil || len(params.IdempotencyKey) == 0 || params.Status != StatusPreparing {
		return fmt.Errorf("%s: reservation=invalid", operation)
	}
	query := fmt.Sprintf("INSERT INTO %s (id, account_id, idempotency_key, status, kind, request_snapshot, conditions_snapshot) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE id=id", s.executionTable)
	_, err := tx.ExecContext(ctx, query, params.ID, params.AccountID, params.IdempotencyKey, params.Status, params.Kind, []byte(params.RequestSnapshot), nullableJSON(params.ConditionsSnapshot))
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

// SelectPreparationForUpdate locks an execution identified by its owner and key.
//
// Version:
//   - 2026-09-14: Added.
func (s *Store) SelectPreparationForUpdate(ctx context.Context, tx *sql.Tx, accountID uint64, kind string, key []byte) (*Execution, error) {
	const operation = "failed to select execution preparation"
	if s == nil || ctx == nil || tx == nil {
		return nil, fmt.Errorf("%s: dependency=null", operation)
	}
	if accountID == 0 || kind == "" || len(key) == 0 || len(key) > 128 {
		return nil, fmt.Errorf("%s: parameter=invalid", operation)
	}
	var value Execution
	var result []byte
	var preparedAt, expiresAt sql.NullTime
	query := fmt.Sprintf("SELECT id, status, request_snapshot, result_snapshot, prepared_at, expires_at FROM %s WHERE account_id=? AND kind=? AND idempotency_key=? FOR UPDATE", s.executionTable)
	if err := tx.QueryRowContext(ctx, query, accountID, kind, key).Scan(&value.ID, &value.Status, &value.RequestSnapshot, &result, &preparedAt, &expiresAt); err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	value.AccountID, value.Kind, value.IdempotencyKey = &accountID, kind, append([]byte(nil), key...)
	value.ResultSnapshot, value.PreparedAt, value.ExpiresAt = result, preparedAt.Time, expiresAt.Time
	return &value, nil
}

// CompletePreparation saves the immutable preparation result in the caller's transaction.
//
// Version:
//   - 2026-09-14: Added.
func (s *Store) CompletePreparation(ctx context.Context, tx *sql.Tx, id string, result []byte, preparedAt, expiresAt time.Time) error {
	const operation = "failed to complete execution preparation"
	if s == nil || ctx == nil || tx == nil {
		return fmt.Errorf("%s: dependency=null", operation)
	}
	if id == "" || !validJSONObject(result) || preparedAt.IsZero() || !expiresAt.After(preparedAt) {
		return fmt.Errorf("%s: parameter=invalid", operation)
	}
	query := fmt.Sprintf("UPDATE %s SET status=?, result_snapshot=?, prepared_at=?, expires_at=? WHERE id=? AND status=?", s.executionTable)
	updated, err := tx.ExecContext(ctx, query, StatusPrepared, result, preparedAt.UTC(), expiresAt.UTC(), id, StatusPreparing)
	return requireOneRow(updated, err, operation)
}
