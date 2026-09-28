package oms

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	api "github.com/k4k3ru-hub/storage/go/api"
)

const pnlColumns = "id,account_id,account_ref,subject_type,subject_key,position_order_id,definition_version,definition,last_order_id,remaining_quantity,remaining_cost,average_entry_price,realized_pnl,calculation_method,calculation_version,calculation_state,needs_rebuild,version,calculated_at,created_at,updated_at"

func scanPnL(row scanner) (*PnL, error) {
	var v PnL
	var key, definition []byte
	var definitionVersion uint16
	var method sql.NullString
	var version sql.NullInt64
	var calculatedAt sql.NullTime
	if err := row.Scan(&v.ID, &v.AccountID, &v.AccountRef, &v.SubjectType, &key, &v.PositionOrderID,
		&definitionVersion, &definition, &v.LastOrderID, &v.RemainingQuantity, &v.RemainingCost, &v.AverageEntryPrice,
		&v.RealizedPnL, &method, &version, (*[]byte)(&v.CalculationState), &v.NeedsRebuild, &v.Version,
		&calculatedAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	if definitionVersion != 1 {
		return nil, fmt.Errorf("failed to decode pnl scope: %w: definition_version=invalid", ErrConflict)
	}
	var assets pnlDefinition
	if err := json.Unmarshal(definition, &assets); err != nil {
		return nil, fmt.Errorf("failed to decode pnl scope: %w", err)
	}
	v.InventoryAsset, v.AccountingAsset = assets.InventoryAsset, assets.AccountingAsset
	if err := v.PnLScope.validate(); err != nil {
		return nil, fmt.Errorf("failed to decode pnl scope: %w", err)
	}
	expected, err := v.PnLScope.key()
	if err != nil {
		return nil, err
	}
	if len(key) != len(expected) || string(key) != string(expected[:]) || v.Version == 0 {
		return nil, fmt.Errorf("failed to decode pnl scope: %w: identity=invalid", ErrConflict)
	}
	if calculatedAt.Valid {
		if !method.Valid || !version.Valid || version.Int64 < 1 || version.Int64 > math.MaxUint16 {
			return nil, fmt.Errorf("failed to decode pnl checkpoint: %w: calculation=invalid", ErrConflict)
		}
		v.CalculatedAt, v.CalculationMethod, v.CalculationVersion = calculatedAt.Time, method.String, uint16(version.Int64)
		// MySQL renders JSON with whitespace; enforce the same compact size on write and read.
		v.CalculationState, err = canonicalJSON(v.CalculationState)
		if err != nil {
			return nil, fmt.Errorf("failed to decode pnl checkpoint: %w", err)
		}
		if err := v.PnLCheckpoint.validate(v.SubjectType); err != nil {
			return nil, fmt.Errorf("failed to decode pnl checkpoint: %w", err)
		}
	}
	return &v, nil
}

func (s *Store) guardPnL(ctx context.Context, q api.Executor) error {
	if err := s.guard(ctx, q); err != nil {
		return err
	}
	if s.pnlTable == "" {
		return invalid("pnl_table", "empty")
	}
	return nil
}

// EnsurePnL creates an uncalculated subject or retrieves the existing immutable scope.
// Commit this registration before opening the consistent read used for calculation.
// Symbol changes do not create a second cost basis; asset or accounting-unit conflicts fail.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) EnsurePnL(ctx context.Context, tx *sql.Tx, accountID uint64, scope PnLScope) (*PnL, error) {
	const op = "failed to ensure oms pnl"
	if err := s.guardPnL(ctx, tx); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if accountID == 0 {
		return nil, fmt.Errorf("%s: %w", op, invalid("account_id", "empty"))
	}
	if err := scope.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if scope.PositionOrderID != nil {
		root, err := s.SelectOrder(ctx, tx, accountID, *scope.PositionOrderID)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		if !sameUint(root.PositionOrderID, &root.ID) || root.AccountRef != scope.AccountRef {
			return nil, fmt.Errorf("%s: %w: position_scope=invalid", op, ErrConflict)
		}
	}
	key, err := scope.key()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	definition, err := json.Marshal(pnlDefinition{scope.InventoryAsset, scope.AccountingAsset})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	query := "INSERT INTO " + quoted(s.pnlTable) +
		" (id,account_id,account_ref,subject_type,subject_key,position_order_id,definition_version,definition) VALUES (?,?,?,?,?,?,1,?) ON DUPLICATE KEY UPDATE id=id"
	if _, err := tx.ExecContext(ctx, query, pnlIDGenerator.Generate(), accountID, scope.AccountRef, scope.SubjectType, key[:], scope.PositionOrderID, definition); err != nil {
		return nil, writeError(op, err)
	}
	value, err := scanPnL(tx.QueryRowContext(ctx, "SELECT "+pnlColumns+" FROM "+quoted(s.pnlTable)+" WHERE account_id=? AND subject_type=? AND subject_key=? FOR UPDATE", accountID, scope.SubjectType, key[:]))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if !samePnLScope(value.PnLScope, scope) {
		return nil, fmt.Errorf("%s: %w: scope=mismatch", op, ErrConflict)
	}
	return value, nil
}

// SelectPnL retrieves owned durable checkpoint state without reading market prices.
// Read this and its OMS inputs in one repeatable-read view after EnsurePnL was committed.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) SelectPnL(ctx context.Context, q api.Executor, accountID, pnlID uint64) (*PnL, error) {
	const op = "failed to select oms pnl"
	if err := s.guardPnL(ctx, q); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, pnlID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	v, err := scanPnL(q.QueryRowContext(ctx, "SELECT "+pnlColumns+" FROM "+quoted(s.pnlTable)+" WHERE account_id=? AND id=?", accountID, pnlID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return v, nil
}

// ListPnL lists a wallet's subjects by ascending ID after an exclusive cursor.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) ListPnL(ctx context.Context, q api.Executor, accountID uint64, accountRef string, afterID uint64, limit int) ([]PnL, error) {
	const op = "failed to list oms pnl"
	if err := s.guardPnL(ctx, q); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if accountID == 0 {
		return nil, fmt.Errorf("%s: %w", op, invalid("account_id", "empty"))
	}
	if err := textValue("account_ref", accountRef, 128, true); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	rows, err := q.QueryContext(ctx, "SELECT "+pnlColumns+" FROM "+quoted(s.pnlTable)+" WHERE account_id=? AND account_ref=? AND id>? ORDER BY id LIMIT ?", accountID, accountRef, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	v, err := readRows(rows, scanPnL)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return v, nil
}

func pnlOrderScope(v *PnL) (string, []any) {
	where, args := "o.account_id=? AND o.account_ref=?", []any{v.AccountID, v.AccountRef}
	if v.PositionOrderID != nil {
		where += " AND o.position_order_id=?"
		args = append(args, *v.PositionOrderID)
	}
	return where, args
}

// ListPnLOrders reads full order records after an exclusive ID for checkpoint or tail calculation.
// It includes unfinished orders; callers must not skip them when advancing the checkpoint.
// Spot scans the whole wallet conservatively; the calculator resolves exact asset contributions.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) ListPnLOrders(ctx context.Context, tx *sql.Tx, accountID, pnlID, afterID uint64, limit int) ([]Order, error) {
	const op = "failed to list oms pnl orders"
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	v, err := s.SelectPnL(ctx, tx, accountID, pnlID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	where, args := pnlOrderScope(v)
	// Only orders participate in this SELECT, so unqualified projection names are unambiguous.
	rows, err := tx.QueryContext(ctx, "SELECT "+orderColumns+" FROM "+quoted(s.orderTable)+" o WHERE "+where+" AND o.id>? ORDER BY o.id LIMIT ?", append(args, afterID, limit)...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanOrder)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

// SavePnL publishes a calculated complete prefix using optimistic version comparison.
// The expected version and source histories must come from the same consistent read.
// Rebuild must be true after invalidation, cursor rewind or calculation-contract changes.
// Callers own economic-order validation, exact-state semantics and multi-subject atomicity.
// ErrConflict requires a fresh read and recalculation, never a blind version-only retry.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) SavePnL(ctx context.Context, tx *sql.Tx, accountID, pnlID, expectedVersion uint64, checkpoint PnLCheckpoint, rebuild bool) error {
	const op = "failed to save oms pnl"
	if expectedVersion == 0 || expectedVersion == math.MaxUint64 {
		return fmt.Errorf("%s: %w", op, invalid("version", "out_of_range"))
	}
	previous, err := s.SelectPnL(ctx, tx, accountID, pnlID)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if previous.Version != expectedVersion {
		return fmt.Errorf("%s: %w: version=mismatch", op, ErrConflict)
	}
	if err := checkpoint.validate(previous.SubjectType); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	checkpoint.CalculationState, err = canonicalJSON(checkpoint.CalculationState)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if len(checkpoint.CalculationState) > maxPnLStateBytes {
		return fmt.Errorf("%s: %w", op, invalid("calculation_state", "too_long"))
	}
	if !rebuild && (previous.NeedsRebuild ||
		previous.LastOrderID != nil && (checkpoint.LastOrderID == nil || *checkpoint.LastOrderID < *previous.LastOrderID) ||
		previous.CalculationMethod != checkpoint.CalculationMethod || previous.CalculationVersion != checkpoint.CalculationVersion) {
		return fmt.Errorf("%s: %w: rebuild=required", op, ErrConflict)
	}
	if err := s.validatePnLPrefix(ctx, tx, previous, checkpoint.LastOrderID); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+quoted(s.pnlTable)+
		" SET last_order_id=?,remaining_quantity=?,remaining_cost=?,average_entry_price=?,realized_pnl=?,calculation_method=?,calculation_version=?,calculation_state=?,calculated_at=?,needs_rebuild=FALSE,version=version+1 WHERE account_id=? AND id=? AND version=?",
		checkpoint.LastOrderID, checkpoint.RemainingQuantity, checkpoint.RemainingCost, checkpoint.AverageEntryPrice, checkpoint.RealizedPnL,
		checkpoint.CalculationMethod, checkpoint.CalculationVersion, []byte(checkpoint.CalculationState), utc(checkpoint.CalculatedAt), accountID, pnlID, expectedVersion)
	if err != nil {
		return writeError(op, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if n != 1 {
		return fmt.Errorf("%s: %w: version=mismatch", op, ErrConflict)
	}
	return nil
}

func (s *Store) validatePnLPrefix(ctx context.Context, tx *sql.Tx, v *PnL, lastID *uint64) error {
	if lastID == nil {
		return nil
	}
	last, err := s.SelectOrder(ctx, tx, v.AccountID, *lastID)
	if err != nil {
		return err
	}
	if last.AccountRef != v.AccountRef || v.PositionOrderID != nil && !sameUint(last.PositionOrderID, v.PositionOrderID) {
		return fmt.Errorf("failed to validate pnl prefix: %w: order_scope=mismatch", ErrConflict)
	}
	where, args := pnlOrderScope(v)
	query := "SELECT EXISTS(SELECT 1 FROM " + quoted(s.orderTable) + " o WHERE " + where +
		" AND o.id<=? AND (o.status IN ('pending','partially_filled') OR o.completed_at IS NULL OR EXISTS(SELECT 1 FROM " + quoted(s.executionTable) +
		" e WHERE e.order_id=o.id AND (e.status IN ('pending','partially_filled') OR e.completed_at IS NULL OR e.fees_complete=FALSE))))"
	var incomplete bool
	if err := tx.QueryRowContext(ctx, query, append(args, *lastID)...).Scan(&incomplete); err != nil {
		return err
	}
	if incomplete {
		return fmt.Errorf("failed to validate pnl prefix: %w: order_prefix=incomplete", ErrConflict)
	}
	return nil
}

// InvalidatePnL marks an owned checkpoint for full rebuild and rejects in-flight publication.
// Existing values remain available as explicitly stale historical state until rebuilt.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) InvalidatePnL(ctx context.Context, tx *sql.Tx, accountID, pnlID uint64) error {
	const op = "failed to invalidate oms pnl"
	if err := s.guardPnL(ctx, tx); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, pnlID); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+quoted(s.pnlTable)+" SET needs_rebuild=TRUE,version=version+1 WHERE account_id=? AND id=?", accountID, pnlID)
	if err != nil {
		return writeError(op, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if n != 1 {
		return fmt.Errorf("%s: %w", op, sql.ErrNoRows)
	}
	return nil
}

func (s *Store) touchPnL(ctx context.Context, tx *sql.Tx, order Order) error {
	if s.pnlTable == "" {
		return nil
	}
	// Invalidation deliberately covers the wallet, including cross-asset cost dependencies.
	// Even writes beyond the prefix change version, fencing calculations with older inputs.
	_, err := tx.ExecContext(ctx, "UPDATE "+quoted(s.pnlTable)+
		" SET needs_rebuild=(needs_rebuild OR (last_order_id IS NOT NULL AND last_order_id>=?)),version=version+1 WHERE account_id=? AND account_ref=?", order.ID, order.AccountID, order.AccountRef)
	if err != nil {
		return fmt.Errorf("failed to invalidate oms pnl source: %w", err)
	}
	return nil
}
