package oms

import (
	"context"
	"fmt"
	"strings"

	api "github.com/k4k3ru-hub/storage/go/api"
)

// ListOrders lists an account's snapshots by descending ID before an exclusive cursor.
// A zero cursor starts with the newest ID; limits range from 1 through 200.
//
// Version:
//   - 2026-09-27: Added.
func (s *Store) ListOrders(ctx context.Context, q api.Executor, accountID, beforeID uint64, limit int) ([]Order, error) {
	const op = "failed to list oms orders"
	if err := s.guard(ctx, q); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if accountID == 0 {
		return nil, fmt.Errorf("%s: %w", op, invalid("account_id", "empty"))
	}
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	query := "SELECT " + orderColumns + " FROM " + quoted(s.orderTable) + " WHERE account_id=?"
	args := []any{accountID}
	if beforeID != 0 {
		query += " AND id<?"
		args = append(args, beforeID)
	}
	rows, err := q.QueryContext(ctx, query+" ORDER BY id DESC LIMIT ?", append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanOrder)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

// ListPositionOrders lists an owned representative order and its related orders by descending ID.
// A zero cursor starts with the newest ID. Membership does not imply a remaining position.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) ListPositionOrders(ctx context.Context, q api.Executor, accountID, positionOrderID, beforeID uint64, limit int) ([]Order, error) {
	const op = "failed to list oms position orders"
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	root, err := s.SelectOrder(ctx, q, accountID, positionOrderID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if root.PositionOrderID == nil || *root.PositionOrderID != root.ID {
		return nil, fmt.Errorf("%s: %w: position_order_id=invalid", op, ErrConflict)
	}
	query := "SELECT " + orderColumns + " FROM " + quoted(s.orderTable) + " WHERE account_id=? AND position_order_id=?"
	args := []any{accountID, positionOrderID}
	if beforeID != 0 {
		query += " AND id<?"
		args = append(args, beforeID)
	}
	rows, err := q.QueryContext(ctx, query+" ORDER BY id DESC LIMIT ?", append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanOrder)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

// ListOnchainEvents lists owned events after an exclusive order-local sequence.
// Recovery payloads and opaque protocol data are omitted from this read model.
// Use a transaction for consistency with order and execution snapshots.
//
// Version:
//   - 2026-09-27: Added.
func (s *Store) ListOnchainEvents(ctx context.Context, q api.Executor, accountID, orderID, afterSequence uint64, limit int) ([]OnchainEvent, error) {
	const op = "failed to list oms onchain events"
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	if _, err := s.SelectOrder(ctx, q, accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	columns := strings.Replace(onchainEventColumns, "tx_payload", "NULL", 1)
	columns = strings.Replace(columns, "protocol_data", "NULL", 1)
	rows, err := q.QueryContext(ctx, "SELECT "+columns+" FROM "+quoted(s.onchainEventTable)+" WHERE order_id=? AND sequence>? ORDER BY sequence LIMIT ?", orderID, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanOnchainEvent)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

// ListOrderFees lists owned fee components and adjustments after an exclusive ID.
// Components retain execution and event references; no aggregation is performed.
//
// Version:
//   - 2026-09-27: Added.
func (s *Store) ListOrderFees(ctx context.Context, q api.Executor, accountID, orderID, afterID uint64, limit int) ([]ExecutionFee, error) {
	const op = "failed to list oms order fees"
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	if _, err := s.SelectOrder(ctx, q, accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	rows, err := q.QueryContext(ctx, "SELECT "+executionFeeColumns+" FROM "+quoted(s.feeTable)+" WHERE order_id=? AND id>? ORDER BY id LIMIT ?", orderID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanExecutionFee)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}
