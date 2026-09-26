package oms

import (
	"context"
	"database/sql"
	"fmt"
)

// InsertOrder inserts an initial pending snapshot in the caller's transaction.
// ID zero requests generation. Commit it with the first accepted submission; roll back on any error.
//
// Version:
//   - 2026-09-26: Replace AMM-specific creation with generic order creation.
func (s *Store) InsertOrder(ctx context.Context, tx *sql.Tx, order Order) (uint64, error) {
	const op = "failed to insert oms order"
	if err := s.guard(ctx, tx); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	if order.ID == 0 {
		order.ID = GenerateOrderID()
	}
	if order.Status == "" {
		order.Status = OrderStatusPending
	}
	if order.FilledQuantity == "" {
		order.FilledQuantity = "0"
	}
	if order.Status != OrderStatusPending || order.FilledQuantity != "0" || order.CompletedAt != nil || order.LastExecutionSequence != 0 {
		return 0, fmt.Errorf("%s: %w", op, invalid("initial_state", "invalid"))
	}
	order.CreatedAt = created(order.CreatedAt)
	order.UpdatedAt = order.CreatedAt
	order.ExpiresAt = optionalTime(order.ExpiresAt)
	if err := order.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	if order.ParentOrderID != nil {
		if _, err := s.SelectOrderForUpdate(ctx, tx, order.AccountID, *order.ParentOrderID); err != nil {
			return 0, fmt.Errorf("%s: %w", op, err)
		}
	}
	_, err := tx.ExecContext(ctx, insertSQL(s.orderTable, orderColumns), orderArgs(order)...)
	if err != nil {
		return 0, writeError(op, err)
	}
	return order.ID, nil
}
