package oms

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	api "github.com/k4k3ru-hub/storage/go/api"
)

// InsertOnchainAMMPoolOrder inserts a validated parent and swap child in the caller's transaction.
// Roll back the transaction on any error; commit only after all related writes succeed.
// Zero ID requests storage ID generation. Inputs are not mutated.
//
// Returns:
//   - Inserted order ID, or an error (including ErrDuplicate).
//
// Version:
//   - 2026-09-20: Added.
func (s *Store) InsertOnchainAMMPoolOrder(ctx context.Context, tx *sql.Tx, order Order, swap OnchainAMMPoolSwap) (uint64, error) {
	const op = "failed to insert oms amm pool order"
	if err := s.guard(ctx, tx); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	if order.ID == 0 {
		order.ID = GenerateOrderID()
	}
	if order.Domain != DomainOnchainAMMPool {
		return 0, fmt.Errorf("%s: %w", op, invalid("domain", "invalid"))
	}
	if swap.OrderID != 0 && swap.OrderID != order.ID {
		return 0, fmt.Errorf("%s: %w", op, invalid("order_id", "invalid"))
	}
	swap.OrderID = order.ID
	if order.FilledQuantity == "" {
		order.FilledQuantity = "0"
	}
	order.CreatedAt = created(order.CreatedAt)
	order.UpdatedAt = order.CreatedAt
	order.ExpiresAt = optionalTime(order.ExpiresAt)
	order.CompletedAt = optionalTime(order.CompletedAt)
	swap.CreatedAt = order.CreatedAt
	swap.UpdatedAt = order.CreatedAt
	if err := order.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	if err := swap.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	if order.ParentOrderID != nil {
		// Parent identity is immutable; requiring an existing owned parent prevents cycles.
		if _, err := s.SelectOrderForUpdate(ctx, tx, order.AccountID, *order.ParentOrderID); err != nil {
			return 0, fmt.Errorf("%s: %w", op, err)
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+quoted(s.orderTable)+" ("+orderColumns+") VALUES ("+orderPlaceholders+")", orderArgs(order)...); err != nil {
		return 0, writeError(op, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+quoted(s.swapTable)+" ("+swapColumns+") VALUES ("+swapPlaceholders+")", swapArgs(swap)...); err != nil {
		return 0, writeError(op, err)
	}
	return order.ID, nil
}

// SelectOnchainAMMPoolSwap retrieves an owned order's AMM swap details.
//
// Version:
//   - 2026-09-20: Added.
func (s *Store) SelectOnchainAMMPoolSwap(ctx context.Context, executor api.Executor, accountID, orderID uint64) (*OnchainAMMPoolSwap, error) {
	const op = "failed to select oms amm pool swap"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	columns := "s." + strings.ReplaceAll(swapColumns, ",", ",s.")
	query := "SELECT " + columns + " FROM " + quoted(s.swapTable) + " s JOIN " + quoted(s.orderTable) + " o ON o.id=s.order_id WHERE o.account_id=? AND o.id=?"
	value, err := scanSwap(executor.QueryRowContext(ctx, query, accountID, orderID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}
