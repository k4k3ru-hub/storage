package ammpool

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type SenderPruneResult struct {
	Events       int64
	Transactions int64
}

// PruneSenders deletes expired events before unreferenced transactions, bounded per table.
// Retained references postpone transaction deletion without extending its stored expiry.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) PruneSenders(ctx context.Context, now time.Time, limit int) (out SenderPruneResult, err error) {
	if !senderTime(now) || limit < 1 || limit > 1000 {
		return out, fmt.Errorf("failed to prune sender state: bounds=out_of_range")
	}
	err = s.withSenderTransaction(ctx, func(tx *sql.Tx) error {
		queries := []struct {
			query string
			count *int64
		}{
			{`DELETE FROM onchain_amm_pool_new_pair_sender_events WHERE expires_at<=? ORDER BY expires_at,id LIMIT ?`, &out.Events},
			{`DELETE FROM onchain_amm_pool_new_pair_sender_transactions WHERE expires_at<=?
 AND NOT EXISTS (SELECT 1 FROM onchain_amm_pool_new_pair_sender_events e WHERE e.transaction_ref=onchain_amm_pool_new_pair_sender_transactions.id)
 ORDER BY expires_at,id LIMIT ?`, &out.Transactions},
		}
		for _, q := range queries {
			r, err := tx.ExecContext(ctx, s.query(q.query), dbTime(now), limit)
			if err != nil {
				return fmt.Errorf("failed to prune sender state: %w", err)
			}
			n, err := r.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to count pruned sender rows: %w", err)
			}
			*q.count = n
		}
		return nil
	})
	if err != nil {
		return SenderPruneResult{}, err
	}
	return out, nil
}

// ReleaseSenderSnapshot deletes unchanged, empty collection metadata after invalid windows expire.
// The application must first stop collection for the pool and discard its pending work.
// False means the state changed, still has children, or remains invalid in a current window.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) ReleaseSenderSnapshot(ctx context.Context, p SenderSnapshot, now time.Time) (bool, error) {
	if err := p.Validate(); err != nil {
		return false, fmt.Errorf("failed to release sender snapshot: %w", err)
	}
	if !senderTime(now) {
		return false, fmt.Errorf("failed to release sender snapshot: now=invalid")
	}
	id := p.Pool.ID()
	r, err := s.db.ExecContext(ctx, s.query(`DELETE FROM onchain_amm_pool_new_pair_sender_snapshots
 WHERE pool_id=? AND creation_event_id=? AND generation=? AND initialized_at=? AND updated_at=? AND updated_at<=?
 AND (invalid_to IS NULL OR invalid_to<=?)
 AND NOT EXISTS (SELECT 1 FROM onchain_amm_pool_new_pair_sender_events e WHERE e.pool_id=onchain_amm_pool_new_pair_sender_snapshots.pool_id)`), id[:], p.CreationEventID[:], p.Generation, dbTime(p.InitializedAt), dbTime(p.UpdatedAt), dbTime(now), dbTime(now.Truncate(time.Minute).Add(-15*time.Minute)))
	if err != nil {
		return false, fmt.Errorf("failed to release sender snapshot: %w", err)
	}
	n, err := r.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to count released sender snapshots: %w", err)
	}
	return n == 1, nil
}
