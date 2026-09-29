package ammpool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PruneWithDeletedSnapshots prunes history and reports each committed snapshot deletion batch.
// The callback receives at most 1,000 identities, without JSON or child data. It runs after
// commit and before the next batch, including when a later batch fails. Callers must serialize
// their state updates with pruning; a returned error may leave earlier batches committed.
//
// Version:
//   - 2026-09-29: Added.
//   - 2026-09-29: Compose independently callable, bounded history and snapshot batches.
func (s *Store) PruneWithDeletedSnapshots(ctx context.Context, eventBefore, snapshotBefore time.Time, deleted func([]Identity)) error {
	if eventBefore.IsZero() || snapshotBefore.IsZero() {
		return fmt.Errorf("failed to prune amm pool history: retention=invalid")
	}
	for batch := 0; batch < 100; batch++ {
		count, err := s.PruneEventBatch(ctx, eventBefore, 1000)
		if err != nil {
			return err
		}
		if count < 1000 {
			break
		}
	}
	for batch := 0; batch < 100; batch++ {
		ids, err := s.PruneSnapshotBatch(ctx, snapshotBefore, 1000)
		if err != nil {
			return err
		}
		if len(ids) > 0 && deleted != nil {
			deleted(ids)
		}
		if len(ids) < 1000 {
			break
		}
	}
	return nil
}

// PruneEventBatch deletes at most limit expired history rows without touching sender evidence.
// Callers can run this independently of in-memory sender synchronization.
//
// Version:
//   - 2026-09-29: Added.
func (s *Store) PruneEventBatch(ctx context.Context, before time.Time, limit int) (int64, error) {
	if before.IsZero() || limit < 1 || limit > 1000 {
		return 0, fmt.Errorf("failed to prune amm pool history: bounds=out_of_range")
	}
	result, err := s.db.ExecContext(ctx, s.query("DELETE FROM onchain_amm_pool_new_pair_events WHERE observed_at<? LIMIT ?"), before.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("failed to prune amm pool history: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to read pruned row count: %w", err)
	}
	return count, nil
}

// PruneSnapshotBatch deletes at most limit expired parents and returns only committed identities.
// Callers must synchronize the deletion and adoption together with sender commits and GC.
// A commit error may leave the outcome uncertain; no identities are returned on error.
//
// Version:
//   - 2026-09-29: Added.
func (s *Store) PruneSnapshotBatch(ctx context.Context, before time.Time, limit int) (out []Identity, err error) {
	if before.IsZero() || limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("failed to prune amm pool snapshots: bounds=out_of_range")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin snapshot pruning: %w", err)
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("failed to roll back snapshot pruning: %w", e))
		}
	}()
	rows, err := tx.QueryContext(ctx, s.query(`SELECT id,chain_family,chain,network,venue,pool_id
 FROM onchain_amm_pool_new_pair_snapshots WHERE pool_created_at<? LIMIT ? FOR UPDATE`), before.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to lock expired snapshots: %w", err)
	}
	var keys []any
	readErr := func() error {
		for rows.Next() {
			var id []byte
			var p Identity
			if err := rows.Scan(&id, &p.ChainFamily, &p.Chain, &p.Network, &p.Venue, &p.PoolID); err != nil {
				return fmt.Errorf("failed to read expired snapshot: %w", err)
			}
			out = append(out, p)
			keys = append(keys, id)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to read expired snapshots: %w", err)
		}
		return nil
	}()
	if closeErr := rows.Close(); closeErr != nil {
		readErr = errors.Join(readErr, fmt.Errorf("failed to close expired snapshots: %w", closeErr))
	}
	if readErr != nil {
		return nil, readErr
	}
	if len(keys) > 0 {
		query := "DELETE FROM onchain_amm_pool_new_pair_snapshots WHERE id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",") + ")"
		result, err := tx.ExecContext(ctx, s.query(query), keys...)
		if err != nil {
			return nil, fmt.Errorf("failed to delete expired snapshots: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("failed to count deleted snapshots: %w", err)
		}
		if count != int64(len(keys)) {
			return nil, fmt.Errorf("failed to verify deleted snapshots: row_count=invalid")
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit snapshot pruning: %w", err)
	}
	return out, nil
}
