package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const pruneSnapshotCandidatesSQL = `SELECT id,chain_family,chain,network,venue,pool_id,pool_created_at
 FROM onchain_amm_pool_new_pair_snapshots WHERE is_canonical=? AND pool_created_at<?
 ORDER BY pool_created_at,id LIMIT ? FOR UPDATE`

type pruneSnapshotCandidate struct {
	id        []byte
	identity  Identity
	createdAt time.Time
}

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
// It locks up to limit candidates in each canonical range and deletes the oldest combined limit.
// Callers must synchronize the deletion and adoption together with sender commits and GC.
// A commit error may leave the outcome uncertain; no identities are returned on error.
//
// Version:
//   - 2026-09-29: Added.
//   - 2026-10-03: Use indexed canonical ranges and order the combined candidates by age and ID.
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
	var candidates []pruneSnapshotCandidate
	for _, canonical := range []bool{false, true} {
		group, err := s.lockPruneSnapshotCandidates(ctx, tx, canonical, before, limit)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, group...)
	}
	slices.SortFunc(candidates, func(a, b pruneSnapshotCandidate) int {
		if order := a.createdAt.Compare(b.createdAt); order != 0 {
			return order
		}
		return bytes.Compare(a.id, b.id)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	var keys []any
	for _, candidate := range candidates {
		out = append(out, candidate.identity)
		keys = append(keys, candidate.id)
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

func (s *Store) lockPruneSnapshotCandidates(ctx context.Context, tx *sql.Tx, canonical bool, before time.Time, limit int) (out []pruneSnapshotCandidate, err error) {
	rows, err := tx.QueryContext(ctx, s.query(pruneSnapshotCandidatesSQL), canonical, before.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to lock expired snapshots: %w: is_canonical=%t", err, canonical)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close expired snapshots: %w", closeErr))
		}
	}()
	for rows.Next() {
		var candidate pruneSnapshotCandidate
		p := &candidate.identity
		if err := rows.Scan(&candidate.id, &p.ChainFamily, &p.Chain, &p.Network, &p.Venue, &p.PoolID, &candidate.createdAt); err != nil {
			return nil, fmt.Errorf("failed to read expired snapshot: %w", err)
		}
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read expired snapshots: %w", err)
	}
	return out, nil
}
