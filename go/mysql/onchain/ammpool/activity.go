package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type ActivityMinute struct {
	Pool      Identity
	Start     time.Time
	Totals    json.RawMessage
	UpdatedAt time.Time
}

func (b Batch) validateActivity() error {
	parents := make(map[Identity]bool, len(b.Snapshots))
	for _, p := range b.Snapshots {
		parents[p.Identity] = true
	}
	if len(b.ActivityMinutes) > 1441 || len(b.ResetActivity) > len(parents) {
		return fmt.Errorf("failed to validate activity batch: size=too_long")
	}
	resets := make(map[Identity]bool)
	for _, p := range b.ResetActivity {
		if !parents[p] || resets[p] {
			return fmt.Errorf("failed to validate activity batch: reset=invalid")
		}
		resets[p] = true
	}
	type key struct {
		pool   Identity
		minute int64
	}
	seen := make(map[key]bool)
	for _, m := range b.ActivityMinutes {
		if !parents[m.Pool] || !sameScope(m.Pool, b.Cursor.Source) {
			return fmt.Errorf("failed to validate activity batch: parent=invalid")
		}
		if m.Start.UnixMicro() <= 0 || !m.Start.Equal(m.Start.Truncate(time.Minute)) || m.UpdatedAt.IsZero() {
			return fmt.Errorf("failed to validate activity batch: timestamp=invalid")
		}
		if len(m.Totals) > 16384 {
			return fmt.Errorf("failed to validate activity batch: totals=too_long max_length=16384")
		}
		value := bytes.TrimSpace(m.Totals)
		if !json.Valid(value) || len(value) == 0 || value[0] != '{' && value[0] != '[' {
			return fmt.Errorf("failed to validate activity batch: totals=invalid")
		}
		k := key{m.Pool, m.Start.UnixMicro()}
		if seen[k] {
			return fmt.Errorf("failed to validate activity batch: minute=duplicate")
		}
		seen[k] = true
	}
	return nil
}

func (s *Store) commitActivity(ctx context.Context, tx *sql.Tx, b Batch) error {
	for _, p := range b.ResetActivity {
		id := p.ID()
		if _, err := tx.ExecContext(ctx, s.query("DELETE FROM onchain_amm_pool_new_pair_activity_minutes WHERE pool_id=?"), id[:]); err != nil {
			return fmt.Errorf("failed to reset activity minutes: %w", err)
		}
	}
	for _, m := range b.ActivityMinutes {
		id := m.Pool.ID()
		if _, err := tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_activity_minutes
 (pool_id,minute_started_at,totals,updated_at) VALUES (?,?,?,?)
 ON DUPLICATE KEY UPDATE totals=VALUES(totals),updated_at=VALUES(updated_at)`), id[:], m.Start.UTC(), []byte(m.Totals), m.UpdatedAt.UTC()); err != nil {
			return fmt.Errorf("failed to save activity minute: %w", err)
		}
	}
	return nil
}

// ActivityMinutes reads a pool's exact minute snapshots within [from,to).
//
// Version:
//   - 2026-09-27: Added.
func (s *Store) ActivityMinutes(ctx context.Context, pool Identity, from, to time.Time) (out []ActivityMinute, err error) {
	if err := pool.Validate(); err != nil {
		return nil, fmt.Errorf("failed to read activity minutes: %w", err)
	}
	if from.UnixMicro() <= 0 || !from.Equal(from.Truncate(time.Minute)) || !to.Equal(to.Truncate(time.Minute)) || !to.After(from) || to.Sub(from) > 24*time.Hour+time.Minute {
		return nil, fmt.Errorf("failed to read activity minutes: interval=out_of_range")
	}
	id := pool.ID()
	rows, err := s.db.QueryContext(ctx, s.query("SELECT minute_started_at,totals,updated_at FROM onchain_amm_pool_new_pair_activity_minutes WHERE pool_id=? AND minute_started_at>=? AND minute_started_at<? ORDER BY minute_started_at LIMIT 1441"), id[:], from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("failed to read activity minutes: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close activity minutes: %w", e))
		}
	}()
	for rows.Next() {
		m := ActivityMinute{Pool: pool}
		if err := rows.Scan(&m.Start, &m.Totals, &m.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to decode activity minute: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read activity minutes: %w", err)
	}
	return out, nil
}

// PruneActivityMinutes removes expired minutes in bounded batches independently of Pool retention.
//
// Version:
//   - 2026-09-27: Added.
func (s *Store) PruneActivityMinutes(ctx context.Context, before time.Time) error {
	if before.UnixMicro() <= 0 {
		return fmt.Errorf("failed to prune activity minutes: cutoff=invalid")
	}
	for i := 0; i < 100; i++ {
		r, err := s.db.ExecContext(ctx, s.query("DELETE FROM onchain_amm_pool_new_pair_activity_minutes WHERE minute_started_at<? LIMIT 1000"), before.UTC())
		if err != nil {
			return fmt.Errorf("failed to prune activity minutes: %w", err)
		}
		n, err := r.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to count pruned activity minutes: %w", err)
		}
		if n < 1000 {
			return nil
		}
	}
	return nil
}

// WalkActivityMinutes streams retained source minutes without a per-Pool query or a second full copy.
// The source key is checked against the snapshot's public source identifier.
//
// Version:
//   - 2026-09-27: Added.
func (s *Store) WalkActivityMinutes(ctx context.Context, source Source, from, to time.Time, consume func(ActivityMinute) error) (err error) {
	if err := source.Validate(); err != nil {
		return fmt.Errorf("failed to restore activity minutes: %w", err)
	}
	if consume == nil {
		return fmt.Errorf("failed to restore activity minutes: consumer=null")
	}
	if from.UnixMicro() <= 0 || !to.After(from) || to.Sub(from) > 24*time.Hour+time.Minute {
		return fmt.Errorf("failed to restore activity minutes: interval=out_of_range")
	}
	rows, err := s.db.QueryContext(ctx, s.query(`SELECT p.pool_id,m.minute_started_at,m.totals,m.updated_at
 FROM onchain_amm_pool_new_pair_activity_minutes m JOIN onchain_amm_pool_new_pair_snapshots p ON p.id=m.pool_id
 WHERE p.chain_family=? AND p.chain=? AND p.network=? AND p.venue=? AND p.is_canonical=1
 AND JSON_UNQUOTE(JSON_EXTRACT(p.state,'$.source'))=? AND m.minute_started_at>=? AND m.minute_started_at<?
 ORDER BY m.pool_id,m.minute_started_at`), source.ChainFamily, source.Chain, source.Network, source.Venue, source.Key, from.UTC(), to.UTC())
	if err != nil {
		return fmt.Errorf("failed to restore activity minutes: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close restored activity minutes: %w", e))
		}
	}()
	for rows.Next() {
		m := ActivityMinute{Pool: Identity{ChainFamily: source.ChainFamily, Chain: source.Chain, Network: source.Network, Venue: source.Venue}}
		if err := rows.Scan(&m.Pool.PoolID, &m.Start, &m.Totals, &m.UpdatedAt); err != nil {
			return fmt.Errorf("failed to decode restored activity minute: %w", err)
		}
		if err := consume(m); err != nil {
			return fmt.Errorf("failed to restore activity minute: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to restore activity minutes: %w", err)
	}
	return nil
}
