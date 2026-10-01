package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const lpCheckpointPayloadText = "CAST(payload AS CHAR CHARACTER SET utf8mb4)"

// SaveLPCheckpoint saves one verified projection without rewriting its parent or advancing history.
// The returned cursor and metadata are usable only after a successful commit. Callers must
// reconcile uncertain commits before continuing with their in-memory cursor.
//
// Version:
//   - 2026-10-01: Added.
//   - 2026-10-01: Avoid absent-row gap locks between independent source saves.
func (s *Store) SaveLPCheckpoint(ctx context.Context, p LPCheckpointSaveParams) (out LPCheckpointSaveResult, err error) {
	if s == nil || s.db == nil || ctx == nil {
		return out, fmt.Errorf("failed to save lp checkpoint: dependency=null")
	}
	if err := validateLPCheckpointSave(p); err != nil {
		return out, fmt.Errorf("failed to save lp checkpoint: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, fmt.Errorf("failed to begin lp checkpoint save: %w", err)
	}
	defer func() {
		closeLPCheckpointTransaction(tx, &err)
		if err != nil {
			out = LPCheckpointSaveResult{}
		}
	}()
	c, v := Cursor{Source: p.Source}, p.Checkpoint
	sid, pid := p.Source.ID(), v.Pool.ID()
	err = tx.QueryRowContext(ctx, s.query(`SELECT position,revision,updated_at
 FROM onchain_amm_pool_new_pair_sync_cursors WHERE id=? FOR UPDATE`), sid[:]).Scan(&c.Position, &c.Revision, &c.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) || err == nil && c.Revision != p.ExpectedCursorRevision {
		return out, fmt.Errorf("failed to save lp checkpoint: %w", ErrCursorConflict)
	}
	if err != nil {
		return out, fmt.Errorf("failed to lock lp checkpoint cursor: %w", err)
	}
	var identity Identity
	var parentRevision uint64
	m := LPCheckpointMetadata{}
	err = tx.QueryRowContext(ctx, s.query(`SELECT chain_family,chain,network,venue,pool_id,revision,pool_created_at,is_canonical
 FROM onchain_amm_pool_new_pair_snapshots WHERE id=? FOR UPDATE`), pid[:]).Scan(&identity.ChainFamily, &identity.Chain, &identity.Network, &identity.Venue, &identity.PoolID, &parentRevision, &m.PoolCreatedAt, &m.PoolCanonical)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (identity != v.Pool || !m.PoolCanonical || parentRevision != p.ExpectedParentRevision) {
		return out, fmt.Errorf("failed to save lp checkpoint: %w: parent=invalid", ErrLPCheckpointConflict)
	}
	if err != nil {
		return out, fmt.Errorf("failed to lock lp checkpoint parent: %w", err)
	}
	var eventPool, eventSource []byte
	var eventType string
	var canonical bool
	err = tx.QueryRowContext(ctx, s.query(`SELECT pool_id,source_id,event_type,is_canonical
 FROM onchain_amm_pool_new_pair_events WHERE id=? FOR UPDATE`), v.CreationEventID[:]).Scan(&eventPool, &eventSource, &eventType, &canonical)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (!bytes.Equal(eventPool, pid[:]) || !bytes.Equal(eventSource, sid[:]) || eventType != "created" || !canonical) {
		return out, fmt.Errorf("failed to save lp checkpoint: %w: creation_evidence=invalid", ErrLPCheckpointConflict)
	}
	if err != nil {
		return out, fmt.Errorf("failed to lock lp checkpoint creation evidence: %w", err)
	}
	// The source lock serializes owned resets and saves; the parent lock serializes
	// other-source saves and parent deletion. Keep this first consistent read after
	// those locks so repeatable-read sees their committed state. A locking read of
	// an absent checkpoint would block unrelated inserts in the same index gap.
	var savedSource, savedCreation []byte
	err = tx.QueryRowContext(ctx, s.query(`SELECT source_id,creation_event_id
 FROM onchain_amm_pool_new_pair_lp_checkpoints WHERE pool_id=?`), pid[:]).Scan(&savedSource, &savedCreation)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("failed to read lp checkpoint ownership: %w", err)
	}
	if err == nil && (!bytes.Equal(savedSource, sid[:]) || !bytes.Equal(savedCreation, v.CreationEventID[:])) {
		return out, fmt.Errorf("failed to save lp checkpoint: %w: generation=invalid", ErrLPCheckpointConflict)
	}
	// JSON normalization can expand the input. Measure the exact output expression
	// before INSERT so size violations remain inspectable without a driver dependency.
	var size uint64
	if err := tx.QueryRowContext(ctx, `SELECT OCTET_LENGTH(CAST(CAST(? AS JSON) AS CHAR CHARACTER SET utf8mb4))`, []byte(v.Payload)).Scan(&size); err != nil {
		return out, fmt.Errorf("failed to measure lp checkpoint: %w", err)
	}
	if size > MaxLPCheckpointBytes {
		return out, fmt.Errorf("failed to save lp checkpoint: %w: max_bytes=%d", ErrLPCheckpointCapacity, MaxLPCheckpointBytes)
	}
	c.Revision++
	c.UpdatedAt = dbTime(v.UpdatedAt)
	_, err = tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_lp_checkpoints
 (pool_id,source_id,creation_event_id,format_version,position_kind,position_number,position_id,event_index,payload,revision,updated_at)
 VALUES (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE
 format_version=VALUES(format_version),position_kind=VALUES(position_kind),position_number=VALUES(position_number),
 position_id=VALUES(position_id),event_index=VALUES(event_index),payload=VALUES(payload),revision=VALUES(revision),updated_at=VALUES(updated_at)`),
		pid[:], sid[:], v.CreationEventID[:], v.FormatVersion, v.Position.Kind, v.Position.Number, v.Position.ID, v.Position.Index, []byte(v.Payload), c.Revision, c.UpdatedAt)
	if err != nil {
		return out, fmt.Errorf("failed to write lp checkpoint: %w", err)
	}
	if err := s.checkLPCheckpointCapacity(ctx, tx, sid); err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, s.query(`UPDATE onchain_amm_pool_new_pair_sync_cursors SET revision=?,updated_at=? WHERE id=?`), c.Revision, c.UpdatedAt, sid[:])
	if err != nil {
		return out, fmt.Errorf("failed to update lp checkpoint cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return out, fmt.Errorf("failed to commit lp checkpoint: %w", err)
	}
	m.Ref = LPCheckpointRef{Pool: v.Pool, Revision: c.Revision}
	m.CreationEventID, m.FormatVersion, m.Position = v.CreationEventID, v.FormatVersion, v.Position
	m.PayloadBytes, m.UpdatedAt = uint32(size), c.UpdatedAt
	return LPCheckpointSaveResult{Cursor: c, Metadata: m}, nil
}

func (s *Store) checkLPCheckpointCapacity(ctx context.Context, tx *sql.Tx, source [32]byte) (err error) {
	// Current locking reads avoid a stale repeatable-read quota snapshot. All
	// checkpoint writers for this source also hold the same source cursor lock.
	rows, err := tx.QueryContext(ctx, s.query(`SELECT payload_bytes FROM onchain_amm_pool_new_pair_lp_checkpoints
 WHERE source_id=? ORDER BY pool_id LIMIT ? FOR UPDATE`), source[:], MaxLPCheckpointPools+1)
	if err != nil {
		return fmt.Errorf("failed to read lp checkpoint capacity: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close lp checkpoint capacity rows: %w", e))
		}
	}()
	var count, total uint64
	for rows.Next() {
		var size uint64
		if err := rows.Scan(&size); err != nil {
			return fmt.Errorf("failed to decode lp checkpoint capacity: %w", err)
		}
		count++
		total += size
		if count > MaxLPCheckpointPools || total > MaxLPCheckpointSourceBytes || size > MaxLPCheckpointBytes {
			return fmt.Errorf("failed to save lp checkpoint: %w: max_rows=%d max_bytes=%d", ErrLPCheckpointCapacity, MaxLPCheckpointPools, MaxLPCheckpointSourceBytes)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read lp checkpoint capacity: %w", err)
	}
	return nil
}

func closeLPCheckpointTransaction(tx *sql.Tx, result *error) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		*result = errors.Join(*result, fmt.Errorf("failed to close lp checkpoint transaction: %w", err))
	}
}
