package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const lpCheckpointMetadataColumns = `p.id,p.chain_family,p.chain,p.network,p.venue,p.pool_id,
 c.creation_event_id,c.format_version,c.position_kind,c.position_number,c.position_id,c.event_index,
 c.payload_bytes,c.revision,c.updated_at,p.pool_created_at,p.is_canonical`

const lpCheckpointParentJoin = ` FROM onchain_amm_pool_new_pair_lp_checkpoints c
 JOIN onchain_amm_pool_new_pair_snapshots p ON p.id=c.pool_id`

func scanLPCheckpointMetadata(row scanner) (LPCheckpointMetadata, error) {
	var m LPCheckpointMetadata
	var id, creation []byte
	p := &m.Ref.Pool
	if err := row.Scan(&id, &p.ChainFamily, &p.Chain, &p.Network, &p.Venue, &p.PoolID,
		&creation, &m.FormatVersion, &m.Position.Kind, &m.Position.Number, &m.Position.ID, &m.Position.Index,
		&m.PayloadBytes, &m.Ref.Revision, &m.UpdatedAt, &m.PoolCreatedAt, &m.PoolCanonical); err != nil {
		return m, fmt.Errorf("failed to decode lp checkpoint metadata: %w", err)
	}
	want := p.ID()
	if !bytes.Equal(id, want[:]) || len(creation) != 32 || m.Ref.Revision == 0 || m.FormatVersion == 0 || !senderTime(m.UpdatedAt) || !senderTime(m.PoolCreatedAt) {
		return m, fmt.Errorf("failed to decode lp checkpoint metadata: identity_or_revision=invalid")
	}
	copy(m.CreationEventID[:], creation)
	if m.CreationEventID == ([32]byte{}) || !validText(m.Position.Kind, 16) || !validText(m.Position.ID, 128) || m.Position.Index != nil && !validText(*m.Position.Index, 128) {
		return m, fmt.Errorf("failed to decode lp checkpoint metadata: position_or_creation=invalid")
	}
	if err := p.Validate(); err != nil {
		return m, fmt.Errorf("failed to decode lp checkpoint metadata: %w", err)
	}
	if m.PayloadBytes < 2 || m.PayloadBytes > MaxLPCheckpointBytes {
		return m, fmt.Errorf("failed to decode lp checkpoint metadata: %w", ErrLPCheckpointCapacity)
	}
	return m, nil
}

// ListLPCheckpointMetadata reads source-scoped metadata without loading parent or LP JSON.
// Noncanonical and expired parents remain visible for bounded cleanup by the caller.
//
// Version:
//   - 2026-10-01: Added.
func (s *Store) ListLPCheckpointMetadata(ctx context.Context, p LPCheckpointListParams) (out LPCheckpointPage, err error) {
	if s == nil || s.db == nil || ctx == nil {
		return out, fmt.Errorf("failed to list lp checkpoints: dependency=null")
	}
	if err := p.Source.Validate(); err != nil {
		return out, fmt.Errorf("failed to list lp checkpoints: %w", err)
	}
	if p.Limit < 1 || p.Limit > MaxLPCheckpointBatch {
		return out, fmt.Errorf("failed to list lp checkpoints: limit=out_of_range max_rows=%d", MaxLPCheckpointBatch)
	}
	sid := p.Source.ID()
	query := "SELECT " + lpCheckpointMetadataColumns + lpCheckpointParentJoin + " WHERE c.source_id=?"
	args := []any{sid[:]}
	if p.AfterPoolID != nil {
		query += " AND c.pool_id>?"
		args = append(args, p.AfterPoolID[:])
	}
	query += " ORDER BY c.pool_id LIMIT ?"
	args = append(args, p.Limit+1)
	rows, err := s.db.QueryContext(ctx, s.query(query), args...)
	if err != nil {
		return out, fmt.Errorf("failed to list lp checkpoints: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close lp checkpoint metadata rows: %w", e))
		}
		if err != nil {
			out = LPCheckpointPage{}
		}
	}()
	for rows.Next() {
		m, err := scanLPCheckpointMetadata(rows)
		if err != nil {
			return out, err
		}
		if !sameScope(m.Ref.Pool, p.Source) {
			return out, fmt.Errorf("failed to list lp checkpoints: scope=invalid")
		}
		if len(out.Items) == p.Limit {
			id := out.Items[len(out.Items)-1].Ref.Pool.ID()
			out.NextAfterID = &id
			break
		}
		out.Items = append(out.Items, m)
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("failed to list lp checkpoints: %w", err)
	}
	return out, nil
}

// LoadLPCheckpoints reads exact revisions from one repeatable-read view within a 4 MiB budget.
// Missing, superseded and noncanonical references are omitted. Any error discards the
// entire result of this call. A successful read still requires chain validation by the caller.
//
// Version:
//   - 2026-10-01: Added.
func (s *Store) LoadLPCheckpoints(ctx context.Context, source Source, refs []LPCheckpointRef) (out []LPCheckpointEntry, err error) {
	if s == nil || s.db == nil || ctx == nil {
		return nil, fmt.Errorf("failed to load lp checkpoints: dependency=null")
	}
	if err := validateLPCheckpointRefs(source, refs); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to begin lp checkpoint read: %w", err)
	}
	defer func() {
		closeLPCheckpointTransaction(tx, &err)
		if err != nil {
			out = nil
		}
	}()
	metadata, err := s.readLPCheckpointSelection(ctx, tx, source, refs)
	if err != nil {
		return nil, err
	}
	if len(metadata) > 0 {
		selected := make([]LPCheckpointRef, 0, len(metadata))
		for _, m := range metadata {
			selected = append(selected, m.Ref)
		}
		where, args := lpCheckpointSelection(source, selected)
		rows, err := tx.QueryContext(ctx, s.query("SELECT c.pool_id,c.revision,"+lpCheckpointPayloadText+" FROM onchain_amm_pool_new_pair_lp_checkpoints c WHERE "+where+" ORDER BY c.pool_id"), args...)
		if err != nil {
			return nil, fmt.Errorf("failed to read lp checkpoint payloads: %w", err)
		}
		out, err = readLPCheckpointPayloads(rows, metadata)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to finish lp checkpoint read: %w", err)
	}
	return out, nil
}

func validateLPCheckpointRefs(source Source, refs []LPCheckpointRef) error {
	if err := source.Validate(); err != nil {
		return fmt.Errorf("failed to load lp checkpoints: %w", err)
	}
	if len(refs) < 1 || len(refs) > MaxLPCheckpointBatch {
		return fmt.Errorf("failed to load lp checkpoints: size=out_of_range max_rows=%d", MaxLPCheckpointBatch)
	}
	seen := make(map[Identity]bool, len(refs))
	for _, ref := range refs {
		if err := ref.Pool.Validate(); err != nil {
			return fmt.Errorf("failed to load lp checkpoints: %w", err)
		}
		if ref.Revision == 0 || seen[ref.Pool] || !sameScope(ref.Pool, source) {
			return fmt.Errorf("failed to load lp checkpoints: identity_or_revision=invalid")
		}
		seen[ref.Pool] = true
	}
	return nil
}

func lpCheckpointSelection(source Source, refs []LPCheckpointRef) (string, []any) {
	sid := source.ID()
	args := []any{sid[:]}
	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		id := ref.Pool.ID()
		parts = append(parts, "(c.pool_id=? AND c.revision=?)")
		args = append(args, id[:], ref.Revision)
	}
	return "c.source_id=? AND (" + strings.Join(parts, " OR ") + ")", args
}

func (s *Store) readLPCheckpointSelection(ctx context.Context, tx *sql.Tx, source Source, refs []LPCheckpointRef) (out []LPCheckpointMetadata, err error) {
	where, args := lpCheckpointSelection(source, refs)
	query := "SELECT " + lpCheckpointMetadataColumns + lpCheckpointParentJoin + `
 JOIN onchain_amm_pool_new_pair_events e ON e.id=c.creation_event_id AND e.pool_id=c.pool_id AND e.source_id=c.source_id
 WHERE ` + where + " AND p.is_canonical=1 AND e.is_canonical=1 AND e.event_type='created' ORDER BY c.pool_id"
	rows, err := tx.QueryContext(ctx, s.query(query), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to select lp checkpoint metadata: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close selected lp checkpoint metadata: %w", e))
		}
	}()
	var total uint64
	for rows.Next() {
		m, err := scanLPCheckpointMetadata(rows)
		if err != nil {
			return nil, err
		}
		if !sameScope(m.Ref.Pool, source) {
			return nil, fmt.Errorf("failed to select lp checkpoint metadata: scope=invalid")
		}
		total += uint64(m.PayloadBytes)
		if total > MaxLPCheckpointReadBytes {
			return nil, fmt.Errorf("failed to load lp checkpoints: %w: max_bytes=%d", ErrLPCheckpointCapacity, MaxLPCheckpointReadBytes)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to select lp checkpoint metadata: %w", err)
	}
	return out, nil
}

func readLPCheckpointPayloads(rows *sql.Rows, metadata []LPCheckpointMetadata) (out []LPCheckpointEntry, err error) {
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close lp checkpoint payload rows: %w", e))
		}
	}()
	for rows.Next() {
		var id []byte
		var revision uint64
		var payload []byte
		if err := rows.Scan(&id, &revision, &payload); err != nil {
			return nil, fmt.Errorf("failed to decode lp checkpoint payload: %w", err)
		}
		if len(out) >= len(metadata) {
			return nil, fmt.Errorf("failed to load lp checkpoints: row_count=invalid")
		}
		m := metadata[len(out)]
		want := m.Ref.Pool.ID()
		if !bytes.Equal(id, want[:]) || revision != m.Ref.Revision || len(payload) != int(m.PayloadBytes) {
			return nil, fmt.Errorf("failed to load lp checkpoints: projection=invalid")
		}
		v := LPCheckpoint{Pool: m.Ref.Pool, CreationEventID: m.CreationEventID, FormatVersion: m.FormatVersion, Position: m.Position, Payload: payload, UpdatedAt: m.UpdatedAt}
		if err := validateLPCheckpoint(v); err != nil {
			return nil, err
		}
		out = append(out, LPCheckpointEntry{Metadata: m, Checkpoint: v})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read lp checkpoint payloads: %w", err)
	}
	if len(out) != len(metadata) {
		return nil, fmt.Errorf("failed to load lp checkpoints: row_count=invalid")
	}
	return out, nil
}
