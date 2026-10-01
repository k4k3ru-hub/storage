package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type lpCheckpointReset struct {
	all       bool
	canceled  map[[32]byte]bool
	creations map[[32]byte]bool
}

func (s *Store) commitLPCheckpointResets(ctx context.Context, tx *sql.Tx, b Batch) error {
	intents := make(map[[32]byte]*lpCheckpointReset)
	intent := func(pool Identity) *lpCheckpointReset {
		id := pool.ID()
		v := intents[id]
		if v == nil {
			v = &lpCheckpointReset{canceled: map[[32]byte]bool{}, creations: map[[32]byte]bool{}}
			intents[id] = v
		}
		return v
	}
	for _, p := range b.ResetLPCheckpoints {
		intent(p).all = true
	}
	for _, p := range b.Snapshots {
		if !p.Canonical {
			intent(p.Identity).all = true
		}
	}
	for _, e := range b.Events {
		if e.Type == "created" {
			v := intent(e.Pool)
			if e.Canonical {
				v.creations[e.ID()] = true
			} else {
				v.canceled[e.ID()] = true
			}
		}
	}
	ids := make([][32]byte, 0, len(intents))
	for id := range intents {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	for start := 0; start < len(ids); start += MaxLPCheckpointBatch {
		chunk := ids[start:min(start+MaxLPCheckpointBatch, len(ids))]
		if err := s.deleteLPCheckpointChunk(ctx, tx, b.Cursor.Source.ID(), chunk, intents); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) deleteLPCheckpointChunk(ctx context.Context, tx *sql.Tx, source [32]byte, ids [][32]byte, intents map[[32]byte]*lpCheckpointReset) error {
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id[:])
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	rows, err := tx.QueryContext(ctx, s.query(`SELECT pool_id,source_id,creation_event_id
 FROM onchain_amm_pool_new_pair_lp_checkpoints WHERE pool_id IN (`+marks+`) ORDER BY pool_id FOR UPDATE`), args...)
	if err != nil {
		return fmt.Errorf("failed to lock lp checkpoint resets: %w", err)
	}
	deletes, readErr := lpCheckpointResetKeys(rows, source, intents)
	if closeErr := rows.Close(); closeErr != nil {
		readErr = errors.Join(readErr, fmt.Errorf("failed to close lp checkpoint reset rows: %w", closeErr))
	}
	if readErr != nil {
		return readErr
	}
	if len(deletes) == 0 {
		return nil
	}
	marks = strings.TrimSuffix(strings.Repeat("?,", len(deletes)), ",")
	if _, err := tx.ExecContext(ctx, s.query("DELETE FROM onchain_amm_pool_new_pair_lp_checkpoints WHERE pool_id IN ("+marks+")"), deletes...); err != nil {
		return fmt.Errorf("failed to delete lp checkpoints: %w", err)
	}
	return nil
}

func lpCheckpointResetKeys(rows *sql.Rows, source [32]byte, intents map[[32]byte]*lpCheckpointReset) ([]any, error) {
	var deletes []any
	for rows.Next() {
		var pool, savedSource, creation []byte
		if err := rows.Scan(&pool, &savedSource, &creation); err != nil {
			return nil, fmt.Errorf("failed to decode lp checkpoint reset: %w", err)
		}
		if len(pool) != 32 || len(creation) != 32 || !bytes.Equal(savedSource, source[:]) {
			return nil, fmt.Errorf("failed to reset lp checkpoint: %w: ownership=invalid", ErrLPCheckpointConflict)
		}
		id, eventID := [32]byte(pool), [32]byte(creation)
		v := intents[id]
		if v == nil {
			return nil, fmt.Errorf("failed to reset lp checkpoint: identity=invalid")
		}
		remove := v.all || v.canceled[eventID]
		for created := range v.creations {
			remove = remove || created != eventID
		}
		if remove {
			deletes = append(deletes, pool)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read lp checkpoint resets: %w", err)
	}
	return deletes, nil
}
