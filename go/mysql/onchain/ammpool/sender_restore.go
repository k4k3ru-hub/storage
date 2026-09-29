package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// WalkSenderState streams parents and all three sender tables from one repeatable-read view.
// Limits include expired, abandoned and noncanonical rows awaiting cleanup. Consumers must
// stage results and discard them on any error; no partial restore is a valid empty state.
// Callbacks must not write through this transaction and must account for retained memory.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) WalkSenderState(ctx context.Context, limits SenderRestoreLimits, consume SenderStateConsumer) (err error) {
	if limits.Pools < 1 || limits.Pools > MaxSenderPools || limits.Transactions < 1 || limits.Transactions > MaxSenderTransactions || limits.Events < 1 || limits.Events > MaxSenderEvents || limits.EventsPerPool < 1 || limits.EventsPerPool > MaxSenderPoolEvents {
		return fmt.Errorf("failed to restore sender state: limits=out_of_range")
	}
	if consume.Pool == nil || consume.Transaction == nil || consume.Event == nil {
		return fmt.Errorf("failed to restore sender state: consumer=null")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("failed to restore sender state: %w", err)
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("failed to close sender restore transaction: %w", e))
		}
	}()
	states := make(map[Identity]uint64)
	parentColumns := "p." + strings.ReplaceAll(snapshotColumns, ",", ",p.")
	err = s.walkSenderRows(ctx, tx, `SELECT p.id,`+parentColumns+`,c.creation_event_id,c.generation,c.initialized_at,c.invalid_from,c.invalid_to,c.updated_at
 FROM onchain_amm_pool_new_pair_sender_snapshots c JOIN onchain_amm_pool_new_pair_snapshots p ON p.id=c.pool_id
 ORDER BY c.pool_id LIMIT ?`, limits.Pools, func(row scanner) error {
		var p Snapshot
		var c SenderSnapshot
		var id, creation []byte
		err := row.Scan(&id, &p.Identity.ChainFamily, &p.Identity.Chain, &p.Identity.Network, &p.Identity.Venue, &p.Identity.PoolID, &p.Token0ID, &p.Token1ID, &p.CreatedAt, &p.SwapObservedAt, &p.PositionKind, &p.SwapObservedPositionNumber, &p.SwapObservedPositionID, &p.ConfirmedAt, &p.LiquidityUSD, &p.LiquidityEvaluatedAt, &p.State, &p.Revision, &p.Canonical, &p.UpdatedAt,
			&creation, &c.Generation, &c.InitializedAt, &c.InvalidFrom, &c.InvalidTo, &c.UpdatedAt)
		if err != nil {
			return fmt.Errorf("failed to decode sender parent: %w", err)
		}
		want := p.Identity.ID()
		if !bytes.Equal(id, want[:]) || len(creation) != 32 {
			return fmt.Errorf("failed to decode sender parent: identity=invalid")
		}
		c.Pool = p.Identity
		copy(c.CreationEventID[:], creation)
		if err := c.Validate(); err != nil {
			return err
		}
		states[c.Pool] = c.Generation
		return consume.Pool(p, c)
	})
	if err != nil {
		return fmt.Errorf("failed to restore sender parents: %w", err)
	}
	err = s.walkSenderRows(ctx, tx, "SELECT "+senderTransactionColumns+" FROM onchain_amm_pool_new_pair_sender_transactions ORDER BY id LIMIT ?", limits.Transactions, func(row scanner) error {
		t, err := scanSenderTransaction(row)
		if err != nil {
			return fmt.Errorf("failed to decode restored sender transaction: %w", err)
		}
		return consume.Transaction(t)
	})
	if err != nil {
		return fmt.Errorf("failed to restore sender transactions: %w", err)
	}
	counts := make(map[Identity]int)
	err = s.walkSenderRows(ctx, tx, `SELECT e.id,p.chain_family,p.chain,p.network,p.venue,p.pool_id,
 e.generation,e.transaction_ref,t.chain_family,t.chain,t.network,t.transaction_id,
 e.position_number,e.position_id,e.event_index,e.minute_started_at,e.direction,e.is_canonical,e.observed_at,e.expires_at,e.updated_at
 FROM onchain_amm_pool_new_pair_sender_events e
 JOIN onchain_amm_pool_new_pair_snapshots p ON p.id=e.pool_id
 JOIN onchain_amm_pool_new_pair_sender_transactions t ON t.id=e.transaction_ref
 ORDER BY e.pool_id,e.id LIMIT ?`, limits.Events, func(row scanner) error {
		var e SenderEvent
		var id, ref []byte
		if err := row.Scan(&id, &e.Pool.ChainFamily, &e.Pool.Chain, &e.Pool.Network, &e.Pool.Venue, &e.Pool.PoolID, &e.Generation, &ref, &e.Transaction.ChainFamily, &e.Transaction.Chain, &e.Transaction.Network, &e.Transaction.TransactionID,
			&e.PositionNumber, &e.PositionID, &e.Index, &e.MinuteStartedAt, &e.Direction, &e.Canonical, &e.ObservedAt, &e.ExpiresAt, &e.UpdatedAt); err != nil {
			return fmt.Errorf("failed to decode restored sender event: %w", err)
		}
		want, wantRef := e.ID(), e.Transaction.ID()
		if !bytes.Equal(id, want[:]) || !bytes.Equal(ref, wantRef[:]) || states[e.Pool] != e.Generation {
			return fmt.Errorf("failed to decode restored sender event: identity_or_generation=invalid")
		}
		if err := e.Validate(); err != nil {
			return err
		}
		counts[e.Pool]++
		if counts[e.Pool] > limits.EventsPerPool {
			return fmt.Errorf("failed to restore sender pool events: %w: max_rows=%d", ErrSenderCapacity, limits.EventsPerPool)
		}
		return consume.Event(e)
	})
	if err != nil {
		return fmt.Errorf("failed to restore sender events: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to finish sender restore: %w", err)
	}
	return nil
}

func (s *Store) walkSenderRows(ctx context.Context, tx *sql.Tx, query string, limit int, consume func(scanner) error) (err error) {
	rows, err := tx.QueryContext(ctx, s.query(query), limit+1)
	if err != nil {
		return fmt.Errorf("failed to read sender rows: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close sender rows: %w", e))
		}
	}()
	n := 0
	for rows.Next() {
		n++
		if n > limit {
			return fmt.Errorf("failed to restore sender rows: %w: max_rows=%d", ErrSenderCapacity, limit)
		}
		if err := consume(rows); err != nil {
			return fmt.Errorf("failed to restore sender row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read sender rows: %w", err)
	}
	return nil
}
