package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"
)

const senderTransactionColumns = `id,chain_family,chain,network,transaction_id,sender_id,status,attempts,next_attempt_at,deadline_at,expires_at,created_at,updated_at`

func scanSenderTransaction(row scanner) (SenderTransaction, error) {
	var t SenderTransaction
	var id []byte
	if err := row.Scan(&id, &t.Key.ChainFamily, &t.Key.Chain, &t.Key.Network, &t.Key.TransactionID, &t.SenderID, &t.Status, &t.Attempts, &t.NextAttemptAt, &t.DeadlineAt, &t.ExpiresAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return t, err
	}
	want := t.Key.ID()
	if !bytes.Equal(id, want[:]) {
		return t, fmt.Errorf("failed to decode sender transaction: identity=invalid")
	}
	return t, t.Validate()
}

func (s *Store) senderTransaction(ctx context.Context, tx *sql.Tx, key SenderTransactionKey) (SenderTransaction, error) {
	id := key.ID()
	return scanSenderTransaction(tx.QueryRowContext(ctx, s.query("SELECT "+senderTransactionColumns+" FROM onchain_amm_pool_new_pair_sender_transactions WHERE id=? FOR UPDATE"), id[:]))
}

func (s *Store) commitSenders(ctx context.Context, tx *sql.Tx, b Batch) error {
	if len(b.SenderSnapshots)+len(b.SenderTransactions)+len(b.SenderEvents)+len(b.SenderEvidence)+len(b.SenderInvalidations)+len(b.ResetSenders)+len(b.AdmitSenderEvents)+len(b.ReacceptSenderEvents) == 0 {
		return nil
	}
	for _, v := range b.SenderInvalidations {
		id := v.Pool.ID()
		// This path also works when bounded restore failed and the application has
		// no trusted copy of the stored generation. Never create an empty snapshot.
		_, err := tx.ExecContext(ctx, s.query(`UPDATE onchain_amm_pool_new_pair_sender_snapshots
 SET invalid_from=LEAST(COALESCE(invalid_from,?),?),invalid_to=GREATEST(COALESCE(invalid_to,?),?),updated_at=GREATEST(updated_at,?)
 WHERE pool_id=?`), dbTime(v.From), dbTime(v.From), dbTime(v.To), dbTime(v.To), dbTime(v.UpdatedAt), id[:])
		if err != nil {
			return fmt.Errorf("failed to invalidate sender snapshot: %w", err)
		}
	}
	resets := make(map[Identity]bool, len(b.ResetSenders))
	for _, p := range b.ResetSenders {
		resets[p] = true
	}
	for _, p := range b.SenderSnapshots {
		if err := s.commitSenderSnapshot(ctx, tx, p, resets[p.Pool]); err != nil {
			return err
		}
	}
	// All sources acquire shared transaction locks in the same order.
	transactions := append([]SenderTransaction(nil), b.SenderTransactions...)
	sort.Slice(transactions, func(i, j int) bool {
		a, c := transactions[i].Key.ID(), transactions[j].Key.ID()
		return bytes.Compare(a[:], c[:]) < 0
	})
	for _, t := range transactions {
		id := t.Key.ID()
		_, err := tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_sender_transactions
 (id,chain_family,chain,network,transaction_id,sender_id,status,attempts,next_attempt_at,deadline_at,expires_at,created_at,updated_at)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=id`), id[:], t.Key.ChainFamily, t.Key.Chain, t.Key.Network, t.Key.TransactionID, t.SenderID, t.Status, t.Attempts, senderUTC(t.NextAttemptAt), dbTime(t.DeadlineAt), dbTime(t.ExpiresAt), dbTime(t.CreatedAt), dbTime(t.UpdatedAt))
		if err != nil {
			return fmt.Errorf("failed to initialize sender transaction: %w", err)
		}
		saved, err := s.senderTransaction(ctx, tx, t.Key)
		if err != nil {
			return fmt.Errorf("failed to verify sender transaction: %w", err)
		}
		if saved.Key != t.Key || saved.SenderID != nil && t.SenderID != nil && *saved.SenderID != *t.SenderID {
			return fmt.Errorf("failed to initialize sender transaction: %w", ErrSenderConflict)
		}
	}
	if len(b.SenderEvents) == 0 {
		return nil
	}
	wanted := make(map[[32]byte]bool, len(b.SenderEvents))
	for _, e := range b.SenderEvents {
		wanted[e.ID()] = true
	}
	evidence := make(map[[32]byte]Event, len(b.SenderEvents))
	for _, e := range b.Events {
		id := e.ID()
		if wanted[id] {
			evidence[id] = e
		}
	}
	for _, e := range b.SenderEvidence {
		id := e.ID()
		if raw, ok := evidence[id]; ok && (raw.PositionNumber != e.PositionNumber || raw.Canonical != e.Canonical || !sameTime(raw.ObservedAt, e.ObservedAt)) {
			return fmt.Errorf("failed to verify sender evidence: archive_event=conflicting")
		}
		evidence[id] = e
	}
	admissions := make(map[[32]byte]bool, len(b.AdmitSenderEvents))
	for _, id := range b.AdmitSenderEvents {
		admissions[id] = true
	}
	reacceptances := make(map[[32]byte]bool, len(b.ReacceptSenderEvents))
	for _, id := range b.ReacceptSenderEvents {
		reacceptances[id] = true
	}
	for _, e := range b.SenderEvents {
		id := e.ID()
		if err := s.commitSenderEvent(ctx, tx, e, evidence, admissions[id], reacceptances[id]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) commitSenderSnapshot(ctx context.Context, tx *sql.Tx, p SenderSnapshot, reset bool) error {
	id := p.Pool.ID()
	old := SenderSnapshot{Pool: p.Pool}
	var creation []byte
	err := tx.QueryRowContext(ctx, s.query(`SELECT creation_event_id,generation,initialized_at,invalid_from,invalid_to,updated_at
 FROM onchain_amm_pool_new_pair_sender_snapshots WHERE pool_id=? FOR UPDATE`), id[:]).Scan(&creation, &old.Generation, &old.InitializedAt, &old.InvalidFrom, &old.InvalidTo, &old.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		if reset || p.Generation != 1 {
			return fmt.Errorf("failed to initialize sender snapshot: %w", ErrSenderConflict)
		}
		_, err = tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_sender_snapshots
 (pool_id,creation_event_id,generation,initialized_at,invalid_from,invalid_to,updated_at) VALUES (?,?,?,?,?,?,?)`), id[:], p.CreationEventID[:], p.Generation, dbTime(p.InitializedAt), senderUTC(p.InvalidFrom), senderUTC(p.InvalidTo), dbTime(p.UpdatedAt))
		if err != nil {
			return fmt.Errorf("failed to initialize sender snapshot: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to lock sender snapshot: %w", err)
	}
	if len(creation) != 32 {
		return fmt.Errorf("failed to decode sender snapshot: creation_event_id=invalid")
	}
	copy(old.CreationEventID[:], creation)
	if err := old.Validate(); err != nil {
		return err
	}
	if dbTime(p.UpdatedAt).Before(dbTime(old.UpdatedAt)) {
		return fmt.Errorf("failed to update sender snapshot: %w", ErrSenderConflict)
	}
	if reset {
		if old.Generation == ^uint64(0) || p.Generation != old.Generation+1 {
			return fmt.Errorf("failed to reset sender snapshot: %w", ErrSenderConflict)
		}
		if _, err := tx.ExecContext(ctx, s.query("DELETE FROM onchain_amm_pool_new_pair_sender_events WHERE pool_id=?"), id[:]); err != nil {
			return fmt.Errorf("failed to reset sender events: %w", err)
		}
	} else {
		if p.Generation != old.Generation || p.CreationEventID != old.CreationEventID || !sameTime(p.InitializedAt, old.InitializedAt) {
			return fmt.Errorf("failed to update sender snapshot: %w", ErrSenderConflict)
		}
		// A live invalid interval may only expand. Expired intervals can be discarded.
		if old.InvalidTo != nil && old.InvalidTo.After(p.UpdatedAt.Truncate(time.Minute).Add(-15*time.Minute)) &&
			(p.InvalidFrom == nil || p.InvalidFrom.After(*old.InvalidFrom) || p.InvalidTo.Before(*old.InvalidTo)) {
			return fmt.Errorf("failed to update sender snapshot: invalid_interval=invalid")
		}
	}
	_, err = tx.ExecContext(ctx, s.query(`UPDATE onchain_amm_pool_new_pair_sender_snapshots
 SET creation_event_id=?,generation=?,initialized_at=?,invalid_from=?,invalid_to=?,updated_at=? WHERE pool_id=?`), p.CreationEventID[:], p.Generation, dbTime(p.InitializedAt), senderUTC(p.InvalidFrom), senderUTC(p.InvalidTo), dbTime(p.UpdatedAt), id[:])
	if err != nil {
		return fmt.Errorf("failed to update sender snapshot: %w", err)
	}
	return nil
}

func (s *Store) commitSenderEvent(ctx context.Context, tx *sql.Tx, e SenderEvent, evidence map[[32]byte]Event, admit, reaccept bool) error {
	id, poolID, ref := e.ID(), e.Pool.ID(), e.Transaction.ID()
	base, hasEvidence := evidence[id]
	if admit || reaccept {
		if !hasEvidence || !base.Canonical || base.PositionNumber != e.PositionNumber {
			return fmt.Errorf("failed to adopt sender event: activity_event=invalid")
		}
	}
	old := e
	var savedRef []byte
	err := tx.QueryRowContext(ctx, s.query(`SELECT generation,transaction_ref,position_number,minute_started_at,direction,is_canonical,observed_at,expires_at,updated_at
 FROM onchain_amm_pool_new_pair_sender_events WHERE id=? FOR UPDATE`), id[:]).Scan(&old.Generation, &savedRef, &old.PositionNumber, &old.MinuteStartedAt, &old.Direction, &old.Canonical, &old.ObservedAt, &old.ExpiresAt, &old.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		// Corrections, cancellations and reacceptances never recreate a removed row.
		if !admit {
			return nil
		}
		if !sameTime(base.ObservedAt, e.ObservedAt) {
			return fmt.Errorf("failed to admit sender event: activity_event=invalid")
		}
		if !dbTime(e.ExpiresAt).After(dbTime(e.UpdatedAt)) {
			return fmt.Errorf("failed to admit sender event: expires_at=out_of_range")
		}
		_, err = tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_sender_events
 (id,pool_id,generation,transaction_ref,position_number,position_id,event_index,minute_started_at,direction,is_canonical,observed_at,expires_at,updated_at)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`), id[:], poolID[:], e.Generation, ref[:], e.PositionNumber, e.PositionID, e.Index, senderUTC(e.MinuteStartedAt), e.Direction, e.Canonical, dbTime(e.ObservedAt), dbTime(e.ExpiresAt), dbTime(e.UpdatedAt))
		if err != nil {
			return fmt.Errorf("failed to admit sender event: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to lock sender event: %w", err)
	}
	if old.Generation != e.Generation || !bytes.Equal(savedRef, ref[:]) || old.PositionNumber != e.PositionNumber || old.Direction != e.Direction || !sameTime(old.ObservedAt, e.ObservedAt) || dbTime(e.UpdatedAt).Before(dbTime(old.UpdatedAt)) {
		return fmt.Errorf("failed to update sender event: %w", ErrSenderConflict)
	}
	if !e.Canonical {
		if old.Canonical && (!hasEvidence || base.Canonical || base.PositionNumber != e.PositionNumber) {
			return fmt.Errorf("failed to cancel sender event: activity_event=invalid")
		}
		// A cancellation must succeed even if its accompanying time correction is late.
		// Keep the old minute/expiry, so cancellation cannot prolong retention either.
		e.MinuteStartedAt = old.MinuteStartedAt
		e.ExpiresAt = old.ExpiresAt
	} else {
		if !old.Canonical && !reaccept {
			return fmt.Errorf("failed to update sender event: %w", ErrSenderConflict)
		}
		if !dbTime(old.ExpiresAt).After(dbTime(e.UpdatedAt)) || !dbTime(e.ExpiresAt).After(dbTime(e.UpdatedAt)) {
			// Logical expiry applies even before GC. Leave the contribution unavailable,
			// without rolling back an otherwise valid Activity correction/reacceptance.
			return nil
		}
		if old.MinuteStartedAt != nil && !sameOptionalTime(old.MinuteStartedAt, e.MinuteStartedAt) {
			return fmt.Errorf("failed to update sender event: minute_started_at=invalid")
		}
	}
	_, err = tx.ExecContext(ctx, s.query(`UPDATE onchain_amm_pool_new_pair_sender_events
 SET minute_started_at=?,is_canonical=?,expires_at=?,updated_at=? WHERE id=?`), senderUTC(e.MinuteStartedAt), e.Canonical, dbTime(e.ExpiresAt), dbTime(e.UpdatedAt), id[:])
	if err != nil {
		return fmt.Errorf("failed to update sender event: %w", err)
	}
	return nil
}
