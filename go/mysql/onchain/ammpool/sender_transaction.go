package ammpool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SenderTransaction reads a shared resolution, preserving sql.ErrNoRows.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) SenderTransaction(ctx context.Context, key SenderTransactionKey) (SenderTransaction, error) {
	if err := key.Validate(); err != nil {
		return SenderTransaction{}, fmt.Errorf("failed to read sender transaction: %w", err)
	}
	id := key.ID()
	t, err := scanSenderTransaction(s.db.QueryRowContext(ctx, s.query("SELECT "+senderTransactionColumns+" FROM onchain_amm_pool_new_pair_sender_transactions WHERE id=?"), id[:]))
	if err != nil {
		return SenderTransaction{}, fmt.Errorf("failed to read sender transaction: %w", err)
	}
	return t, nil
}

// ReserveSenderAttempt durably consumes one attempt and returns its ten-second lease.
// The caller must acquire its RPC budget separately and send only after this commits.
// A crash after reservation consumes the attempt, even if no request was sent.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) ReserveSenderAttempt(ctx context.Context, key SenderTransactionKey, expectedAttempts uint8, now time.Time) (out SenderTransaction, err error) {
	if err := key.Validate(); err != nil {
		return out, fmt.Errorf("failed to reserve sender attempt: %w", err)
	}
	if !senderTime(now) || expectedAttempts >= MaxSenderAttempts {
		return out, fmt.Errorf("failed to reserve sender attempt: reservation=invalid")
	}
	err = s.withSenderTransaction(ctx, func(tx *sql.Tx) error {
		t, err := s.senderTransaction(ctx, tx, key)
		if err != nil {
			return fmt.Errorf("failed to lock sender transaction: %w", err)
		}
		now = dbTime(now)
		if t.Status != SenderPending || t.Attempts != expectedAttempts || t.NextAttemptAt.After(now) || !now.Before(t.DeadlineAt) || now.Before(t.UpdatedAt) {
			return ErrSenderConflict
		}
		t.Attempts++
		until := now.Add(10 * time.Second)
		if until.After(t.DeadlineAt) {
			until = t.DeadlineAt
		}
		t.NextAttemptAt = &until
		t.UpdatedAt = now
		if err := s.saveSenderTransaction(ctx, tx, t); err != nil {
			return err
		}
		out = t
		return nil
	})
	if err != nil {
		return SenderTransaction{}, fmt.Errorf("failed to reserve sender attempt: %w", err)
	}
	return out, nil
}

// CompleteSenderAttempt saves a result only while the returned reservation still owns the row.
// Retry timestamps must precede the fixed deadline; terminal records cannot be reopened.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) CompleteSenderAttempt(ctx context.Context, reservation SenderTransaction, result SenderAttemptResult) error {
	if err := reservation.Validate(); err != nil {
		return fmt.Errorf("failed to complete sender attempt: %w", err)
	}
	if reservation.Status != SenderPending || reservation.Attempts == 0 || !senderTime(result.UpdatedAt) || dbTime(result.UpdatedAt).Before(dbTime(reservation.UpdatedAt)) {
		return fmt.Errorf("failed to complete sender attempt: reservation=invalid")
	}
	updated := reservation
	updated.Status = result.Status
	updated.SenderID = result.SenderID
	updated.NextAttemptAt = result.NextAttemptAt
	updated.UpdatedAt = dbTime(result.UpdatedAt)
	if err := updated.Validate(); err != nil {
		return fmt.Errorf("failed to complete sender attempt: %w", err)
	}
	if updated.Status != SenderAbandoned && !updated.UpdatedAt.Before(updated.DeadlineAt) {
		return fmt.Errorf("failed to complete sender attempt: deadline=out_of_range")
	}
	if updated.Status == SenderPending && (updated.Attempts >= MaxSenderAttempts || !dbTime(*updated.NextAttemptAt).After(updated.UpdatedAt) || !dbTime(*updated.NextAttemptAt).Before(dbTime(updated.DeadlineAt)) || sameTime(updated.UpdatedAt, reservation.UpdatedAt) && sameOptionalTime(updated.NextAttemptAt, reservation.NextAttemptAt)) {
		return fmt.Errorf("failed to complete sender attempt: retry=invalid")
	}
	err := s.withSenderTransaction(ctx, func(tx *sql.Tx) error {
		t, err := s.senderTransaction(ctx, tx, reservation.Key)
		if err != nil {
			return fmt.Errorf("failed to lock sender transaction: %w", err)
		}
		if t.Status != SenderPending || t.Attempts != reservation.Attempts || !sameTime(t.UpdatedAt, reservation.UpdatedAt) || !sameOptionalTime(t.NextAttemptAt, reservation.NextAttemptAt) || !sameTime(t.CreatedAt, reservation.CreatedAt) || !sameTime(t.DeadlineAt, reservation.DeadlineAt) || !sameTime(t.ExpiresAt, reservation.ExpiresAt) {
			return ErrSenderConflict
		}
		return s.saveSenderTransaction(ctx, tx, updated)
	})
	if err != nil {
		return fmt.Errorf("failed to complete sender attempt: %w", err)
	}
	return nil
}

func (s *Store) saveSenderTransaction(ctx context.Context, tx *sql.Tx, t SenderTransaction) error {
	id := t.Key.ID()
	_, err := tx.ExecContext(ctx, s.query(`UPDATE onchain_amm_pool_new_pair_sender_transactions
 SET sender_id=?,status=?,attempts=?,next_attempt_at=?,updated_at=? WHERE id=?`), t.SenderID, t.Status, t.Attempts, senderUTC(t.NextAttemptAt), dbTime(t.UpdatedAt), id[:])
	if err != nil {
		return fmt.Errorf("failed to save sender transaction: %w", err)
	}
	return nil
}

func (s *Store) withSenderTransaction(ctx context.Context, apply func(*sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin sender transaction: %w", err)
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("failed to roll back sender transaction: %w", e))
		}
	}()
	if err := apply(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit sender transaction: %w", err)
	}
	return nil
}

// ExpireSenderTransactions abandons expired or exhausted pending records in one bounded batch.
// A final in-flight attempt is retained until its lease expires.
//
// Version:
//   - 2026-09-28: Added.
func (s *Store) ExpireSenderTransactions(ctx context.Context, now time.Time, limit int) (int64, error) {
	if !senderTime(now) || limit < 1 || limit > 1000 {
		return 0, fmt.Errorf("failed to expire sender transactions: bounds=out_of_range")
	}
	r, err := s.db.ExecContext(ctx, s.query(`UPDATE onchain_amm_pool_new_pair_sender_transactions
 SET status='abandoned',next_attempt_at=NULL,updated_at=?
 WHERE status='pending' AND updated_at<=? AND (deadline_at<=? OR (attempts>=4 AND next_attempt_at<=?))
 ORDER BY deadline_at,id LIMIT ?`), dbTime(now), dbTime(now), dbTime(now), dbTime(now), limit)
	if err != nil {
		return 0, fmt.Errorf("failed to expire sender transactions: %w", err)
	}
	n, err := r.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to count expired sender transactions: %w", err)
	}
	return n, nil
}
