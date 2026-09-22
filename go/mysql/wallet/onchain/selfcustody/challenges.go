package selfcustody

import (
	"context"
	"database/sql"
	"fmt"
	api "github.com/k4k3ru-hub/storage/go/api"
	"time"
)

const challengeColumns = "id,subject,binding_hash,chain_family,address,nonce,message,created_at,expires_at,consumed_at"

type ConsumeChallengeParams struct {
	ID          uint64
	Subject     string
	BindingHash [32]byte
	ChainFamily string
	Address     string
	Now         time.Time
}

// InsertLinkChallenge saves an unconsumed ownership challenge, without verifying a signature.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) InsertLinkChallenge(ctx context.Context, e api.Executor, v LinkChallenge) error {
	const op = "failed to insert wallet link challenge"
	if err := s.guard(ctx, e); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(v.ID, "id"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := textField(v.Subject, "subject", 255, false); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(v.ChainFamily, v.Address); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := timestamp(v.CreatedAt, "created_at"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := timestamp(v.ExpiresAt, "expires_at"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if !v.ExpiresAt.After(v.CreatedAt) {
		return fmt.Errorf("%s: %w", op, invalid("expires_at", "out_of_range"))
	}
	if v.ConsumedAt != nil {
		return fmt.Errorf("%s: %w", op, invalid("consumed_at", "invalid"))
	}
	if v.BindingHash == ([32]byte{}) || v.Nonce == ([32]byte{}) {
		return fmt.Errorf("%s: %w", op, invalid("challenge_binding", "empty"))
	}
	if len(v.Message) == 0 {
		return fmt.Errorf("%s: %w", op, invalid("message", "empty"))
	}
	if len(v.Message) > 65535 {
		return fmt.Errorf("%s: %w", op, invalid("message", "too_long"))
	}
	_, err := e.ExecContext(ctx, "INSERT INTO "+quote(s.challengeTable)+" ("+challengeColumns+") VALUES (?,?,?,?,?,?,?,?,?,NULL)", v.ID, v.Subject, v.BindingHash[:], v.ChainFamily, v.Address, v.Nonce[:], v.Message, v.CreatedAt.UTC(), v.ExpiresAt.UTC())
	return dbError(op, err)
}

// SelectLinkChallenge retrieves a challenge only within the supplied subject.
// Signature verification belongs to the caller; expired and consumed records remain readable.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) SelectLinkChallenge(ctx context.Context, e api.Executor, id uint64, subject string) (*LinkChallenge, error) {
	const op = "failed to select wallet link challenge"
	if err := s.guard(ctx, e); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(id, "id"); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := textField(subject, "subject", 255, false); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	v, err := scanChallenge(e.QueryRowContext(ctx, "SELECT "+challengeColumns+" FROM "+quote(s.challengeTable)+" WHERE id=? AND subject=?", id, subject))
	if err != nil {
		return nil, dbError(op, err)
	}
	return v, nil
}

// ConsumeLinkChallenge consumes a matching, unexpired challenge in the caller's transaction.
// Call only after verifying the exact stored message's signature. Roll back on errors.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) ConsumeLinkChallenge(ctx context.Context, tx *sql.Tx, p ConsumeChallengeParams) error {
	const op = "failed to consume wallet link challenge"
	if err := s.guard(ctx, tx); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(p.ID, "id"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := textField(p.Subject, "subject", 255, false); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(p.ChainFamily, p.Address); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := timestamp(p.Now, "now"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if p.BindingHash == ([32]byte{}) {
		return fmt.Errorf("%s: %w", op, invalid("binding_hash", "empty"))
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+quote(s.challengeTable)+" SET consumed_at=? WHERE id=? AND subject=? AND binding_hash=? AND chain_family=? AND address=? AND consumed_at IS NULL AND created_at<=? AND expires_at>?", p.Now.UTC(), p.ID, p.Subject, p.BindingHash[:], p.ChainFamily, p.Address, p.Now.UTC(), p.Now.UTC())
	if err != nil {
		return dbError(op, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return dbError(op, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: %w", op, ErrConflict)
	}
	return nil
}
