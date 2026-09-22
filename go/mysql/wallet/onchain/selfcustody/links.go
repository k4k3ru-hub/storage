package selfcustody

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	api "github.com/k4k3ru-hub/storage/go/api"
	"time"
)

const linkColumns = "id,address_id,subject,display_name,linked_at,unlinked_at"

type CompleteLinkParams struct {
	ID          uint64
	AddressID   uint64
	DisplayName string
	Challenge   ConsumeChallengeParams
}

// CompleteLink consumes verified proof and inserts a link in the caller's transaction.
// The caller must verify the stored challenge's exact signature first and roll back
// the transaction on every error. This method never commits or verifies signatures.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) CompleteLink(ctx context.Context, tx *sql.Tx, p CompleteLinkParams) (*Link, error) {
	const op = "failed to complete wallet link"
	if err := s.guard(ctx, tx); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(p.ID, "id"); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(p.AddressID, "address_id"); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := textField(p.DisplayName, "display_name", 128, true); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	// All mutations lock the persistent identity first, including when there is no active link.
	a, err := scanAddress(tx.QueryRowContext(ctx, "SELECT id,chain_family,address,created_at FROM "+quote(s.addressTable)+" WHERE id=? FOR UPDATE", p.AddressID))
	if err != nil {
		return nil, dbError(op, err)
	}
	if a.ChainFamily != p.Challenge.ChainFamily || a.Address != p.Challenge.Address || p.Challenge.Now.Before(a.CreatedAt) {
		return nil, fmt.Errorf("%s: %w", op, ErrConflict)
	}
	var existing uint64
	err = tx.QueryRowContext(ctx, "SELECT id FROM "+quote(s.linkTable)+" WHERE active_address_id=? FOR UPDATE", p.AddressID).Scan(&existing)
	if err == nil {
		return nil, fmt.Errorf("%s: %w", op, ErrConflict)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, dbError(op, err)
	}
	if err := s.ConsumeLinkChallenge(ctx, tx, p.Challenge); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO "+quote(s.linkTable)+" (id,address_id,subject,display_name,linked_at) VALUES (?,?,?,?,?)", p.ID, p.AddressID, p.Challenge.Subject, p.DisplayName, p.Challenge.Now.UTC())
	if err != nil {
		return nil, dbError(op, err)
	}
	return &Link{ID: p.ID, AddressID: p.AddressID, Subject: p.Challenge.Subject, DisplayName: p.DisplayName, LinkedAt: p.Challenge.Now.UTC()}, nil
}

// SelectLink retrieves an active or historical link owned by the supplied subject.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) SelectLink(ctx context.Context, e api.Executor, id uint64, subject string) (*Link, error) {
	const op = "failed to select wallet link"
	if err := s.guard(ctx, e); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(id, "id"); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := textField(subject, "subject", 255, false); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	v, err := scanLink(e.QueryRowContext(ctx, "SELECT "+linkColumns+" FROM "+quote(s.linkTable)+" WHERE id=? AND subject=?", id, subject))
	if err != nil {
		return nil, dbError(op, err)
	}
	return v, nil
}

// Unlink closes an owned active link without deleting history or touching OMS.
// Call only after application lifecycle checks and execution fencing. Roll back on errors.
// Repeated calls for the same owned closed link preserve its original unlink time.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) Unlink(ctx context.Context, tx *sql.Tx, id uint64, subject string, now time.Time) error {
	const op = "failed to unlink wallet"
	if err := timestamp(now, "now"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	v, err := s.SelectLink(ctx, tx, id, subject)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	var addressID uint64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM "+quote(s.addressTable)+" WHERE id=? FOR UPDATE", v.AddressID).Scan(&addressID); err != nil {
		return dbError(op, err)
	}
	v, err = scanLink(tx.QueryRowContext(ctx, "SELECT "+linkColumns+" FROM "+quote(s.linkTable)+" WHERE id=? AND subject=? FOR UPDATE", id, subject))
	if err != nil {
		return dbError(op, err)
	}
	if v.UnlinkedAt != nil {
		return nil
	}
	if now.Before(v.LinkedAt) {
		return fmt.Errorf("%s: %w", op, invalid("now", "out_of_range"))
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+quote(s.linkTable)+" SET unlinked_at=? WHERE id=? AND subject=? AND unlinked_at IS NULL", now.UTC(), id, subject)
	return dbError(op, err)
}

// ListLinks reads bounded subject-owned links in ascending ID order.
// Set includeHistory to include closed links; afterID is an exclusive cursor.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) ListLinks(ctx context.Context, e api.Executor, subject string, afterID uint64, limit int, includeHistory bool) (result []Link, err error) {
	const op = "failed to list wallet links"
	if err := s.guard(ctx, e); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := textField(subject, "subject", 255, false); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	query := "SELECT " + linkColumns + " FROM " + quote(s.linkTable) + " WHERE subject=? AND id>?"
	if !includeHistory {
		query += " AND unlinked_at IS NULL"
	}
	query += " ORDER BY id ASC LIMIT ?"
	rows, err := e.QueryContext(ctx, query, subject, afterID, limit)
	if err != nil {
		return nil, dbError(op, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, dbError(op, closeErr))
			result = nil
		}
	}()
	result = make([]Link, 0)
	for rows.Next() {
		v, scanErr := scanLink(rows)
		if scanErr != nil {
			return nil, dbError(op, scanErr)
		}
		result = append(result, *v)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError(op, err)
	}
	return result, nil
}
