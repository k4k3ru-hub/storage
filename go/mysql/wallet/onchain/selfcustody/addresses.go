package selfcustody

import (
	"context"
	"fmt"
	api "github.com/k4k3ru-hub/storage/go/api"
)

// InsertAddress saves a canonical address without changing existing identities.
// The caller supplies the ID and performs chain-specific canonicalization.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) InsertAddress(ctx context.Context, e api.Executor, v Address) error {
	const op = "failed to insert wallet address"
	if err := s.guard(ctx, e); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(v.ID, "id"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(v.ChainFamily, v.Address); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := timestamp(v.CreatedAt, "created_at"); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	_, err := e.ExecContext(ctx, "INSERT INTO "+quote(s.addressTable)+" (id,chain_family,address,created_at) VALUES (?,?,?,?)", v.ID, v.ChainFamily, v.Address, v.CreatedAt.UTC())
	return dbError(op, err)
}

// SelectAddress reads an address by ID, preserving sql.ErrNoRows when absent.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) SelectAddress(ctx context.Context, e api.Executor, id uint64) (*Address, error) {
	const op = "failed to select wallet address"
	if err := s.guard(ctx, e); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := positive(id, "id"); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	v, err := scanAddress(e.QueryRowContext(ctx, "SELECT id,chain_family,address,created_at FROM "+quote(s.addressTable)+" WHERE id=?", id))
	if err != nil {
		return nil, dbError(op, err)
	}
	return v, nil
}

// SelectAddressByIdentity finds a canonical chain-family/address identity.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) SelectAddressByIdentity(ctx context.Context, e api.Executor, family, address string) (*Address, error) {
	const op = "failed to select wallet address by identity"
	if err := s.guard(ctx, e); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(family, address); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	v, err := scanAddress(e.QueryRowContext(ctx, "SELECT id,chain_family,address,created_at FROM "+quote(s.addressTable)+" WHERE chain_family=? AND address=?", family, address))
	if err != nil {
		return nil, dbError(op, err)
	}
	return v, nil
}
