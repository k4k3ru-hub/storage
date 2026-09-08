package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Revoke marks an owned credential revoked within the caller's transaction.
// The caller must commit the transaction before reporting success.
// Already revoked rows succeed; missing and differently owned rows return false.
//
// Version:
//   - 2026-09-09: Added.
func (s *CredentialStore) Revoke(ctx context.Context, tx *sql.Tx, accountID, credentialID uint64) (bool, error) {
	if s == nil || tx == nil || ctx == nil {
		return false, fmt.Errorf("failed to revoke credential: dependency=null")
	}
	if accountID == 0 || credentialID == 0 {
		return false, fmt.Errorf("failed to revoke credential: identifier=empty")
	}
	var status CredentialStatus
	err := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT status FROM %s WHERE id = ? AND account_id = ? FOR UPDATE", s.tableName), credentialID, accountID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to read revocation target: %w", err)
	}
	if status == CredentialStatusRevoked {
		return true, nil
	}
	result, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND account_id = ?", s.tableName), CredentialStatusRevoked, credentialID, accountID)
	if err != nil {
		return false, fmt.Errorf("failed to revoke credential: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to check credential revocation: %w", err)
	}
	if count != 1 {
		return false, fmt.Errorf("failed to revoke credential: affected_rows=invalid")
	}
	return true, nil
}
