package oms

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	api "github.com/k4k3ru-hub/storage/go/api"
	validator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const DefaultPerpetualVenueAccountSnapshotTableName = "oms_perpetual_venue_account_snapshots"

//go:embed schema.perpetual_venue_account_snapshots.sql
var perpetualVenueAccountSnapshotSchema string

type PerpetualVenueAccountScope struct {
	AccountID      uint64
	Venue          string
	Network        string
	TradingAccount string
}

type PerpetualVenueAccountCheckpoint struct {
	SnapshotVersion   uint16
	ObservedThroughMS uint64
	Snapshot          json.RawMessage
}

type PerpetualVenueAccountSnapshot struct {
	PerpetualVenueAccountScope
	PerpetualVenueAccountCheckpoint
	Revision  uint64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type PerpetualVenueAccountSnapshotStore struct{ table string }

// NewPerpetualVenueAccountSnapshotStore composes independent OMS checkpoint persistence.
// The caller owns the database, transactions and venue-specific JSON validation.
//
// Version:
//   - 2026-10-04: Added.
func NewPerpetualVenueAccountSnapshotStore(table string) (*PerpetualVenueAccountSnapshotStore, error) {
	if err := validator.ValidateSQLIdentifier(table, "table_name"); err != nil {
		return nil, fmt.Errorf("failed to create perpetual venue account snapshot store: %w", errors.Join(ErrInvalidParameter, err))
	}
	if len(table) > 64 {
		return nil, fmt.Errorf("failed to create perpetual venue account snapshot store: %w", invalid("table_name", "too_long"))
	}
	return &PerpetualVenueAccountSnapshotStore{table: table}, nil
}

// Validate validates the tenant and exact venue account identity without case normalization.
//
// Version:
//   - 2026-10-04: Added.
func (s PerpetualVenueAccountScope) Validate() error {
	if s.AccountID == 0 {
		return fmt.Errorf("failed to validate perpetual venue account scope: %w", invalid("account_id", "empty"))
	}
	for _, v := range []struct {
		name, value string
		max         int
	}{{"venue", s.Venue, 64}, {"network", s.Network, 64}, {"trading_account", s.TradingAccount, 255}} {
		if err := textValue(v.name, v.value, v.max, true); err != nil {
			return fmt.Errorf("failed to validate perpetual venue account scope: %w", err)
		}
	}
	return nil
}

// Validate validates a checkpoint envelope; the producer validates its versioned content.
//
// Version:
//   - 2026-10-04: Added.
func (c PerpetualVenueAccountCheckpoint) Validate() error {
	if c.SnapshotVersion == 0 || c.ObservedThroughMS == 0 {
		return fmt.Errorf("failed to validate perpetual venue account checkpoint: %w", invalid("version_or_time", "empty"))
	}
	if len(c.Snapshot) > 8*1024*1024 {
		return fmt.Errorf("failed to validate perpetual venue account checkpoint: %w", invalid("snapshot", "too_long"))
	}
	if err := jsonObject("snapshot", c.Snapshot, true); err != nil {
		return fmt.Errorf("failed to validate perpetual venue account checkpoint: %w", err)
	}
	return nil
}

// CreateTable creates the checkpoint table only from explicit migration code.
//
// Version:
//   - 2026-10-04: Added.
func (s *PerpetualVenueAccountSnapshotStore) CreateTable(ctx context.Context, executor api.Executor) error {
	if s == nil || s.table == "" || ctx == nil || executor == nil {
		return fmt.Errorf("failed to create perpetual venue account snapshot table: %w", invalid("dependency", "null"))
	}
	definition := strings.Replace(perpetualVenueAccountSnapshotSchema, DefaultPerpetualVenueAccountSnapshotTableName, quoted(s.table), 1)
	// MySQL constraint names are schema-wide; custom test tables need distinct names.
	definition = strings.ReplaceAll(definition, "CONSTRAINT ck_oms_perp_venue_account_snapshot_identity", "")
	definition = strings.ReplaceAll(definition, "CONSTRAINT ck_oms_perp_venue_account_snapshot_state", "")
	if _, err := executor.ExecContext(ctx, definition); err != nil {
		return fmt.Errorf("failed to create perpetual venue account snapshot table: %w", err)
	}
	return nil
}

// Select reads only the exact tenant-owned venue account checkpoint.
//
// Version:
//   - 2026-10-04: Added.
func (s *PerpetualVenueAccountSnapshotStore) Select(ctx context.Context, executor api.Executor, scope PerpetualVenueAccountScope) (*PerpetualVenueAccountSnapshot, error) {
	if s == nil || s.table == "" || ctx == nil || executor == nil {
		return nil, fmt.Errorf("failed to read perpetual venue account snapshot: %w", invalid("dependency", "null"))
	}
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("failed to read perpetual venue account snapshot: %w", err)
	}
	result := &PerpetualVenueAccountSnapshot{PerpetualVenueAccountScope: scope}
	err := executor.QueryRowContext(ctx, "SELECT snapshot_version, revision, observed_through_ms, snapshot, created_at, updated_at FROM "+quoted(s.table)+" WHERE account_id=? AND venue=? AND network=? AND trading_account=?", scope.AccountID, scope.Venue, scope.Network, scope.TradingAccount).Scan(&result.SnapshotVersion, &result.Revision, &result.ObservedThroughMS, &result.Snapshot, &result.CreatedAt, &result.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to read perpetual venue account snapshot: %w", err)
	}
	if err := result.PerpetualVenueAccountCheckpoint.Validate(); err != nil {
		return nil, fmt.Errorf("failed to read perpetual venue account snapshot: %w", err)
	}
	return result, nil
}

// Save atomically publishes state and progress using compare-and-swap revision control.
// expectedRevision=0 inserts an absent row. A stale revision or time regression conflicts.
// The caller must commit before publishing the result and reload on ErrConflict.
//
// Version:
//   - 2026-10-04: Added.
func (s *PerpetualVenueAccountSnapshotStore) Save(ctx context.Context, tx *sql.Tx, scope PerpetualVenueAccountScope, expectedRevision uint64, checkpoint PerpetualVenueAccountCheckpoint) error {
	const op = "failed to save perpetual venue account snapshot"
	if s == nil || s.table == "" || ctx == nil || tx == nil {
		return fmt.Errorf("%s: %w", op, invalid("dependency", "null"))
	}
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if err := checkpoint.Validate(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if expectedRevision == math.MaxUint64 {
		return fmt.Errorf("%s: %w", op, invalid("revision", "out_of_range"))
	}
	if expectedRevision == 0 {
		_, err := tx.ExecContext(ctx, "INSERT INTO "+quoted(s.table)+" (account_id,venue,network,trading_account,snapshot_version,revision,observed_through_ms,snapshot) VALUES (?,?,?,?,?,1,?,?)", scope.AccountID, scope.Venue, scope.Network, scope.TradingAccount, checkpoint.SnapshotVersion, checkpoint.ObservedThroughMS, []byte(checkpoint.Snapshot))
		if err != nil {
			wrapped := writeError(op, err)
			if errors.Is(wrapped, ErrDuplicate) {
				return fmt.Errorf("%s: %w", op, errors.Join(ErrConflict, err))
			}
			return wrapped
		}
		return nil
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+quoted(s.table)+" SET snapshot_version=?, revision=revision+1, observed_through_ms=?, snapshot=?, updated_at=CURRENT_TIMESTAMP(6) WHERE account_id=? AND venue=? AND network=? AND trading_account=? AND revision=? AND observed_through_ms<=?", checkpoint.SnapshotVersion, checkpoint.ObservedThroughMS, []byte(checkpoint.Snapshot), scope.AccountID, scope.Venue, scope.Network, scope.TradingAccount, expectedRevision, checkpoint.ObservedThroughMS)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if n != 1 {
		return fmt.Errorf("%s: %w", op, ErrConflict)
	}
	return nil
}
