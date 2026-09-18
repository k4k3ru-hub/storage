package ammpool

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

//go:embed schema.sql
var schema string

var ErrCursorConflict = errors.New("amm pool cursor conflict")

type Store struct {
	db         *sql.DB
	tableNames *strings.Replacer
}

// NewStore composes a repository with an application-owned connection pool.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func NewStore(db *sql.DB) (*Store, error) {
	return NewStoreWithTablePrefix(db, "")
}

// NewStoreWithTablePrefix composes a repository with validated application table names.
// The prefix is applied to all three tables; an empty prefix preserves default names.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func NewStoreWithTablePrefix(db *sql.DB, prefix string) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("failed to create amm pool store: database=null")
	}
	if prefix != "" && !regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(prefix) {
		return nil, fmt.Errorf("failed to create amm pool store: table_prefix=invalid")
	}
	names := []string{"onchain_amm_pool_new_pair_snapshots", "onchain_amm_pool_new_pair_events", "onchain_amm_pool_new_pair_sync_cursors"}
	replacements := make([]string, 0, len(names)*2)
	for _, name := range names {
		if len(prefix)+len(name) > 64 {
			return nil, fmt.Errorf("failed to create amm pool store: table_name=too_long max_length=64")
		}
		replacements = append(replacements, name, "`"+prefix+name+"`")
	}
	return &Store{db: db, tableNames: strings.NewReplacer(replacements...)}, nil
}

func (s *Store) query(query string) string { return s.tableNames.Replace(query) }

// Schema returns DDL using this store's configured application table names.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func (s *Store) Schema() string { return s.query(schema) }

// Schema returns the version-one DDL for application migrations.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func Schema() string { return schema }

// CreateTables applies initial DDL when explicitly called by a migration runner.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func (s *Store) CreateTables(ctx context.Context) error {
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, s.query(statement)); err != nil {
			return fmt.Errorf("failed to create amm pool tables: %w", err)
		}
	}
	return nil
}

// Cursor reads a durable source position; a missing source starts at revision zero.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func (s *Store) Cursor(ctx context.Context, source Source) (Cursor, error) {
	c := Cursor{Source: source}
	if err := source.Validate(); err != nil {
		return c, fmt.Errorf("failed to read amm pool cursor: %w", err)
	}
	id := source.ID()
	err := s.db.QueryRowContext(ctx, s.query("SELECT position, revision, updated_at FROM onchain_amm_pool_new_pair_sync_cursors WHERE id=?"), id[:]).Scan(&c.Position, &c.Revision, &c.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return c, nil
	}
	if err != nil {
		return c, fmt.Errorf("failed to read amm pool cursor: %w", err)
	}
	return c, nil
}

// Commit atomically applies events, snapshots and an optimistic source checkpoint.
// Cursor.Revision is the previously read revision; the committed revision increases by one.
// Callers must rebuild affected snapshots before submitting reorg corrections.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
//   - 2026-09-18: Save snapshots before events to satisfy snapshot ownership constraints.
func (s *Store) Commit(ctx context.Context, b Batch) (err error) {
	if err = b.Validate(); err != nil {
		return fmt.Errorf("failed to commit amm pool batch: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to commit amm pool batch: %w", err)
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("failed to roll back amm pool batch: %w", e))
		}
	}()
	c := b.Cursor
	sourceID := c.Source.ID()
	_, err = tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_sync_cursors
 (id,chain_family,chain,network,venue,source_key,position,revision,updated_at) VALUES (?,?,?,?,?,?,?,0,?)
 ON DUPLICATE KEY UPDATE id=id`), sourceID[:], c.Source.ChainFamily, c.Source.Chain, c.Source.Network, c.Source.Venue, c.Source.Key, []byte(`{}`), c.UpdatedAt.UTC())
	if err != nil {
		return fmt.Errorf("failed to initialize amm pool cursor: %w", err)
	}
	var revision uint64
	if err = tx.QueryRowContext(ctx, s.query("SELECT revision FROM onchain_amm_pool_new_pair_sync_cursors WHERE id=? FOR UPDATE"), sourceID[:]).Scan(&revision); err != nil {
		return fmt.Errorf("failed to lock amm pool cursor: %w", err)
	}
	if revision != c.Revision {
		return fmt.Errorf("failed to commit amm pool batch: %w", ErrCursorConflict)
	}
	for _, p := range b.Snapshots {
		id := p.Identity.ID()
		i := p.Identity
		_, err = tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_snapshots
  (id,chain_family,chain,network,venue,pool_id,token0_id,token1_id,pool_created_at,first_liquidity_at,first_swap_at,position_kind,first_liquidity_position_number,first_liquidity_position_id,first_swap_position_number,first_swap_position_id,event_scan_from_position,event_scan_through_position,event_scan_through_position_id,confirmed_at,liquidity_usd,state,revision,is_canonical,updated_at)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?) ON DUPLICATE KEY UPDATE
  token0_id=VALUES(token0_id),token1_id=VALUES(token1_id),pool_created_at=VALUES(pool_created_at),
  first_liquidity_at=VALUES(first_liquidity_at),first_swap_at=VALUES(first_swap_at),liquidity_usd=VALUES(liquidity_usd),
  position_kind=VALUES(position_kind),first_liquidity_position_number=VALUES(first_liquidity_position_number),first_liquidity_position_id=VALUES(first_liquidity_position_id),first_swap_position_number=VALUES(first_swap_position_number),first_swap_position_id=VALUES(first_swap_position_id),event_scan_from_position=VALUES(event_scan_from_position),event_scan_through_position=VALUES(event_scan_through_position),event_scan_through_position_id=VALUES(event_scan_through_position_id),confirmed_at=VALUES(confirmed_at),
  state=VALUES(state),revision=revision+1,is_canonical=VALUES(is_canonical),updated_at=VALUES(updated_at)`),
			id[:], i.ChainFamily, i.Chain, i.Network, i.Venue, i.PoolID, p.Token0ID, p.Token1ID, p.CreatedAt.UTC(), utc(p.FirstLiquidityAt), utc(p.FirstSwapAt), p.PositionKind, p.FirstLiquidityPositionNumber, p.FirstLiquidityPositionID, p.FirstSwapPositionNumber, p.FirstSwapPositionID, p.EventScanFromPosition, p.EventScanThroughPosition, p.EventScanThroughPositionID, utc(p.ConfirmedAt), p.LiquidityUSD, []byte(p.State), p.Canonical, p.UpdatedAt.UTC())
		if err != nil {
			return fmt.Errorf("failed to save amm pool snapshot: %w", err)
		}
	}
	for _, e := range b.Events {
		id, pid := e.ID(), e.Pool.ID()
		_, err = tx.ExecContext(ctx, s.query(`INSERT INTO onchain_amm_pool_new_pair_events
	  (id,pool_id,source_id,position_number,position_id,transaction_id,event_index,event_type,occurred_at,observed_at,payload,is_canonical)
	  VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE is_canonical=VALUES(is_canonical)`),
			id[:], pid[:], sourceID[:], e.PositionNumber, e.PositionID, e.TransactionID, e.Index, e.Type, e.OccurredAt.UTC(), e.ObservedAt.UTC(), []byte(e.Payload), e.Canonical)
		if err != nil {
			return fmt.Errorf("failed to save amm pool event: %w", err)
		}
	}
	_, err = tx.ExecContext(ctx, s.query("UPDATE onchain_amm_pool_new_pair_sync_cursors SET position=?,revision=revision+1,updated_at=? WHERE id=?"), []byte(c.Position), c.UpdatedAt.UTC(), sourceID[:])
	if err != nil {
		return fmt.Errorf("failed to save amm pool cursor: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit amm pool batch: %w", err)
	}
	return nil
}

func utc(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

const snapshotColumns = `chain_family,chain,network,venue,pool_id,token0_id,token1_id,pool_created_at,first_liquidity_at,first_swap_at,position_kind,first_liquidity_position_number,first_liquidity_position_id,first_swap_position_number,first_swap_position_id,event_scan_from_position,event_scan_through_position,event_scan_through_position_id,confirmed_at,liquidity_usd,state,revision,is_canonical,updated_at`

type scanner interface{ Scan(...any) error }

func scanSnapshot(row scanner) (Snapshot, error) {
	var p Snapshot
	err := row.Scan(&p.Identity.ChainFamily, &p.Identity.Chain, &p.Identity.Network, &p.Identity.Venue, &p.Identity.PoolID, &p.Token0ID, &p.Token1ID, &p.CreatedAt, &p.FirstLiquidityAt, &p.FirstSwapAt, &p.PositionKind, &p.FirstLiquidityPositionNumber, &p.FirstLiquidityPositionID, &p.FirstSwapPositionNumber, &p.FirstSwapPositionID, &p.EventScanFromPosition, &p.EventScanThroughPosition, &p.EventScanThroughPositionID, &p.ConfirmedAt, &p.LiquidityUSD, &p.State, &p.Revision, &p.Canonical, &p.UpdatedAt)
	return p, err
}

// Get returns a canonical pool snapshot, preserving sql.ErrNoRows when unavailable.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func (s *Store) Get(ctx context.Context, i Identity) (Snapshot, error) {
	if err := i.Validate(); err != nil {
		return Snapshot{}, fmt.Errorf("failed to get amm pool snapshot: %w", err)
	}
	id := i.ID()
	p, err := scanSnapshot(s.db.QueryRowContext(ctx, s.query("SELECT "+snapshotColumns+" FROM onchain_amm_pool_new_pair_snapshots WHERE id=? AND is_canonical=1"), id[:]))
	if err != nil {
		return p, fmt.Errorf("failed to get amm pool snapshot: %w", err)
	}
	return p, nil
}

// Load returns canonical snapshots whose lifecycle anchor is retained, in deterministic order.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
//   - 2026-09-18: Select by first liquidity time when available.
func (s *Store) Load(ctx context.Context, since time.Time) (result []Snapshot, err error) {
	rows, err := s.db.QueryContext(ctx, s.query("SELECT "+snapshotColumns+" FROM onchain_amm_pool_new_pair_snapshots WHERE is_canonical=1 AND COALESCE(first_liquidity_at,pool_created_at)>=? ORDER BY COALESCE(first_liquidity_at,pool_created_at),id"), since.UTC())
	if err != nil {
		return nil, fmt.Errorf("failed to load amm pool snapshots: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close amm pool snapshots: %w", e))
		}
	}()
	result = []Snapshot{}
	for rows.Next() {
		p, e := scanSnapshot(rows)
		if e != nil {
			return nil, fmt.Errorf("failed to decode amm pool snapshot: %w", e)
		}
		result = append(result, p)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to load amm pool snapshots: %w", err)
	}
	return result, nil
}

// Prune deletes expired history and lifecycle snapshots in bounded batches; source cursors remain.
// Deleting a snapshot cascades to its event history.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
//   - 2026-09-18: Prune snapshots by their lifecycle anchor.
func (s *Store) Prune(ctx context.Context, eventBefore, snapshotBefore time.Time) error {
	if eventBefore.IsZero() || snapshotBefore.IsZero() {
		return fmt.Errorf("failed to prune amm pool history: retention=invalid")
	}
	for _, q := range []struct {
		sql    string
		before time.Time
	}{
		{"DELETE FROM onchain_amm_pool_new_pair_events WHERE observed_at<? LIMIT 1000", eventBefore},
		{"DELETE FROM onchain_amm_pool_new_pair_snapshots WHERE COALESCE(first_liquidity_at,pool_created_at)<? LIMIT 1000", snapshotBefore},
	} {
		for batch := 0; batch < 100; batch++ {
			result, err := s.db.ExecContext(ctx, s.query(q.sql), q.before.UTC())
			if err != nil {
				return fmt.Errorf("failed to prune amm pool history: %w", err)
			}
			count, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to read pruned row count: %w", err)
			}
			if count < 1000 {
				break
			}
		}
	}
	return nil
}

// Events loads canonical events of unconfirmed lifecycle-retained pools for one source.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Use NewPair tables and durable verification state.
func (s *Store) Events(ctx context.Context, source Source, since time.Time) (out []Event, err error) {
	if err = source.Validate(); err != nil {
		return nil, fmt.Errorf("failed to load amm pool events: %w", err)
	}
	id := source.ID()
	rows, err := s.db.QueryContext(ctx, s.query(`SELECT p.chain_family,p.chain,p.network,p.venue,p.pool_id,e.position_number,e.position_id,e.transaction_id,e.event_index,e.event_type,e.occurred_at,e.observed_at,e.payload,e.is_canonical
 FROM onchain_amm_pool_new_pair_events e JOIN onchain_amm_pool_new_pair_snapshots p ON p.id=e.pool_id
 WHERE e.source_id=? AND e.is_canonical=1 AND p.is_canonical=1 AND p.confirmed_at IS NULL AND COALESCE(p.first_liquidity_at,p.pool_created_at)>=? ORDER BY e.position_number,e.transaction_id,e.event_index`), id[:], since.UTC())
	if err != nil {
		return nil, fmt.Errorf("failed to load amm pool events: %w", err)
	}
	defer func() {
		if e := rows.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("failed to close amm pool events: %w", e))
		}
	}()
	for rows.Next() {
		e := Event{Source: source}
		if err = rows.Scan(&e.Pool.ChainFamily, &e.Pool.Chain, &e.Pool.Network, &e.Pool.Venue, &e.Pool.PoolID, &e.PositionNumber, &e.PositionID, &e.TransactionID, &e.Index, &e.Type, &e.OccurredAt, &e.ObservedAt, &e.Payload, &e.Canonical); err != nil {
			return nil, fmt.Errorf("failed to decode amm pool event: %w", err)
		}
		out = append(out, e)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to load amm pool events: %w", err)
	}
	return out, nil
}
