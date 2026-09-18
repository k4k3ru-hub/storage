//go:build mysqlintegration

package ammpool

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// TestMySQLObservationLifecycle exercises persistence against an isolated real MySQL database.
//
// Version:
//   - 2026-09-19: Added.
func TestMySQLObservationLifecycle(t *testing.T) {
	dsn := os.Getenv("AMMPOOL_TEST_DSN")
	if dsn == "" {
		t.Skip("AMMPOOL_TEST_DSN is not configured")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal("failed to configure test connector")
	}
	admin := sql.OpenDB(connector)
	defer func() {
		if err := admin.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := fmt.Sprintf("newpair_test_%x", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"`"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP DATABASE `"+name+"`"); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = name
	connector, err = mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal("failed to configure isolated connector")
	}
	db := sql.OpenDB(connector)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	store, err := NewStoreWithTablePrefix(db, "market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTables(ctx); err != nil {
		t.Fatal("idempotent DDL", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	cutoff := now.Add(-24 * time.Hour)
	source := Source{"evm", "base", "mainnet", "uniswap-v3", "factory"}
	cursor := Cursor{Source: source, Position: json.RawMessage(`{}`), UpdatedAt: now}
	kind, id, usd := "block", "block-hash", "2000.000000000000000000"
	position := uint64(0)
	observed := now.Add(-time.Minute)
	makeSnapshot := func(pool string, created time.Time) Snapshot {
		return Snapshot{
			Identity: Identity{"evm", "base", "mainnet", "uniswap-v3", pool}, Token0ID: "token0", Token1ID: "token1",
			CreatedAt: created, SwapObservedAt: &observed, LiquidityUSD: &usd, LiquidityEvaluatedAt: &observed,
			Verification: Verification{PositionKind: &kind, SwapObservedPositionNumber: &position, SwapObservedPositionID: &id},
			State:        json.RawMessage(`{}`), Canonical: true, UpdatedAt: now,
		}
	}
	old := makeSnapshot("old", cutoff.Add(-time.Microsecond))
	boundary := makeSnapshot("boundary", cutoff)
	confirmed := makeSnapshot("confirmed", now.Add(-time.Hour))
	confirmed.ConfirmedAt = &now
	unknown := makeSnapshot("unknown", now)
	unknown.Verification = Verification{}
	unknown.SwapObservedAt = nil
	unknown.LiquidityUSD = nil
	unknown.LiquidityEvaluatedAt = nil
	snapshots := []Snapshot{old, boundary, confirmed, unknown}
	events := []Event{}
	for _, p := range snapshots[:3] {
		events = append(events, Event{Pool: p.Identity, Source: source, PositionID: id, TransactionID: "tx", Index: "0", Type: "swap", OccurredAt: observed, ObservedAt: now, Payload: json.RawMessage(`{}`), Canonical: true})
	}
	batch := Batch{Cursor: cursor, Snapshots: snapshots, Events: events}
	if err := store.Commit(ctx, batch); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, confirmed.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConfirmedAt == nil || !got.ConfirmedAt.Equal(now) || got.SwapObservedAt == nil || !got.SwapObservedAt.Equal(observed) || got.SwapObservedPositionNumber == nil || *got.SwapObservedPositionNumber != 0 || got.LiquidityUSD == nil || *got.LiquidityUSD != usd || got.LiquidityEvaluatedAt == nil || !got.LiquidityEvaluatedAt.Equal(observed) {
		t.Fatalf("evidence round trip: %+v", got)
	}
	loaded, err := store.Load(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 3 || loaded[0].Identity.PoolID != "boundary" || loaded[1].Identity.PoolID != "confirmed" || loaded[2].Identity.PoolID != "unknown" {
		t.Fatalf("creation cutoff/order: %+v", loaded)
	}
	loadedEvents, err := store.Events(ctx, source, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if len(loadedEvents) != 1 || loadedEvents[0].Pool.PoolID != "boundary" {
		t.Fatalf("event candidate selection: %+v", loadedEvents)
	}
	if err := store.Commit(ctx, batch); !errors.Is(err, ErrCursorConflict) {
		t.Fatalf("stale cursor error: %v", err)
	}
	cursor, err = store.Cursor(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	cleared := confirmed
	cleared.Verification = Verification{}
	cleared.SwapObservedAt = nil
	cleared.LiquidityUSD = nil
	cleared.LiquidityEvaluatedAt = nil
	if err := store.Commit(ctx, Batch{Cursor: cursor, Snapshots: []Snapshot{cleared}}); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(ctx, confirmed.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if got.SwapObservedAt != nil || got.PositionKind != nil || got.SwapObservedPositionNumber != nil || got.SwapObservedPositionID != nil || got.ConfirmedAt != nil || got.LiquidityUSD != nil || got.LiquidityEvaluatedAt != nil || got.Revision != 2 {
		t.Fatalf("nullable update: %+v", got)
	}
	if err := store.Prune(ctx, now.Add(-48*time.Hour), cutoff); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, old.Identity); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("old pool retained by recent observation: %v", err)
	}
	if _, err := store.Get(ctx, boundary.Identity); err != nil {
		t.Fatal("boundary removed", err)
	}
	poolID := old.Identity.ID()
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM market_hub_onchain_amm_pool_new_pair_events WHERE pool_id=?", poolID[:]).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("cascade left orphan events")
	}
	cursor, err = store.Cursor(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Revision != 2 {
		t.Fatal("prune removed source cursor")
	}
}
