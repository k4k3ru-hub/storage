//go:build mysqlintegration

package ammpool

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"os"
	"testing"
	"time"
)

// TestMySQLActivityAtomicity verifies exact updates, cursor rollback, generation reset and cascade cleanup.
//
// Version:
//   - 2026-09-27: Added.
func TestMySQLActivityAtomicity(t *testing.T) {
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
		t.Fatal("invalid test connector")
	}
	admin := sql.OpenDB(connector)
	defer func() {
		if err := admin.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	name := fmt.Sprintf("activity_test_%x", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"`"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(c, "DROP DATABASE `"+name+"`"); err != nil {
			t.Error(err)
		}
	}()
	cfg.DBName = name
	connector, err = mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal("invalid test connector")
	}
	db := sql.OpenDB(connector)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	s, err := NewStoreWithTablePrefix(db, "market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Minute)
	p := Snapshot{Identity: Identity{"evm", "base", "mainnet", "uniswap-v4", "pool"}, Token0ID: "a", Token1ID: "b", CreatedAt: now, UpdatedAt: now, Canonical: true, State: json.RawMessage(`{"v":1,"source":"factory"}`)}
	m := ActivityMinute{Pool: p.Identity, Start: now, UpdatedAt: now, Totals: json.RawMessage(`{"count":"9007199254740993"}`)}
	b := Batch{Cursor: Cursor{Source: Source{"evm", "base", "mainnet", "uniswap-v4", "factory"}, Position: json.RawMessage(`{}`), UpdatedAt: now}, Snapshots: []Snapshot{p}, ActivityMinutes: []ActivityMinute{m}}
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, b); !errors.Is(err, ErrCursorConflict) {
		t.Fatal("stale cursor accepted", err)
	}
	rows, err := s.ActivityMinutes(ctx, p.Identity, now, now.Add(time.Minute))
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	var totals map[string]string
	if err := json.Unmarshal(rows[0].Totals, &totals); err != nil || totals["count"] != "9007199254740993" {
		t.Fatal(totals, err)
	}
	walked := 0
	if err := s.WalkActivityMinutes(ctx, b.Cursor.Source, now, now.Add(time.Minute), func(row ActivityMinute) error {
		walked++
		if row.Pool != p.Identity {
			return fmt.Errorf("failed to verify activity identity")
		}
		return nil
	}); err != nil || walked != 1 {
		t.Fatal("source restore failed", walked, err)
	}
	other := b.Cursor.Source
	other.Key = "other"
	if err := s.WalkActivityMinutes(ctx, other, now, now.Add(time.Minute), func(row ActivityMinute) error { return fmt.Errorf("failed to isolate source") }); err != nil {
		t.Fatal(err)
	}
	b.Cursor.Revision = 1
	b.Snapshots[0].State = json.RawMessage(`{"v":2,"source":"factory"}`)
	b.ActivityMinutes[0].UpdatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := s.Commit(ctx, b); err == nil {
		t.Fatal("invalid SQL timestamp accepted")
	}
	saved, err := s.Get(ctx, p.Identity)
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		V int `json:"v"`
	}
	if err := json.Unmarshal(saved.State, &state); err != nil || state.V != 1 {
		t.Fatal("snapshot not rolled back", state, err)
	}
	b.ActivityMinutes[0].UpdatedAt = now
	b.ActivityMinutes[0].Totals = json.RawMessage(`{"count":"0"}`)
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	rows, err = s.ActivityMinutes(ctx, p.Identity, now, now.Add(time.Minute))
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if err := json.Unmarshal(rows[0].Totals, &totals); err != nil || totals["count"] != "0" {
		t.Fatal("correction was added instead of replaced", totals, err)
	}
	b.Cursor.Revision = 2
	b.ResetActivity = []Identity{p.Identity}
	b.ActivityMinutes = nil
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	rows, err = s.ActivityMinutes(ctx, p.Identity, now, now.Add(time.Minute))
	if err != nil || len(rows) != 0 {
		t.Fatal("reset retained old minutes", rows, err)
	}
	b.Cursor.Revision = 3
	b.ResetActivity = nil
	b.ActivityMinutes = []ActivityMinute{m}
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.Prune(ctx, now.Add(time.Minute), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	rows, err = s.ActivityMinutes(ctx, p.Identity, now, now.Add(time.Minute))
	if err != nil || len(rows) != 0 {
		t.Fatal("cascade retained orphan minutes", rows, err)
	}
}
