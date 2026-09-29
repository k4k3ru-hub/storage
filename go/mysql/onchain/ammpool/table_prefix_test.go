package ammpool

import (
	"database/sql"
	"strings"
	"testing"
)

// TestTablePrefix verifies application-specific NewPair names and injection rejection.
//
// Version:
//   - 2026-09-16: Added.
//   - 2026-09-18: Verify NewPair-specific table names.
//   - 2026-09-27: Include the activity minute table.
//   - 2026-09-28: Include sender tables and generated constraint bounds.
func TestTablePrefix(t *testing.T) {
	db := new(sql.DB)
	prefixed, err := NewStoreWithTablePrefix(db, "market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	original, err := NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"onchain_amm_pool_new_pair_snapshots", "onchain_amm_pool_new_pair_events", "onchain_amm_pool_new_pair_sync_cursors", "onchain_amm_pool_new_pair_activity_minutes", "onchain_amm_pool_new_pair_sender_snapshots", "onchain_amm_pool_new_pair_sender_transactions", "onchain_amm_pool_new_pair_sender_events"} {
		if !strings.Contains(prefixed.Schema(), "`market_hub_"+name+"`") {
			t.Fatal("configured DDL omitted table", name)
		}
		if !strings.Contains(original.Schema(), "`"+name+"`") {
			t.Fatal("default table changed", name)
		}
	}
	query := prefixed.query("SELECT * FROM onchain_amm_pool_new_pair_events e JOIN onchain_amm_pool_new_pair_snapshots p ON p.id=e.pool_id")
	if !strings.Contains(query, "`market_hub_onchain_amm_pool_new_pair_events`") || !strings.Contains(query, "`market_hub_onchain_amm_pool_new_pair_snapshots`") {
		t.Fatal(query)
	}
	for _, prefix := range []string{"bad-prefix", "x`;DROP TABLE x;--", "a.b", " ", strings.Repeat("a", 64)} {
		if _, err := NewStoreWithTablePrefix(db, prefix); err == nil {
			t.Fatal("invalid prefix accepted")
		}
	}
	maxPrefix := 64 - len("onchain_amm_pool_new_pair_sender_transactions") - len("_ibfk_1")
	if _, err := NewStoreWithTablePrefix(db, strings.Repeat("a", maxPrefix)); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStoreWithTablePrefix(db, strings.Repeat("a", maxPrefix+1)); err == nil {
		t.Fatal("oversized generated constraint accepted")
	}
}

// TestSchemaUsesSnapshotOwnership verifies that deleting a snapshot deletes its event history.
//
// Version:
//   - 2026-09-18: Added.
func TestSchemaUsesSnapshotOwnership(t *testing.T) {
	if !strings.Contains(schema, "FOREIGN KEY (pool_id) REFERENCES onchain_amm_pool_new_pair_snapshots(id) ON DELETE CASCADE") {
		t.Fatal("event ownership cascade missing")
	}
	if strings.Contains(schema, "ON UPDATE CASCADE") {
		t.Fatal("identity update cascade must not be enabled")
	}
}
