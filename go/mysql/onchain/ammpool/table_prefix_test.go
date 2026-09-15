package ammpool

import (
	"database/sql"
	"strings"
	"testing"
)

// TestTablePrefix verifies application-specific names, default compatibility and injection rejection.
//
// Version:
//   - 2026-09-16: Added.
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
	for _, name := range []string{"onchain_amm_pool_snapshots", "onchain_amm_pool_events", "onchain_amm_pool_sync_cursors"} {
		if !strings.Contains(prefixed.Schema(), "`market_hub_"+name+"`") {
			t.Fatal("configured DDL omitted table", name)
		}
		if !strings.Contains(original.Schema(), "`"+name+"`") {
			t.Fatal("default table changed", name)
		}
	}
	query := prefixed.query("SELECT * FROM onchain_amm_pool_events e JOIN onchain_amm_pool_snapshots p ON p.id=e.pool_id")
	if !strings.Contains(query, "`market_hub_onchain_amm_pool_events`") || !strings.Contains(query, "`market_hub_onchain_amm_pool_snapshots`") {
		t.Fatal(query)
	}
	for _, prefix := range []string{"bad-prefix", "x`;DROP TABLE x;--", "a.b", " ", strings.Repeat("a", 64)} {
		if _, err := NewStoreWithTablePrefix(db, prefix); err == nil {
			t.Fatal("invalid prefix accepted")
		}
	}
}
