//go:build mysqlintegration

package ammpool

import (
	"encoding/json"
	"testing"
	"time"
)

// TestMySQLSenderInvalidationAtomicity verifies unavailable-state writes commit or roll back with Activity.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLSenderInvalidationAtomicity(t *testing.T) {
	s, db := senderMySQL(t)
	initial := senderFixture()
	if err := s.Commit(t.Context(), initial); err != nil {
		t.Fatal(err)
	}
	now := initial.Cursor.UpdatedAt.Add(time.Minute)
	b := Batch{Cursor: initial.Cursor, Snapshots: initial.Snapshots, ActivityMinutes: initial.ActivityMinutes}
	b.Cursor.Revision = 1
	b.Cursor.UpdatedAt = now
	b.Snapshots[0].State = json.RawMessage(`{"v":2}`)
	b.Snapshots[0].UpdatedAt = now
	b.ActivityMinutes[0].Totals = json.RawMessage(`{"swapCount":"0"}`)
	b.ActivityMinutes[0].UpdatedAt = now
	b.SenderInvalidations = []SenderInvalidation{{Pool: b.Snapshots[0].Identity, From: now.Add(-15 * time.Minute), To: now.Add(time.Minute), UpdatedAt: now}}
	// Fail the final cursor advance, after parent, minute and invalidation writes.
	if _, err := db.ExecContext(t.Context(), `CREATE TRIGGER reject_sender_commit BEFORE UPDATE ON market_hub_onchain_amm_pool_new_pair_sync_cursors
 FOR EACH ROW BEGIN IF NEW.revision <> OLD.revision THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected failure'; END IF; END`); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(t.Context(), b); err == nil {
		t.Fatal("failed transaction committed")
	}
	pools, _, _ := readSenderState(t, s)
	p, err := s.Get(t.Context(), b.Snapshots[0].Identity)
	if err != nil || p.Revision != 1 || pools[0].InvalidFrom != nil {
		t.Fatal("partial invalidation or Activity commit", err)
	}
	minutes, err := s.ActivityMinutes(t.Context(), p.Identity, initial.ActivityMinutes[0].Start, now)
	if err != nil || len(minutes) != 1 {
		t.Fatal("Activity minute read failed", err)
	}
	var totals map[string]string
	if err := json.Unmarshal(minutes[0].Totals, &totals); err != nil || totals["swapCount"] != "1" {
		t.Fatal("Activity minute was not rolled back", err)
	}
	if _, err := db.ExecContext(t.Context(), "DROP TRIGGER reject_sender_commit"); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	pools, _, events := readSenderState(t, s)
	if len(events) != 1 || pools[0].InvalidFrom == nil || !pools[0].InvalidFrom.Equal(b.SenderInvalidations[0].From) || !pools[0].InvalidTo.Equal(b.SenderInvalidations[0].To) {
		t.Fatal("unavailable interval not persisted")
	}
	// Repeated failure intervals may expand, but may never clear earlier invalid minutes.
	b.Cursor.Revision++
	b.SenderInvalidations[0].From = now
	b.SenderInvalidations[0].To = now.Add(2 * time.Minute)
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	pools, _, _ = readSenderState(t, s)
	if !pools[0].InvalidFrom.Equal(now.Add(-15*time.Minute)) || !pools[0].InvalidTo.Equal(now.Add(2*time.Minute)) {
		t.Fatal("invalidation interval did not expand")
	}
	// A parent with no prior sender evidence stays absent, rather than becoming a known zero.
	b.Cursor.Revision++
	b.Snapshots[0].Identity.PoolID = "unobserved"
	b.ActivityMinutes = nil
	b.SenderInvalidations[0].Pool = b.Snapshots[0].Identity
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	pools, _, _ = readSenderState(t, s)
	if len(pools) != 1 {
		t.Fatal("invalidation invented empty sender state")
	}
}
