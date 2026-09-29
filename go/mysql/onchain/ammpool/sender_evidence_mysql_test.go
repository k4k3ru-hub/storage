//go:build mysqlintegration

package ammpool

import "testing"

// TestMySQLSenderEphemeralEvidence validates atomic sender proof without growing the raw archive.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLSenderEphemeralEvidence(t *testing.T) {
	s, db := senderMySQL(t)
	b := senderFixture()
	b.SenderEvidence = b.Events
	b.Events = nil
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(t.Context(), s.query("SELECT COUNT(*) FROM onchain_amm_pool_new_pair_events")).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("ephemeral proof was archived", count)
	}
	_, _, events := readSenderState(t, s)
	if len(events) != 1 {
		t.Fatal("missing sender occurrence")
	}
	b = senderCorrection(b)
	b.Cursor.Revision = 1
	b.SenderEvents[0].Canonical = false
	if err := s.Commit(t.Context(), b); err == nil {
		t.Fatal("contradictory proof accepted")
	}
	b.SenderEvidence[0].Canonical = false
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	_, _, events = readSenderState(t, s)
	if events[0].Canonical {
		t.Fatal("ephemeral cancellation lost")
	}
	b.Cursor.Revision = 2
	b.Events = append([]Event(nil), b.SenderEvidence...)
	b.Events[0].Canonical = true
	if err := s.Commit(t.Context(), b); err == nil {
		t.Fatal("contradictory archive and ephemeral proof accepted")
	}
	if err := db.QueryRowContext(t.Context(), s.query("SELECT COUNT(*) FROM onchain_amm_pool_new_pair_events")).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("conflicting proof did not roll back")
	}
}
