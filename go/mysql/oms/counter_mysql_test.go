package oms

import (
	"database/sql"
	"errors"
	"testing"
)

// TestMySQLCounterSnapshots verifies routed totals, idempotency, reversal and rollback.
//
// Version:
//   - 2026-09-27: Added.
func TestMySQLCounterSnapshots(t *testing.T) {
	s, db := mysqlStore(t)
	insertFixture(t, s, db, counterOrder())
	a := acceptance(1, 1)
	got := appendFixture(t, s, db, a)
	if !sameString(got.State.FilledCounterQuantity, ptr("0")) || !sameString(got.Execution.FilledCounterQuantity, ptr("0")) {
		t.Fatal("missing initial zero")
	}
	b := acceptance(2, 1)
	b.Event.RecordKey = []byte("accept-b")
	b.Event.ExecutionID = ptr("exec_two")
	b.Event.Venue = ptr("another-venue")
	appendFixture(t, s, db, b)
	r := fact(3, 1, 1, EventTypeSucceeded, "a-success")
	appendFixture(t, s, db, r)
	f := fill(4, 1, 1, "a-fill", "60")
	f.Event.OrderCounterQuantity = ptr("1.25")
	appendFixture(t, s, db, f)
	g := fill(5, 1, 2, "b-fill", "40")
	g.Event.ExecutionID = ptr("exec_two")
	g.Event.OrderCounterQuantity = ptr("2.75")
	got = appendFixture(t, s, db, g)
	if !sameString(got.State.FilledCounterQuantity, ptr("4")) || !sameString(got.Execution.FilledCounterQuantity, ptr("2.75")) {
		t.Fatal("routed totals incorrect")
	}
	if !appendFixture(t, s, db, g).Duplicate {
		t.Fatal("duplicate changed counter")
	}
	changed := g
	changed.Event.OrderCounterQuantity = ptr("2.8")
	err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, changed); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("changed duplicate accepted", err)
	}
	c := fill(6, 1, 1, "a-correct", "60")
	c.Event.EventType = EventTypeFillCorrected
	c.Event.ReferenceEventID = &f.Event.ID
	c.Onchain.EventPosition = f.Onchain.EventPosition
	c.Event.OrderCounterQuantity = ptr("0.75")
	got = appendFixture(t, s, db, c)
	if !sameString(got.State.FilledCounterQuantity, ptr("3.5")) {
		t.Fatal("correction not applied")
	}
	reversed := fact(7, 1, 1, EventTypeReversed, "a-reversed")
	reversed.Event.ReferenceEventID = &r.Event.ID
	got = appendFixture(t, s, db, reversed)
	if !sameString(got.State.FilledCounterQuantity, ptr("2.75")) || !sameString(got.Execution.FilledCounterQuantity, ptr("0")) {
		t.Fatal("reversed contribution remained")
	}
	row, err := s.SelectOrder(t.Context(), db, 1, 1)
	must(t, err)
	if !sameString(row.FilledCounterQuantity, got.State.FilledCounterQuantity) {
		t.Fatal("order counter not stored")
	}
	events, err := s.ListOnchainEvents(t.Context(), db, 1, 1, 0, 200)
	must(t, err)
	if !sameString(events[4].Event.OrderCounterQuantity, ptr("2.75")) {
		t.Fatal("event counter not stored")
	}
}
