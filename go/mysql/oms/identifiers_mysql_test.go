package oms

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
)

// TestMySQLPositionOrderMembership verifies group roots, ownership, legacy NULLs and paginated reads.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPositionOrderMembership(t *testing.T) {
	s, db := mysqlStore(t)
	root := testOrder()
	root.PositionOrderID = &root.ID
	insertFixture(t, s, db, root)
	legacy := testOrder()
	legacy.ID, legacy.IdempotencyKey = 2, []byte("legacy")
	legacy.ParentOrderID = &root.ID
	insertFixture(t, s, db, legacy)
	closeOrder := testOrder()
	closeOrder.ID, closeOrder.IdempotencyKey = 3, []byte("close")
	closeOrder.Side, closeOrder.PositionOrderID = "sell", &root.ID
	insertFixture(t, s, db, closeOrder)
	other := testOrder()
	other.ID, other.AccountID, other.IdempotencyKey = 10, 2, []byte("other-account")
	other.PositionOrderID = &other.ID
	insertFixture(t, s, db, other)

	got, err := s.SelectOrderByIdempotencyKey(t.Context(), db, 1, root.IdempotencyKey)
	must(t, err)
	if got.PositionOrderID == nil || *got.PositionOrderID != root.ID {
		t.Fatal("root reference was not persisted")
	}
	got, err = s.SelectOrder(t.Context(), db, 1, legacy.ID)
	must(t, err)
	if got.PositionOrderID != nil || !sameUint(got.ParentOrderID, legacy.ParentOrderID) {
		t.Fatal("legacy parent or NULL group reference changed")
	}
	page, err := s.ListPositionOrders(t.Context(), db, 1, root.ID, 0, 1)
	must(t, err)
	if len(page) != 1 || page[0].ID != closeOrder.ID {
		t.Fatal("first group page omitted close order")
	}
	page, err = s.ListPositionOrders(t.Context(), db, 1, root.ID, page[0].ID, 1)
	must(t, err)
	if len(page) != 1 || page[0].ID != root.ID {
		t.Fatal("group page omitted representative or included unrelated orders")
	}
	page, err = s.ListPositionOrders(t.Context(), db, 1, root.ID, page[0].ID, 1)
	must(t, err)
	if len(page) != 0 {
		t.Fatal("cursor repeated representative")
	}
	if _, err = s.ListPositionOrders(t.Context(), db, 2, root.ID, 0, 20); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("group leaked across accounts")
	}
	if _, err = s.ListPositionOrders(t.Context(), db, 1, closeOrder.ID, 0, 20); !errors.Is(err, ErrConflict) {
		t.Fatal("child treated as representative")
	}
	if _, err = s.ListPositionOrders(t.Context(), db, 1, root.ID, 0, 0); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("invalid limit accepted")
	}
	tests := []struct {
		name   string
		change func(*Order)
		want   error
	}{
		{"cross_account", func(o *Order) { o.PositionOrderID = &other.ID }, sql.ErrNoRows},
		{"missing", func(o *Order) { o.PositionOrderID = ptr(uint64(99)) }, sql.ErrNoRows},
		{"non_root", func(o *Order) { o.PositionOrderID = &closeOrder.ID }, ErrConflict},
		{"unlinked", func(o *Order) { o.PositionOrderID = &legacy.ID }, ErrConflict},
		{"wallet", func(o *Order) { o.AccountRef = "other-wallet" }, ErrConflict},
		{"asset_class", func(o *Order) { o.AssetClass = "forex" }, ErrConflict},
		{"domain", func(o *Order) { o.Domain = "perpetual" }, ErrConflict},
		{"symbol", func(o *Order) { o.Symbol = "ETH/USDC" }, ErrConflict},
		{"zero", func(o *Order) { o.PositionOrderID = ptr(uint64(0)) }, ErrInvalidParameter},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := closeOrder
			o.ID, o.IdempotencyKey = uint64(100+i), []byte(fmt.Sprintf("invalid-%d", i))
			tt.change(&o)
			err := withTx(t.Context(), db, func(tx *sql.Tx) error {
				_, err := s.InsertOrder(t.Context(), tx, o)
				return err
			})
			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
			if _, err = s.SelectOrder(t.Context(), db, o.AccountID, o.ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("rejected group member persisted")
			}
		})
	}
	appendFixture(t, s, db, acceptance(1, 1))
	appendFixture(t, s, db, fill(2, 1, 1, "full-open", "100"))
	got, err = s.SelectOrder(t.Context(), db, 1, root.ID)
	must(t, err)
	if got.Status != OrderStatusFilled || got.FilledQuantity != "100" || !sameUint(got.PositionOrderID, &root.ID) {
		t.Fatal("group changed order state semantics")
	}
}

func withIdentifierFixture(t *testing.T, record OnchainEvent, ids ExecutionIdentifiers) OnchainEvent {
	t.Helper()
	evidence, err := record.Onchain.WithExecutionIdentifiers(ids)
	must(t, err)
	record.Onchain = &evidence
	return record
}

// TestMySQLExecutionIdentifiers verifies durable evidence, rollback, replay, idempotency and ownership.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLExecutionIdentifiers(t *testing.T) {
	s, db := mysqlStore(t)
	insertFixture(t, s, db, testOrder())
	accepted := withIdentifierFixture(t, acceptance(1, 1), ExecutionIdentifiers{ClientOrderID: ptr("client-A")})
	a := appendFixture(t, s, db, accepted)
	if a.Execution.VenueOrderID != nil || a.Execution.ClientOrderID == nil || *a.Execution.ClientOrderID != "client-A" {
		t.Fatal("acceptance did not persist optional client ID")
	}
	if !appendFixture(t, s, db, accepted).Duplicate {
		t.Fatal("acceptance retry was not idempotent")
	}
	ack := withIdentifierFixture(t, fact(2, 1, 1, EventTypeSubmitted, "ack"), ExecutionIdentifiers{VenueOrderID: ptr("Venue-A")})
	abort := errors.New("test abort")
	err := withTx(t.Context(), db, func(tx *sql.Tx) error {
		if _, err := s.AppendOnchainEvent(t.Context(), tx, 1, ack); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	stored, err := s.SelectExecution(t.Context(), db, 1, 1, a.ExecutionRecordID)
	must(t, err)
	if stored.VenueOrderID != nil || !sameString(stored.ClientOrderID, a.Execution.ClientOrderID) {
		t.Fatal("rolled-back identifier affected snapshot")
	}
	if _, err = s.SelectEventByKey(t.Context(), db, 1, 1, ack.Event.RecordKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("rolled-back identifier evidence persisted")
	}
	appendFixture(t, s, db, ack)
	appendFixture(t, s, db, fact(3, 1, 1, EventTypeSucceeded, "result"))
	filled := appendFixture(t, s, db, fill(4, 1, 1, "partial", "40"))
	if *filled.Execution.VenueOrderID != "Venue-A" || *filled.Execution.ClientOrderID != "client-A" || filled.Execution.ExecutionID != "exec_one" {
		t.Fatal("later fill lost identifiers or overwrote public ID")
	}
	evidence, err := s.SelectOnchainEvidence(t.Context(), db, 1, 1, 2)
	must(t, err)
	ids, err := evidence.ExecutionIdentifiers()
	must(t, err)
	if ids == nil || *ids.VenueOrderID != "Venue-A" || evidence.TxPayload != nil {
		t.Fatal("identifier source missing or recovery payload exposed")
	}
	// A newly composed Store verifies stored snapshots by replaying their complete history.
	fresh, err := NewStore(s.orderTable, s.executionTable, s.onchainEventTable, s.feeTable)
	must(t, err)
	late := fact(5, 1, 1, EventTypeEvidenceRecorded, "late-evidence")
	late.Event.ReferenceEventID = ptr(uint64(3))
	late = withIdentifierFixture(t, late, ExecutionIdentifiers{VenueOrderID: ptr("Venue-A"), ClientOrderID: ptr("client-A")})
	appendFixture(t, fresh, db, late)
	if !appendFixture(t, fresh, db, ack).Duplicate {
		t.Fatal("ack retry was not idempotent after subsequent fills")
	}
	for i, ids := range []ExecutionIdentifiers{{VenueOrderID: ptr("venue-a")}, {ClientOrderID: ptr("different-client")}} {
		conflict := fact(uint64(6+i), 1, 1, EventTypeEvidenceRecorded, fmt.Sprintf("conflict-%d", i))
		conflict.Event.ReferenceEventID = ptr(uint64(3))
		conflict = withIdentifierFixture(t, conflict, ids)
		err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, conflict); return err })
		if !errors.Is(err, ErrConflict) {
			t.Fatal("external ID reassignment accepted")
		}
	}
	page, err := fresh.ListExecutions(t.Context(), db, 1, 1, 0, 200)
	must(t, err)
	if len(page) != 1 || *page[0].VenueOrderID != "Venue-A" || *page[0].ClientOrderID != "client-A" {
		t.Fatal("snapshot read lost external identifiers")
	}
	if _, err = fresh.SelectExecution(t.Context(), db, 2, 1, a.ExecutionRecordID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("external identifiers leaked across accounts")
	}
	if _, err = fresh.SelectOnchainEvidence(t.Context(), db, 2, 1, 2); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("identifier evidence leaked across accounts")
	}
	// Each execution owns its IDs independently, even when routed from the same order.
	second := acceptance(20, 1)
	second.Event.RecordKey, second.Event.ExecutionID = []byte("second"), ptr("exec_two")
	b := appendFixture(t, fresh, db, second)
	if b.Execution.VenueOrderID != nil || b.Execution.ClientOrderID != nil {
		t.Fatal("unrelated execution inherited IDs")
	}
	lateClient := withIdentifierFixture(t, fact(21, 1, 20, EventTypeSubmitted, "late-client"), ExecutionIdentifiers{ClientOrderID: ptr("not-saved-before-send")})
	lateClient.Event.ExecutionID = ptr("exec_two")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, lateClient); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("new client ID accepted after initial submission")
	}
	_, err = db.ExecContext(t.Context(), "UPDATE "+quoted(s.executionTable)+" SET venue_order_id='tampered' WHERE id=?", a.ExecutionRecordID)
	must(t, err)
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := fresh.AppendOnchainEvent(t.Context(), tx, 1, ack); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("snapshot ID without matching evidence accepted")
	}
}
