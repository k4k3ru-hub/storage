package oms

import (
	"database/sql"
	"errors"
	"testing"
)

// TestMySQLReadPages verifies owner-scoped cursors and omission of recovery bytes.
//
// Version:
//   - 2026-09-27: Added.
func TestMySQLReadPages(t *testing.T) {
	s, db := mysqlStore(t)
	for _, id := range []uint64{1, 3, 5} {
		o := testOrder()
		o.ID = id
		o.IdempotencyKey = []byte{byte(id)}
		if id == 3 {
			o.AccountID = 2
		}
		insertFixture(t, s, db, o)
	}
	orders, err := s.ListOrders(t.Context(), db, 1, 0, 1)
	must(t, err)
	if len(orders) != 1 || orders[0].ID != 5 {
		t.Fatal("incorrect descending page")
	}
	orders, err = s.ListOrders(t.Context(), db, 1, 5, 10)
	must(t, err)
	if len(orders) != 1 || orders[0].ID != 1 {
		t.Fatal("foreign order or duplicate cursor boundary")
	}
	orders, err = s.ListOrders(t.Context(), db, 9, 0, 10)
	must(t, err)
	if len(orders) != 0 {
		t.Fatal("cross-account list")
	}
	appendFixture(t, s, db, acceptance(1, 1))
	appendFixture(t, s, db, fact(2, 1, 1, EventTypeSucceeded, "success"))
	f := fill(3, 1, 1, "fill", "100")
	f.Fees = []ExecutionFee{fee("commission", "trading", "0.1"), fee("tax", "tax", "0.2")}
	appendFixture(t, s, db, f)
	events, err := s.ListOnchainEvents(t.Context(), db, 1, 1, 0, 1)
	must(t, err)
	if len(events) != 1 || events[0].Event.Sequence != 1 || len(events[0].Onchain.TxPayload) != 0 || len(events[0].Onchain.ProtocolData) != 0 {
		t.Fatal("invalid safe event page")
	}
	payload, err := s.SelectSubmissionPayload(t.Context(), db, 1, 1, 1)
	must(t, err)
	if len(payload) == 0 {
		t.Fatal("test needs persisted payload")
	}
	events, err = s.ListOnchainEvents(t.Context(), db, 1, 1, 1, 10)
	must(t, err)
	if len(events) != 2 || events[0].Event.Sequence != 2 || events[1].Event.Sequence != 3 {
		t.Fatal("event pagination")
	}
	fees, err := s.ListOrderFees(t.Context(), db, 1, 1, 0, 1)
	must(t, err)
	if len(fees) != 1 {
		t.Fatal("fee pagination")
	}
	next, err := s.ListOrderFees(t.Context(), db, 1, 1, fees[0].ID, 10)
	must(t, err)
	if len(next) != 1 || next[0].ID <= fees[0].ID || next[0].EventID != 3 {
		t.Fatal("fee boundary or event lost")
	}
	if _, err = s.ListOnchainEvents(t.Context(), db, 2, 1, 0, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("foreign event read", err)
	}
	if _, err = s.ListOrderFees(t.Context(), db, 2, 1, 0, 1); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("foreign fee read", err)
	}
	for _, limit := range []int{0, 201} {
		if _, err = s.ListOrders(t.Context(), db, 1, 0, limit); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("list bound")
		}
		if _, err = s.ListOnchainEvents(t.Context(), db, 1, 1, 0, limit); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("event bound")
		}
		if _, err = s.ListOrderFees(t.Context(), db, 1, 1, 0, limit); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("fee bound")
		}
	}
}
