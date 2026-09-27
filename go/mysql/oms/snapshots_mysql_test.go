package oms

import (
	"database/sql"
	"errors"
	"testing"
)

// TestMySQLExecutionSnapshots verifies execution aggregation independently from explicit order termination.
//
// Version:
//   - 2026-09-26: Cover two execution snapshots, retry, source fees and partial failure.
func TestMySQLExecutionSnapshots(t *testing.T) {
	s, db := mysqlStore(t)
	insertFixture(t, s, db, testOrder())
	a := acceptance(1, 1)
	a.Event.RequestedQuantity = ptr("60")
	accepted := appendFixture(t, s, db, a)
	if accepted.Execution.Status != ExecutionStatusPending || accepted.Execution.FilledQuantity != "0" {
		t.Fatal("acceptance snapshot")
	}
	b := acceptance(2, 1)
	b.Event.RecordKey = []byte("accept-b")
	b.Event.ExecutionID = ptr("exec_two")
	b.Event.RequestedQuantity = ptr("40")
	appendFixture(t, s, db, b)
	result := fact(3, 1, 1, EventTypeSucceeded, "a-success")
	result.Event.FeesComplete = ptr(true)
	appendFixture(t, s, db, result)
	fillA := fill(4, 1, 1, "a-fill", "60")
	fillA.Event.FeesComplete = ptr(true)
	fillA.Fees = []ExecutionFee{fee("commission-a", "trading", "0.1"), fee("tax-a", "tax", "0.2")}
	done := appendFixture(t, s, db, fillA)
	if done.Execution.Status != ExecutionStatusFilled || done.Execution.FilledQuantity != "60" || !done.Execution.FeesComplete || done.Execution.CompletedAt == nil {
		t.Fatal("execution snapshot not filled")
	}
	failure := fact(5, 1, 2, EventTypeFailed, "b-failure")
	failure.Event.ExecutionID = ptr("exec_two")
	failure.Event.FeesComplete = ptr(true)
	failed := appendFixture(t, s, db, failure)
	if failed.Execution.Status != ExecutionStatusFailed || failed.State.Status != OrderStatusPartiallyFilled || failed.State.FilledQuantity != "60" || failed.State.CompletedAt != nil {
		t.Fatal("execution failure ended order")
	}
	snapshots, err := s.ListExecutions(t.Context(), db, 1, 1, 0, 200)
	must(t, err)
	if len(snapshots) != 2 {
		t.Fatalf("executions=%d, want two snapshots", len(snapshots))
	}
	events, err := s.ListEvents(t.Context(), db, 1, 1, 0, 200)
	must(t, err)
	if len(events) != 5 {
		t.Fatal("history lost")
	}
	costs, err := s.ListExecutionFees(t.Context(), db, 1, 1, done.ExecutionRecordID)
	must(t, err)
	if len(costs) != 2 || costs[0].EventID != done.EventID || costs[1].ExecutionRecordID != done.ExecutionRecordID {
		t.Fatal("cost hierarchy lost")
	}
	if _, err = s.SelectExecution(t.Context(), db, 2, 1, done.ExecutionRecordID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-account snapshot read")
	}
	if _, err = s.ListExecutionFees(t.Context(), db, 2, 1, done.ExecutionRecordID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-account costs read")
	}
	// An already completed execution remains one row when a delayed acknowledgement arrives.
	ack := fact(6, 1, 1, EventTypeSubmitted, "ack")
	got := appendFixture(t, s, db, ack)
	if got.Execution.Status != ExecutionStatusFilled || !got.Execution.CompletedAt.Equal(*done.Execution.CompletedAt) {
		t.Fatal("late acknowledgement regressed execution")
	}
	correction := fill(7, 1, 1, "correct", "50")
	correction.Event.EventType = EventTypeFillCorrected
	correction.Event.ReferenceEventID = ptr(uint64(4))
	correction.Event.FeesComplete = ptr(false)
	got = appendFixture(t, s, db, correction)
	if got.Execution.Status != ExecutionStatusPartiallyFilled || got.Execution.FilledQuantity != "50" || got.State.FilledQuantity != "50" || got.Execution.FeesComplete {
		t.Fatal("correction did not update both snapshots")
	}
	late := fact(8, 1, 1, EventTypeFeesRecorded, "fees-complete")
	late.Event.ReferenceEventID = ptr(uint64(7))
	late.Event.FeesComplete = ptr(true)
	if !appendFixture(t, s, db, late).Execution.FeesComplete {
		t.Fatal("late fee investigation not reflected")
	}
	termination := fact(9, 1, 2, EventTypeOrderCanceled, "stop")
	termination.Event.ExecutionID = ptr("exec_two")
	ended := appendFixture(t, s, db, termination)
	if ended.State.Status != OrderStatusCanceled || ended.State.FilledQuantity != "50" || ended.Execution.Status != ExecutionStatusFailed {
		t.Fatal("order termination overwrote execution outcome")
	}
	c := acceptance(10, 1)
	c.Event.ExecutionID = ptr("exec_three")
	c.Event.RecordKey = []byte("accept-c")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, c); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("new execution on terminated order")
	}
}

// TestMySQLExecutionSnapshotRollback verifies that invalid events cannot leave the execution snapshot ahead of its history.
//
// Version:
//   - 2026-09-26: Cover both snapshot rollback and detected external mutation.
func TestMySQLExecutionSnapshotRollback(t *testing.T) {
	s, db := mysqlStore(t)
	insertFixture(t, s, db, testOrder())
	a := appendFixture(t, s, db, acceptance(1, 1))
	stop := errors.New("test abort")
	err := withTx(t.Context(), db, func(tx *sql.Tx) error {
		if _, err := s.AppendOnchainEvent(t.Context(), tx, 1, fill(2, 1, 1, "fill", "60")); err != nil {
			return err
		}
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
	got, err := s.SelectExecution(t.Context(), db, 1, 1, a.ExecutionRecordID)
	must(t, err)
	if !executionEqual(*got, a.Execution) {
		t.Fatal("snapshot survived rollback")
	}
	_, err = db.ExecContext(t.Context(), "UPDATE "+quoted(s.executionTable)+" SET filled_quantity='50' WHERE id=?", a.ExecutionRecordID)
	must(t, err)
	err = withTx(t.Context(), db, func(tx *sql.Tx) error {
		_, err := s.AppendOnchainEvent(t.Context(), tx, 1, fill(2, 1, 1, "fill", "60"))
		return err
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("external snapshot change accepted")
	}
}
