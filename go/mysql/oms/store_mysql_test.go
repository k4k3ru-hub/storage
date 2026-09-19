package oms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func mysqlStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("K4K3RU_OMS_TEST_DSN")
	if dsn == "" {
		t.Skip("set K4K3RU_OMS_TEST_DSN to an isolated MySQL database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	prefix := fmt.Sprintf("oms_test_%d", GenerateOrderID())
	s, err := NewStore(prefix, prefix+"_swap", prefix+"_exec")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTables(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, name := range []string{s.executionTable, s.swapTable, s.orderTable} {
			if _, err := db.Exec("DROP TABLE " + quoted(name)); err != nil {
				t.Error(err)
			}
		}
	})
	return s, db
}
func inTx(t *testing.T, db *sql.DB, fn func(*sql.Tx) error) error {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = fn(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			t.Fatal(rollbackErr)
		}
		return err
	}
	return tx.Commit()
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestMySQLOrderLifecycle verifies ownership, decimal fidelity, partial fills and atomic corrections.
//
// Version:
//   - 2026-09-20: Added.
func TestMySQLOrderLifecycle(t *testing.T) {
	s, db := mysqlStore(t)
	// Another custom schema in the same database must not collide on named constraints.
	_, second := mysqlStore(t)
	must(t, second.PingContext(t.Context()))
	var id uint64
	order := testOrder()
	order.ID = 0
	order.TakeProfitType = ptr("return_bps")
	order.TakeProfitValue = ptr("2000")
	order.StopLossType = ptr("return_bps")
	order.StopLossValue = ptr("-1000")
	order.Quantity = ptr("100." + strings.Repeat("0", 100) + "1")
	swap := testSwap()
	swap.OrderID = 0
	must(t, inTx(t, db, func(tx *sql.Tx) error {
		var err error
		id, err = s.InsertOnchainAMMPoolOrder(t.Context(), tx, order, swap)
		return err
	}))
	if order.ID != 0 || swap.OrderID != 0 {
		t.Fatal("input mutated")
	}
	stored, err := s.SelectOrder(t.Context(), db, 1, id)
	must(t, err)
	if stored.ID == 0 || stored.Quantity == nil || *stored.Quantity != *order.Quantity || stored.ParentOrderID != nil || stored.CompletedAt != nil {
		t.Fatal("order roundtrip")
	}
	gotSwap, err := s.SelectOnchainAMMPoolSwap(t.Context(), db, 1, id)
	must(t, err)
	if gotSwap.OrderID != id || gotSwap.TokenInDecimals != 6 {
		t.Fatal("swap roundtrip")
	}
	byKey, err := s.SelectOrderByIdempotencyKey(t.Context(), db, 1, order.IdempotencyKey)
	must(t, err)
	if byKey.ID != id {
		t.Fatal("wrong idempotency lookup")
	}
	err = inTx(t, db, func(tx *sql.Tx) error {
		_, err := s.InsertOnchainAMMPoolOrder(t.Context(), tx, order, swap)
		return err
	})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("expected duplicate: %v", err)
	}
	for _, read := range []func() error{
		func() error { _, err := s.SelectOrder(t.Context(), db, 2, id); return err },
		func() error { _, err := s.SelectOnchainAMMPoolSwap(t.Context(), db, 2, id); return err },
		func() error {
			_, err := s.SelectOrderByIdempotencyKey(t.Context(), db, 2, order.IdempotencyKey)
			return err
		},
	} {
		if !errors.Is(read(), sql.ErrNoRows) {
			t.Fatal("cross-account read")
		}
	}
	child := testOrder()
	child.ID = 0
	child.ParentOrderID = &id
	child.Quantity = nil
	child.IdempotencyKey = []byte("close")
	child.AccountID = 2
	err = inTx(t, db, func(tx *sql.Tx) error {
		_, err := s.InsertOnchainAMMPoolOrder(t.Context(), tx, child, swap)
		return err
	})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-account parent: %v", err)
	}
	child.AccountID = 1
	var childID uint64
	must(t, inTx(t, db, func(tx *sql.Tx) error {
		var err error
		childID, err = s.InsertOnchainAMMPoolOrder(t.Context(), tx, child, swap)
		return err
	}))
	childRow, err := s.SelectOrder(t.Context(), db, 1, childID)
	must(t, err)
	if childRow.Quantity != nil || childRow.ParentOrderID == nil || *childRow.ParentOrderID != id {
		t.Fatal("child roundtrip")
	}
	record := testExecution()
	record.ID = 0
	record.OrderID = id
	record.ExecutionID = ptr("exec_shared")
	record.SourceVersion = ptr(uint64(1))
	must(t, inTx(t, db, func(tx *sql.Tx) error {
		approval := record
		approval.ExecType = ExecutionTypePrepared
		approval.Purpose = ExecutionPurposeApproval
		approval.RecordKey = []byte("approval")
		approval.Quantity = nil
		approval.CounterQuantity = nil
		approval.Status = ExecutionStatusRejected
		if _, err := s.InsertExecution(t.Context(), tx, 1, approval); err != nil {
			return err
		}
		if _, err := s.InsertExecution(t.Context(), tx, 1, record); err != nil {
			return err
		}
		second := record
		second.RecordKey = []byte("fill-b")
		second.Quantity = ptr("20")
		second.CounterQuantity = ptr("2")
		if _, err := s.InsertExecution(t.Context(), tx, 1, second); err != nil {
			return err
		}
		next := stored.OrderState
		next.Status = OrderStatusPartiallyFilled
		next.FilledQuantity = "50"
		return s.UpdateOrderState(t.Context(), tx, 1, id, stored.OrderState, next)
	}))
	records, err := s.ListExecutions(t.Context(), db, 1, id, 0, 2)
	must(t, err)
	if len(records) != 2 {
		t.Fatal("page size")
	}
	tail, err := s.ListExecutions(t.Context(), db, 1, id, records[1].ID, 2)
	must(t, err)
	if len(tail) != 1 {
		t.Fatal("pagination")
	}
	stored, err = s.SelectOrder(t.Context(), db, 1, id)
	must(t, err)
	if stored.FilledQuantity != "50" {
		t.Fatal("missing aggregation")
	}
	err = inTx(t, db, func(tx *sql.Tx) error { _, err := s.InsertExecution(t.Context(), tx, 1, record); return err })
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate fill: %v", err)
	}
	if _, err := s.SelectExecutionByKey(t.Context(), db, 2, id, record.RecordKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("execution ownership")
	}
	if _, err := s.ListExecutions(t.Context(), db, 2, id, 0, 10); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("list ownership")
	}
	err = inTx(t, db, func(tx *sql.Tx) error { _, err := s.InsertExecution(t.Context(), tx, 2, record); return err })
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("execution insertion ownership")
	}
	before, err := s.SelectExecutionByKey(t.Context(), db, 1, id, record.RecordKey)
	must(t, err)
	// Reversal and caller-computed cumulative quantity commit together.
	must(t, inTx(t, db, func(tx *sql.Tx) error {
		next := before.ExecutionState
		next.Status = ExecutionStatusReversed
		next.SourceVersion = ptr(uint64(2))
		if err := s.UpdateExecutionState(t.Context(), tx, 1, id, record.RecordKey, before.ExecutionState, next); err != nil {
			return err
		}
		nextOrder := stored.OrderState
		nextOrder.FilledQuantity = "20"
		return s.UpdateOrderState(t.Context(), tx, 1, id, stored.OrderState, nextOrder)
	}))
	err = inTx(t, db, func(tx *sql.Tx) error {
		return s.UpdateExecutionState(t.Context(), tx, 1, id, record.RecordKey, before.ExecutionState, before.ExecutionState)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale record overwritten: %v", err)
	}
	err = inTx(t, db, func(tx *sql.Tx) error {
		return s.UpdateOrderState(t.Context(), tx, 1, id, stored.OrderState, stored.OrderState)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale order overwritten: %v", err)
	}
	stored, err = s.SelectOrder(t.Context(), db, 1, id)
	must(t, err)
	if stored.FilledQuantity != "20" {
		t.Fatal("reversal aggregation")
	}
	// CompletedAt and rejected survive a write/read; storage does not infer transitions.
	must(t, inTx(t, db, func(tx *sql.Tx) error {
		next := childRow.OrderState
		next.Status = OrderStatusRejected
		next.CompletedAt = ptr(time.Now())
		return s.UpdateOrderState(t.Context(), tx, 1, childID, childRow.OrderState, next)
	}))
	updatedChild, err := s.SelectOrder(t.Context(), db, 1, childID)
	must(t, err)
	if updatedChild.Status != OrderStatusRejected || updatedChild.CompletedAt == nil {
		t.Fatal("rejected state lost")
	}
	must(t, inTx(t, db, func(tx *sql.Tx) error {
		return s.UpdateOrderState(t.Context(), tx, 1, childID, updatedChild.OrderState, updatedChild.OrderState)
	}))
	// Explicit rollback leaves neither inserted record nor updated snapshot.
	abort := errors.New("abort test")
	err = inTx(t, db, func(tx *sql.Tx) error {
		third := record
		third.RecordKey = []byte("rolled-back")
		if _, err := s.InsertExecution(t.Context(), tx, 1, third); err != nil {
			return err
		}
		next := stored.OrderState
		next.FilledQuantity = "50"
		if err := s.UpdateOrderState(t.Context(), tx, 1, id, stored.OrderState, next); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if _, err := s.SelectExecutionByKey(t.Context(), db, 1, id, []byte("rolled-back")); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("record not rolled back")
	}
	after, err := s.SelectOrder(t.Context(), db, 1, id)
	must(t, err)
	if after.FilledQuantity != "20" {
		t.Fatal("order not rolled back")
	}
	// Parent and swap disappear together when the surrounding transaction aborts.
	rollbackOrder := testOrder()
	rollbackOrder.ID = 0
	rollbackOrder.IdempotencyKey = []byte("rolled-order")
	err = inTx(t, db, func(tx *sql.Tx) error {
		_, err := s.InsertOnchainAMMPoolOrder(t.Context(), tx, rollbackOrder, swap)
		if err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if _, err := s.SelectOrderByIdempotencyKey(t.Context(), db, 1, rollbackOrder.IdempotencyKey); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("parent not rolled back")
	}
}

// TestMySQLConcurrentWrites verifies duplicate creation and order-lock serialization.
//
// Version:
//   - 2026-09-20: Added.
func TestMySQLConcurrentWrites(t *testing.T) {
	s, db := mysqlStore(t)
	order := testOrder()
	order.ID = 0
	swap := testSwap()
	swap.OrderID = 0
	tx1, err := db.BeginTx(t.Context(), nil)
	must(t, err)
	defer func() {
		if err := tx1.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	id, err := s.InsertOnchainAMMPoolOrder(t.Context(), tx1, order, swap)
	must(t, err)
	done := make(chan error, 1)
	go func() {
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			done <- err
			return
		}
		_, insertErr := s.InsertOnchainAMMPoolOrder(t.Context(), tx, order, swap)
		done <- errors.Join(insertErr, tx.Rollback())
	}()
	must(t, tx1.Commit())
	if err := <-done; !errors.Is(err, ErrDuplicate) {
		t.Fatalf("concurrent duplicate: %v", err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	must(t, err)
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	_, err = s.SelectOrderForUpdate(t.Context(), tx, 1, id)
	must(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	other, err := db.BeginTx(ctx, nil)
	must(t, err)
	_, lockErr := s.SelectOrderForUpdate(ctx, other, 1, id)
	rollbackErr := other.Rollback()
	if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
		t.Fatal(rollbackErr)
	}
	if !errors.Is(lockErr, context.DeadlineExceeded) {
		t.Fatalf("lock was not held: %v", lockErr)
	}
}
