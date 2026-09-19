package oms

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }
func testOrder() Order {
	return Order{ID: 1, AccountID: 1, AccountRef: "wallet", AssetClass: "crypto", Domain: DomainOnchainAMMPool, Venue: "uniswap-v3", Symbol: "ABC/USDC", Side: "exchange", OrderType: "limit", OrderState: OrderState{Status: OrderStatusPending, Quantity: ptr("100"), FilledQuantity: "0"}, IdempotencyKey: []byte("create")}
}
func testSwap() OnchainAMMPoolSwap {
	return OnchainAMMPoolSwap{OrderID: 1, ChainFamily: "evm", Chain: "base", Network: "sepolia", PoolID: "pool", TokenInID: "USDC", TokenOutID: "ABC", TokenInDecimals: 6, TokenOutDecimals: 18, SwapKind: "exact-input", Signer: "signer", Recipient: "recipient", MaximumSlippageBPS: 100, ExecutionTTLMS: 60000}
}
func testExecution() Execution {
	return Execution{ID: 1, OrderID: 1, AttemptNumber: 1, ExecType: ExecutionTypeFilled, Purpose: ExecutionPurposeTrade, ExecutionState: ExecutionState{Status: ExecutionStatusSucceeded, Quantity: ptr("30"), CounterQuantity: ptr("3"), OccurredAt: time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)}, RecordKey: []byte("fill-a"), ExecutionSystem: "tradehub"}
}

// TestOrderValidation verifies approved statuses, decimal precision and condition nullability.
//
// Version:
//   - 2026-09-20: Added.
func TestOrderValidation(t *testing.T) {
	for _, status := range []OrderStatus{OrderStatusPending, OrderStatusProcessing, OrderStatusPartiallyFilled, OrderStatusFilled, OrderStatusCanceled, OrderStatusExpired, OrderStatusFailed, OrderStatusRejected} {
		o := testOrder()
		o.Status = status
		if err := o.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []string{"0", "01", "1.0", "1e2", "-1", "NaN", "", "1.", strings.Repeat("1", 385)} {
		o := testOrder()
		o.Quantity = &value
		if err := o.Validate(); !errors.Is(err, ErrInvalidParameter) {
			t.Fatalf("accepted invalid quantity: length=%d", len(value))
		}
	}
	o := testOrder()
	o.Quantity = ptr("1." + strings.Repeat("0", 300) + "1")
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	o.Quantity = nil
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []func(*Order){
		func(o *Order) { o.ParentOrderID = &o.ID }, func(o *Order) { o.Status = "bad" },
		func(o *Order) { o.TakeProfitType = ptr("price") }, func(o *Order) { o.StopLossValue = ptr("1") },
		func(o *Order) { o.StopLossType = ptr("return_bps"); o.StopLossValue = ptr("-10001") },
		func(o *Order) { o.TakeProfitType = ptr("return_bps"); o.TakeProfitValue = ptr("1.5") },
		func(o *Order) { o.IdempotencyKey = make([]byte, 129) },
	} {
		o := testOrder()
		f(&o)
		if !errors.Is(o.Validate(), ErrInvalidParameter) {
			t.Fatal("invalid order accepted")
		}
	}
	o = testOrder()
	o.TakeProfitType = ptr("return_bps")
	o.TakeProfitValue = ptr("2000")
	o.StopLossType = ptr("return_bps")
	o.StopLossValue = ptr("-1000")
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
}

// TestExecutionValidation verifies rejected stages and filled-only quantities.
//
// Version:
//   - 2026-09-20: Added.
func TestExecutionValidation(t *testing.T) {
	for _, status := range []ExecutionStatus{ExecutionStatusPending, ExecutionStatusSucceeded, ExecutionStatusFailed, ExecutionStatusReversed, ExecutionStatusRejected} {
		e := testExecution()
		e.Status = status
		if err := e.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []func(*Execution){
		func(e *Execution) { e.ExecType = "fill" }, func(e *Execution) { e.Purpose = ExecutionPurposeApproval },
		func(e *Execution) { e.Quantity = nil }, func(e *Execution) { e.CounterQuantity = nil },
		func(e *Execution) { e.ExecType = ExecutionTypeSubmitted }, func(e *Execution) { e.ExpiresAt = ptr(time.Now()) },
		func(e *Execution) { e.AttemptNumber = 0 }, func(e *Execution) { e.OccurredAt = time.Time{} },
	} {
		e := testExecution()
		f(&e)
		if !errors.Is(e.Validate(), ErrInvalidParameter) {
			t.Fatal("invalid execution accepted")
		}
	}
	e := testExecution()
	e.ExecType = ExecutionTypePrepared
	e.Purpose = ExecutionPurposeApproval
	e.Quantity = nil
	e.CounterQuantity = nil
	e.Status = ExecutionStatusRejected
	e.ExpiresAt = ptr(time.Now())
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
}

// TestStoreComposition verifies explicit table configuration and nil dependency rejection.
//
// Version:
//   - 2026-09-20: Added.
func TestStoreComposition(t *testing.T) {
	s, err := NewDefaultStore()
	if err != nil {
		t.Fatal(err)
	}
	if s.orderTable != DefaultOrderTableName || s.swapTable != DefaultSwapTableName || s.executionTable != DefaultExecutionTableName {
		t.Fatal("wrong composition")
	}
	for _, names := range [][3]string{{"", "b", "c"}, {"a;DROP", "b", "c"}, {"a", "a", "c"}, {"A", "a", "c"}, {strings.Repeat("a", 65), "b", "c"}} {
		if _, err := NewStore(names[0], names[1], names[2]); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("invalid tables accepted")
		}
	}
	var tx *sql.Tx
	if _, err := s.SelectOrderForUpdate(context.Background(), tx, 1, 1); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil transaction accepted")
	}
	if _, err := s.SelectOrder(nil, tx, 1, 1); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil context accepted")
	}
	if _, err := s.InsertExecution(context.Background(), tx, 1, testExecution()); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil transaction accepted")
	}
	if a, b := GenerateOrderID(), GenerateOrderID(); a == 0 || b <= a {
		t.Fatal("invalid generated IDs")
	}
}
