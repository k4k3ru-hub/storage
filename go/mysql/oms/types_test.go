package oms

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

var fixtureTime = time.Date(2026, 9, 26, 1, 2, 3, 123456000, time.UTC)

func testOrder() Order {
	return Order{ID: 1, AccountID: 1, AccountRef: "wallet", AssetClass: "crypto", Domain: DomainOnchainAMMPool, Symbol: "SUI/USDC", Side: "exchange", OrderType: "market", OrderState: OrderState{Status: OrderStatusPending, Quantity: ptr("100"), FilledQuantity: "0"}, SpecificationVersion: 1, Specification: json.RawMessage(`{"quantityUnit":"SUI"}`), IdempotencyKey: []byte("order")}
}
func acceptance(id, orderID uint64) OnchainEvent {
	return OnchainEvent{Onchain: testAcceptanceEvidence(id), Event: Event{ExecutionRecordID: id, RequestedQuantity: ptr("100"), ID: id, OrderID: orderID, EventType: EventTypeSubmissionAccepted, RecordKey: []byte("accept"), ExecutionSystem: "tradehub", ExecutionID: ptr("exec_one"), Venue: ptr("cetus"), OccurredAt: fixtureTime}}
}
func fill(id, orderID, root uint64, key, quantity string) OnchainEvent {
	return OnchainEvent{Onchain: testFillEvidence(root, key), Event: Event{ExecutionRecordID: root, ID: id, OrderID: orderID, EventType: EventTypeFilled, SubmissionEventID: ptr(root), RecordKey: []byte(key), ExecutionSystem: "tradehub", ExecutionID: ptr("exec_one"), Quantity: ptr(quantity), CounterQuantity: ptr("1"), QuantityAssetID: ptr("sui:testnet:SUI"), CounterAssetID: ptr("sui:testnet:USDC"), QuantityDecimals: ptr(uint16(9)), CounterDecimals: ptr(uint16(6)), OrderQuantity: ptr(quantity), OccurredAt: fixtureTime}}
}
func fact(id, orderID, root uint64, kind EventType, key string) OnchainEvent {
	r := OnchainEvent{Event: Event{ID: id, OrderID: orderID, EventType: kind, RecordKey: []byte(key), ExecutionSystem: "tradehub", OccurredAt: fixtureTime}}
	if root == 0 {
		root = 1
	}
	r.Event.ExecutionRecordID = root
	r.Onchain = testEventEvidence(root, kind)
	{
		r.Event.SubmissionEventID = ptr(root)
		r.Event.ExecutionID = ptr("exec_one")
	}
	return r
}
func acceptanceDetail(family string) *OnchainEvidence {
	return &OnchainEvidence{ChainFamily: family, Chain: family, Network: "testnet", TxID: "CaseSensitiveTx", SignerID: ptr("signer"), PayloadDigest: ptr("digest"), PayloadEncoding: ptr("fixture"), TxPayload: []byte("signed-fixture-payload"), ProtocolVersion: 1, ProtocolData: json.RawMessage(`{"b":2,"a":1}`)}
}
func resultDetail(family string) *OnchainEvidence {
	return &OnchainEvidence{ChainFamily: family, Chain: family, Network: "testnet", TxID: "CaseSensitiveTx", LedgerUnit: ptr(map[string]string{"evm": "block", "solana": "slot", "sui": "checkpoint"}[family]), LedgerSequence: ptr(uint64(0)), TxPosition: ptr(uint64(0)), FinalityLevel: ptr("finalized"), ProtocolVersion: 1}
}
func fee(key, kind, amount string) ExecutionFee {
	return ExecutionFee{RecordKey: []byte(key), FeeType: kind, AccountingTreatment: "additional", AssetNamespace: "currency", AssetID: "USD", AssetDecimals: 6, Amount: amount, SourceReference: key, OccurredAt: fixtureTime}
}
func sequence(records ...OnchainEvent) []Event {
	out := make([]Event, len(records))
	for i, r := range records {
		out[i] = r.Event
		out[i].Sequence = uint64(i) + 1
	}
	return out
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestValidation verifies numeric fidelity, neutral evidence and rejected legacy stages.
//
// Version:
//   - 2026-09-26: Cover the four-table model.
func TestValidation(t *testing.T) {
	must(t, testOrder().Validate())
	for _, value := range []string{"0", "01", "1.0", "1e2", "-1", "NaN", "", "1.", strings.Repeat("1", 385)} {
		o := testOrder()
		o.Quantity = &value
		if !errors.Is(o.Validate(), ErrInvalidParameter) {
			t.Fatalf("invalid quantity accepted: length=%d", len(value))
		}
	}
	o := testOrder()
	o.Quantity = ptr("1." + strings.Repeat("0", 300) + "1")
	must(t, o.Validate())
	o.Quantity = nil
	must(t, o.Validate())
	for _, change := range []func(*Order){func(o *Order) { o.Status = "processing" }, func(o *Order) { o.Specification = json.RawMessage(`[]`) }, func(o *Order) { o.SpecificationVersion = 0 }, func(o *Order) { o.ParentOrderID = &o.ID }, func(o *Order) { o.TakeProfitType = ptr("price") }} {
		o := testOrder()
		change(&o)
		if !errors.Is(o.Validate(), ErrInvalidParameter) {
			t.Fatal("invalid order accepted")
		}
	}
	for _, kind := range []EventType{"prepared", "approval", "partially_filled"} {
		e := sequence(acceptance(1, 1))[0]
		e.EventType = kind
		if !errors.Is(e.Validate(), ErrInvalidParameter) {
			t.Fatal("legacy fact accepted")
		}
	}
	for _, family := range []string{"evm", "solana", "sui"} {
		r := acceptance(1, 1)
		r.Onchain = acceptanceDetail(family)
		_, err := normalizeRecord(r, 1, nil)
		must(t, err)
		f := fill(2, 1, 1, "fill", "0.1")
		f.Onchain = resultDetail(family)
		f.Onchain.LedgerSequence = ptr(uint64(math.MaxUint64))
		f.Onchain.EventPosition = ptr(map[string]string{"evm": "v1/log/12", "solana": "v1/instruction/3/inner/1", "sui": "v1/event/2"}[family])
		_, err = normalizeRecord(f, 2, nil)
		must(t, err)
		f.Onchain.LedgerUnit = ptr("invalid")
		if _, err := normalizeRecord(f, 2, nil); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("wrong ledger accepted")
		}
	}
	for _, position := range []string{"v1/log/01", "v1/log/-1", "v2/log/1", "v1/event/1"} {
		if eventPosition("evm", position) == nil {
			t.Fatal("invalid position accepted")
		}
	}
	for _, amount := range []string{"-0", "-1.0", "0.0000001", "1e2"} {
		f := fee("fee", "gas", amount)
		f.ID = 1
		f.OrderID = 1
		f.EventID = 1
		f.ExecutionRecordID = 1
		if f.Validate() == nil {
			t.Fatal("invalid fee accepted")
		}
	}
}

// TestStoreComposition verifies explicit four-table composition and nil guards.
//
// Version:
//   - 2026-09-26: Update constructor and remove mutable execution methods.
func TestStoreComposition(t *testing.T) {
	s, err := NewDefaultStore()
	must(t, err)
	if s.orderTable != DefaultOrderTableName || s.executionTable != DefaultExecutionTableName || s.onchainEventTable != DefaultOnchainEventTableName || s.feeTable != DefaultFeeTableName {
		t.Fatal("wrong composition")
	}
	for _, names := range [][4]string{{"", "b", "c", "d"}, {"a;DROP", "b", "c", "d"}, {"a", "A", "c", "d"}, {strings.Repeat("a", 65), "b", "c", "d"}} {
		if _, err := NewStore(names[0], names[1], names[2], names[3]); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("invalid names accepted")
		}
	}
	var tx *sql.Tx
	if _, err := s.SelectOrderForUpdate(context.Background(), tx, 1, 1); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil tx accepted")
	}
	if _, err := s.AppendOnchainEvent(context.Background(), tx, 1, acceptance(1, 1)); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil tx append")
	}
	if _, err := s.InsertOrder(nil, tx, testOrder()); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil context accepted")
	}
	if a, b := GenerateOrderID(), GenerateOrderID(); a == 0 || b <= a {
		t.Fatal("invalid ids")
	}
}

// TestReplayLifecycle verifies partial fills, absolute corrections and deterministic replay.
//
// Version:
//   - 2026-09-26: Added.
func TestReplayLifecycle(t *testing.T) {
	o := testOrder()
	a := acceptance(1, 1)
	f1 := fill(2, 1, 1, "a", "30")
	f2 := fill(3, 1, 1, "b", "70")
	history := sequence(a, f1, f2)
	for n, want := range []OrderStatus{OrderStatusPending, OrderStatusPartiallyFilled, OrderStatusFilled} {
		p, err := ReplayOrder(o, history[:n+1])
		must(t, err)
		if p.State.Status != want {
			t.Fatalf("state=%s", p.State.Status)
		}
	}
	corrected := fill(4, 1, 1, "correction", "20")
	corrected.Event.EventType = EventTypeFillCorrected
	corrected.Event.ReferenceEventID = ptr(uint64(2))
	corrected.Event.SourceVersion = ptr(uint64(2))
	history = sequence(a, f1, f2, corrected)
	p, err := ReplayOrder(o, history)
	must(t, err)
	if p.State.FilledQuantity != "90" || p.State.Status != OrderStatusPartiallyFilled || p.State.CompletedAt != nil || len(p.ActiveFills) != 2 {
		t.Fatal("correction did not replace")
	}
	reversed := fact(5, 1, 1, EventTypeFillReversed, "reversal")
	reversed.Event.ReferenceEventID = ptr(uint64(4))
	reversed.Event.SourceVersion = ptr(uint64(3))
	p, err = ReplayOrder(o, sequence(a, f1, f2, corrected, reversed))
	must(t, err)
	if p.State.FilledQuantity != "70" {
		t.Fatal("reversal did not subtract")
	}
	bad := reversed
	bad.Event.ID = 6
	bad.Event.RecordKey = []byte("again")
	if _, err := ReplayOrder(o, sequence(a, f1, f2, corrected, reversed, bad)); !errors.Is(err, ErrConflict) {
		t.Fatal("reversed inactive fill twice")
	}
	old := corrected
	old.Event.ReferenceEventID = ptr(uint64(4))
	old.Event.ID = 5
	old.Event.RecordKey = []byte("stale")
	old.Event.SourceVersion = ptr(uint64(1))
	if _, err := ReplayOrder(o, sequence(a, f1, f2, corrected, old)); !errors.Is(err, ErrConflict) {
		t.Fatal("stale correction accepted")
	}
	if *f1.Event.Quantity != "30" {
		t.Fatal("input mutated")
	}
}

// TestReplayTermination verifies failure isolation, cancellation, reorg and atomic leg quantities.
//
// Version:
//   - 2026-09-26: Added.
func TestReplayTermination(t *testing.T) {
	a := acceptance(1, 1)
	f := fill(2, 1, 1, "fill", "30")
	cancel := fact(3, 1, 0, EventTypeOrderCanceled, "cancel")
	p, err := ReplayOrder(testOrder(), sequence(a, f, cancel))
	must(t, err)
	if p.State.Status != OrderStatusCanceled || p.State.FilledQuantity != "30" || p.State.CompletedAt == nil {
		t.Fatal("cancel lost fill")
	}
	failed := fact(2, 1, 1, EventTypeFailed, "failed")
	p, err = ReplayOrder(testOrder(), sequence(a, failed))
	must(t, err)
	if p.State.Status != OrderStatusPending {
		t.Fatal("single tx failed order")
	}
	end := fact(3, 1, 0, EventTypeOrderFailed, "end")
	p, err = ReplayOrder(testOrder(), sequence(a, failed, end))
	must(t, err)
	if p.State.Status != OrderStatusFailed {
		t.Fatal("order failure missing")
	}
	success := fact(2, 1, 1, EventTypeSucceeded, "success")
	full := fill(3, 1, 1, "full", "100")
	reverse := fact(4, 1, 1, EventTypeReversed, "reorg")
	reverse.Event.ReferenceEventID = ptr(uint64(2))
	p, err = ReplayOrder(testOrder(), sequence(a, success, full, reverse))
	must(t, err)
	if p.State.Status != OrderStatusPending || p.State.FilledQuantity != "0" || p.State.CompletedAt != nil {
		t.Fatal("reorg left completed fill")
	}
	leg := fill(4, 1, 1, "intermediate", "100")
	leg.Event.OrderQuantity = ptr("0")
	p, err = ReplayOrder(testOrder(), sequence(a, success, full, leg))
	must(t, err)
	if p.State.FilledQuantity != "100" || len(p.ActiveFills) != 2 {
		t.Fatal("intermediate leg counted twice")
	}
	late := fill(4, 1, 1, "late", "30")
	late.Event.OccurredAt = fixtureTime.Add(-time.Hour)
	p, err = ReplayOrder(testOrder(), sequence(a, cancel, late))
	must(t, err)
	if p.State.FilledQuantity != "30" || p.State.Status != OrderStatusCanceled {
		t.Fatal("late fill lost cancellation")
	}
	o := testOrder()
	o.Quantity = nil
	p, err = ReplayOrder(o, sequence(a, full))
	must(t, err)
	if p.State.Status != OrderStatusPartiallyFilled {
		t.Fatal("guessed unknown completion")
	}
}

// TestReplayPrecision verifies exact sums beyond floating-point and SQL DECIMAL precision.
//
// Version:
//   - 2026-09-26: Added.
func TestReplayPrecision(t *testing.T) {
	a := acceptance(1, 1)
	x := fill(2, 1, 1, "x", "0.1")
	y := fill(3, 1, 1, "y", "0.2")
	o := testOrder()
	o.Quantity = ptr("0.3")
	p, err := ReplayOrder(o, sequence(a, x, y))
	must(t, err)
	if p.State.FilledQuantity != "0.3" || p.State.Status != OrderStatusFilled {
		t.Fatal("decimal sum rounded")
	}
	x.Event.OrderQuantity = ptr("1." + strings.Repeat("0", 300) + "1")
	y.Event.OrderQuantity = ptr("2")
	o.Quantity = nil
	p, err = ReplayOrder(o, sequence(a, x, y))
	must(t, err)
	if p.State.FilledQuantity != "3."+strings.Repeat("0", 300)+"1" {
		t.Fatal("precision lost")
	}
}

func testAcceptanceEvidence(id uint64) *OnchainEvidence {
	d := acceptanceDetail("sui")
	d.TxID = fmt.Sprintf("tx-%d", id)
	return d
}
func testEventEvidence(root uint64, kind EventType) *OnchainEvidence {
	d := resultDetail("sui")
	d.TxID = fmt.Sprintf("tx-%d", root)
	if !isResult(kind) && !isFill(kind) {
		d.LedgerUnit = nil
		d.LedgerSequence = nil
		d.TxPosition = nil
		d.FinalityLevel = nil
	}
	return d
}
func testFillEvidence(root uint64, key string) *OnchainEvidence {
	d := testEventEvidence(root, EventTypeFilled)
	var index uint64
	for _, r := range key {
		index = index*31 + uint64(r)
	}
	d.EventPosition = ptr(fmt.Sprintf("v1/event/%d", index))
	return d
}

// TestReplayExecutionOwnership rejects event references crossing execution snapshots.
//
// Version:
//   - 2026-09-27: Enforce execution identity in the pure history projection.
func TestReplayExecutionOwnership(t *testing.T) {
	a := acceptance(1, 1)
	f := fill(2, 1, 1, "fill", "10")
	f.Event.ExecutionRecordID = 999
	if _, err := ReplayOrder(testOrder(), sequence(a, f)); !errors.Is(err, ErrConflict) {
		t.Fatal("cross-execution event accepted", err)
	}
	b := acceptance(3, 1)
	b.Event.RecordKey = []byte("another")
	b.Event.ExecutionID = ptr("another")
	b.Event.ExecutionRecordID = 1
	if _, err := ReplayOrder(testOrder(), sequence(a, b)); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate snapshot root accepted", err)
	}
	f.Event.ExecutionRecordID = 0
	f.Event.Sequence = 2
	if !errors.Is(f.Event.Validate(), ErrInvalidParameter) {
		t.Fatal("missing execution parent accepted")
	}
}
