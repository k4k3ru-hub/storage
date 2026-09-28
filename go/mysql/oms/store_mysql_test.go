package oms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func mysqlStore(t *testing.T, withPnL ...bool) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("K4K3RU_OMS_TEST_DSN")
	if dsn == "" {
		t.Skip("set K4K3RU_OMS_TEST_DSN to an isolated MySQL database")
	}
	db, err := sql.Open("mysql", dsn)
	must(t, err)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	prefix := fmt.Sprintf("oms_test_%d", GenerateOrderID())
	s, err := NewStore(prefix, prefix+"_exec", prefix+"_detail", prefix+"_fee")
	if len(withPnL) > 0 && withPnL[0] {
		s, err = NewStoreWithPnL(prefix, prefix+"_exec", prefix+"_detail", prefix+"_fee", prefix+"_pnl")
	}
	must(t, err)
	must(t, s.CreateTables(t.Context(), db))
	t.Cleanup(func() {
		for _, name := range []string{s.pnlTable, s.feeTable, s.onchainEventTable, s.executionTable, s.orderTable} {
			if name == "" {
				continue
			}
			if _, err := db.Exec("DROP TABLE " + quoted(name)); err != nil {
				t.Error(err)
			}
		}
	})
	return s, db
}
func withTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			err = errors.Join(err, e)
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func insertFixture(t *testing.T, s *Store, db *sql.DB, o Order) {
	t.Helper()
	must(t, withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.InsertOrder(t.Context(), tx, o); return err }))
}
func appendFixture(t *testing.T, s *Store, db *sql.DB, r OnchainEvent) *AppendResult {
	t.Helper()
	var result *AppendResult
	must(t, withTx(t.Context(), db, func(tx *sql.Tx) error {
		var err error
		result, err = s.AppendOnchainEvent(t.Context(), tx, 1, r)
		return err
	}))
	return result
}

// TestMySQLLifecycle verifies atomic append, ownership, duplicate contents, corrections and fees.
//
// Version:
//   - 2026-09-26: Replace legacy mutable-stage tests.
func TestMySQLLifecycle(t *testing.T) {
	s, db := mysqlStore(t)
	o := testOrder()
	insertFixture(t, s, db, o)
	_, err := s.SelectOrder(t.Context(), db, 2, 1)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-account order read")
	}
	a := acceptance(1, 1)
	appendFixture(t, s, db, a)
	f1 := fill(2, 1, 1, "fill-a", "30")
	f1.Fees = []ExecutionFee{fee("commission", "commission", "0.1"), fee("tax", "tax", "0.2")}
	first := appendFixture(t, s, db, f1)
	if first.State.Status != OrderStatusPartiallyFilled || first.State.FilledQuantity != "30" {
		t.Fatal("partial fill missing")
	}
	duplicate := appendFixture(t, s, db, f1)
	if !duplicate.Duplicate || duplicate.EventID != 2 || duplicate.Sequence != 2 {
		t.Fatal("not idempotent")
	}
	mismatch := f1
	mismatch.Event.Quantity = ptr("31")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, mismatch); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("same key overwritten")
	}
	changedFee := f1
	changedFee.Fees = append([]ExecutionFee(nil), f1.Fees...)
	changedFee.Fees[0].Amount = "0.3"
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, changedFee); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("fee mutation accepted")
	}
	f2 := fill(3, 1, 1, "fill-b", "70")
	done := appendFixture(t, s, db, f2)
	if done.State.Status != OrderStatusFilled || done.State.CompletedAt == nil {
		t.Fatal("not filled")
	}
	correction := fill(4, 1, 1, "correction", "20")
	correction.Event.EventType = EventTypeFillCorrected
	correction.Event.ReferenceEventID = ptr(uint64(2))
	corrected := appendFixture(t, s, db, correction)
	if corrected.State.FilledQuantity != "90" || corrected.State.Status != OrderStatusPartiallyFilled || corrected.State.CompletedAt != nil {
		t.Fatal("correction not projected")
	}
	original, err := s.SelectEventByKey(t.Context(), db, 1, 1, []byte("fill-a"))
	must(t, err)
	if *original.Quantity != "30" {
		t.Fatal("original fill changed")
	}
	fees, err := s.ListEventFees(t.Context(), db, 1, 1, 2)
	must(t, err)
	if len(fees) != 2 {
		t.Fatal("multiple fees missing")
	}
	adjustment := fact(5, 1, 1, EventTypeFeesAdjusted, "adjust")
	adjustment.Event.ReferenceEventID = ptr(uint64(2))
	adjustment.Fees = []ExecutionFee{fee("refund", "commission", "-0.05")}
	var feeID uint64
	for _, f := range fees {
		if f.FeeType == "commission" {
			feeID = f.ID
		}
	}
	adjustment.Fees[0].AdjustmentOfFeeID = &feeID
	appendFixture(t, s, db, adjustment)
	originalFees, err := s.ListEventFees(t.Context(), db, 1, 1, 2)
	must(t, err)
	if len(originalFees) != 2 || originalFees[0].Amount != fees[0].Amount {
		t.Fatal("original fees changed")
	}
	adjustments, err := s.ListEventFees(t.Context(), db, 1, 1, 5)
	must(t, err)
	if len(adjustments) != 1 || adjustments[0].Amount != "-0.05" {
		t.Fatal("adjustment lost")
	}
	page, err := s.ListEvents(t.Context(), db, 1, 1, 0, 2)
	must(t, err)
	tail, err := s.ListEvents(t.Context(), db, 1, 1, page[1].Sequence, 200)
	must(t, err)
	if len(page) != 2 || len(tail) != 3 || tail[0].Sequence != 3 {
		t.Fatal("wrong sequence pagination")
	}
	all := append(page, tail...)
	stored, err := s.SelectOrder(t.Context(), db, 1, 1)
	must(t, err)
	projection, err := ReplayOrder(*stored, all)
	must(t, err)
	if !orderStateEqual(stored.OrderState, projection.State) {
		t.Fatal("replay differs from snapshot")
	}
	for _, read := range []func() error{
		func() error { _, err := s.SelectEventByKey(t.Context(), db, 2, 1, []byte("fill-a")); return err },
		func() error { _, err := s.ListEvents(t.Context(), db, 2, 1, 0, 20); return err },
		func() error { _, err := s.ListEventFees(t.Context(), db, 2, 1, 2); return err },
	} {
		if !errors.Is(read(), sql.ErrNoRows) {
			t.Fatal("cross-account history read")
		}
	}
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 2, f1); return err })
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-account write")
	}
}

// TestMySQLOnchain verifies three chain mappings, immutable payloads and source identity checks.
//
// Version:
//   - 2026-09-26: Added.
func TestMySQLOnchain(t *testing.T) {
	for _, family := range []string{"evm", "solana", "sui"} {
		t.Run(family, func(t *testing.T) {
			s, db := mysqlStore(t)
			insertFixture(t, s, db, testOrder())
			a := acceptance(1, 1)
			a.Onchain = acceptanceDetail(family)
			appendFixture(t, s, db, a)
			d, err := s.SelectOnchainEvidence(t.Context(), db, 1, 1, 1)
			must(t, err)
			if d.TxPayload != nil || d.LedgerSequence != nil || d.TxPosition != nil {
				t.Fatal("payload exposed or missing position became zero")
			}
			payload, err := s.SelectSubmissionPayload(t.Context(), db, 1, 1, 1)
			must(t, err)
			if string(payload) != "signed-fixture-payload" {
				t.Fatal("missing recovery payload")
			}
			if _, err := s.SelectSubmissionPayload(t.Context(), db, 2, 1, 1); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("payload ownership")
			}
			duplicate := a
			copy := *a.Onchain
			copy.ProtocolData = []byte(`{ "a": 1, "b": 2 }`)
			duplicate.Onchain = &copy
			if !appendFixture(t, s, db, duplicate).Duplicate {
				t.Fatal("JSON field order caused conflict")
			}
			result := fact(2, 1, 1, EventTypeSucceeded, "result")
			result.Onchain = resultDetail(family)
			result.Onchain.LedgerSequence = ptr(uint64(math.MaxUint64))
			result.Fees = []ExecutionFee{fee("gas", "gas", "-0.002")}
			result.Fees[0].AssetNamespace = "onchain"
			result.Fees[0].AssetChain = ptr(family)
			result.Fees[0].AssetNetwork = ptr("testnet")
			result.Fees[0].AssetID = "native"
			appendFixture(t, s, db, result)
			d, err = s.SelectOnchainEvidence(t.Context(), db, 1, 1, 2)
			must(t, err)
			if d.LedgerSequence == nil || *d.LedgerSequence != math.MaxUint64 || d.TxPosition == nil || *d.TxPosition != 0 {
				t.Fatal("unsigned position fidelity")
			}
			f := fill(3, 1, 1, "fill", "100")
			f.Onchain = resultDetail(family)
			f.Onchain.LedgerSequence = ptr(uint64(math.MaxUint64))
			f.Onchain.EventPosition = ptr(map[string]string{"evm": "v1/log/0", "solana": "v1/instruction/3/inner/1", "sui": "v1/event/0"}[family])
			appendFixture(t, s, db, f)
			dupe := f
			dupe.Event.ID = 4
			dupe.Event.RecordKey = []byte("wrong-transport-key")
			err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, dupe); return err })
			if !errors.Is(err, ErrConflict) {
				t.Fatal("same event counted twice")
			}
			leg := fill(4, 1, 1, "leg", "100")
			leg.Event.OrderQuantity = ptr("0")
			leg.Onchain = resultDetail(family)
			leg.Onchain.LedgerSequence = ptr(uint64(math.MaxUint64))
			leg.Onchain.EventPosition = ptr(map[string]string{"evm": "v1/log/1", "solana": "v1/instruction/3/inner/2", "sui": "v1/event/1"}[family])
			if appendFixture(t, s, db, leg).State.FilledQuantity != "100" {
				t.Fatal("atomic leg overcount")
			}
			reverse := fact(5, 1, 1, EventTypeReversed, "reorg")
			reverse.Event.ReferenceEventID = ptr(uint64(2))
			reverse.Onchain = resultDetail(family)
			state := appendFixture(t, s, db, reverse).State
			if state.FilledQuantity != "0" || state.Status != OrderStatusPending {
				t.Fatal("reorg failed")
			}
			gas, err := s.ListEventFees(t.Context(), db, 1, 1, 2)
			must(t, err)
			if len(gas) != 1 || gas[0].Amount != "-0.002" {
				t.Fatal("reorg deleted gas")
			}
			// Tx uniqueness applies to acceptance only, across orders on the same network.
			o := testOrder()
			o.ID = 2
			o.IdempotencyKey = []byte("second")
			insertFixture(t, s, db, o)
			other := acceptance(10, 2)
			other.Event.ExecutionID = ptr("exec_two")
			other.Onchain = acceptanceDetail(family)
			err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, other); return err })
			if !errors.Is(err, ErrDuplicate) {
				t.Fatalf("duplicate tx not rejected: %v", err)
			}
			other.Onchain.Network = "other-network"
			appendFixture(t, s, db, other)
		})
	}
}

// TestMySQLRollback verifies child-write failures and caller aborts leave no partial history.
//
// Version:
//   - 2026-09-26: Added.
func TestMySQLRollback(t *testing.T) {
	s, db := mysqlStore(t)
	insertFixture(t, s, db, testOrder())
	appendFixture(t, s, db, acceptance(1, 1))
	f := fill(2, 1, 1, "fill", "30")
	f.Fees = []ExecutionFee{fee("commission", "commission", "0.1")}
	f.Fees[0].ID = 1
	appendFixture(t, s, db, f)
	abort := errors.New("test abort")
	r := fill(3, 1, 1, "abort", "70")
	err := withTx(t.Context(), db, func(tx *sql.Tx) error {
		if _, err := s.AppendOnchainEvent(t.Context(), tx, 1, r); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	r.Event.RecordKey = []byte("fee-failure")
	r.Fees = []ExecutionFee{fee("different", "commission", "0.2")}
	r.Fees[0].ID = 1
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, r); return err })
	if !errors.Is(err, ErrDuplicate) {
		t.Fatal("fee ID collision not detected")
	}
	stored, err := s.SelectOrder(t.Context(), db, 1, 1)
	must(t, err)
	if stored.FilledQuantity != "30" || stored.LastEventSequence != 2 {
		t.Fatal("snapshot survived rollback")
	}
	for _, key := range []string{"abort", "fee-failure"} {
		if _, err := s.SelectEventByKey(t.Context(), db, 1, 1, []byte(key)); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal("history survived rollback")
		}
	}
	o := testOrder()
	o.ID = 2
	o.AccountID = 2
	o.ParentOrderID = ptr(uint64(1))
	o.IdempotencyKey = []byte("child")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.InsertOrder(t.Context(), tx, o); return err })
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-account parent")
	}
}

// TestMySQLConcurrentAppend verifies lock serialization, retry deduplication and current reads.
//
// Version:
//   - 2026-09-26: Added.
func TestMySQLConcurrentAppend(t *testing.T) {
	s, db := mysqlStore(t)
	insertFixture(t, s, db, testOrder())
	appendFixture(t, s, db, acceptance(1, 1))
	f := fill(0, 1, 1, "concurrent", "30")
	var group sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		group.Go(func() {
			errs <- withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, f); return err })
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	stored, err := s.SelectOrder(t.Context(), db, 1, 1)
	must(t, err)
	if stored.FilledQuantity != "30" || stored.LastEventSequence != 2 {
		t.Fatal("concurrent duplicate counted twice")
	}
	tx, err := db.BeginTx(t.Context(), nil)
	must(t, err)
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	_, err = s.SelectOrder(t.Context(), tx, 1, 1)
	must(t, err) // establish old REPEATABLE READ snapshot
	appendFixture(t, s, db, fill(4, 1, 1, "outside", "20"))
	result, err := s.AppendOnchainEvent(t.Context(), tx, 1, fill(5, 1, 1, "inside", "50"))
	must(t, err)
	if result.State.FilledQuantity != "100" {
		t.Fatal("append read stale snapshot")
	}
	must(t, tx.Commit())
	lock, err := db.BeginTx(t.Context(), nil)
	must(t, err)
	defer func() {
		if err := lock.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	}()
	_, err = s.SelectOrderForUpdate(t.Context(), lock, 1, 1)
	must(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	other, err := db.BeginTx(ctx, nil)
	must(t, err)
	_, err = s.SelectOrderForUpdate(ctx, other, 1, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock did not block: %v", err)
	}
	if err := other.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Error(err)
	}
}

// TestSchemaParity verifies that the embedded Store DDL matches the approved TradeHub 001.
//
// Version:
//   - 2026-09-26: Added.
func TestSchemaParity(t *testing.T) {
	path := os.Getenv("K4K3RU_OMS_MIGRATION_PATH")
	if path == "" {
		t.Skip("set K4K3RU_OMS_MIGRATION_PATH for cross-repository DDL comparison")
	}
	data, err := os.ReadFile(path)
	must(t, err)
	sql := string(data)
	start := strings.Index(sql, "-- OMS order/execution")
	if start < 0 {
		t.Fatal("OMS schema marker missing")
	}
	if strings.ReplaceAll(sql[start:], "trade_hub_oms_", "oms_") != reviewedSchema {
		t.Fatal("Store DDL differs from TradeHub 001")
	}
}

// TestMySQLRoutingAndDelayedFees verifies generic routing, independent fee revisions and included costs.
//
// Version:
//   - 2026-09-26: Added.
func TestMySQLRoutingAndDelayedFees(t *testing.T) {
	s, db := mysqlStore(t)
	o := testOrder()
	o.AssetClass = "crypto"
	o.Domain = DomainOnchainAMMPool
	o.Symbol = "USDC/JPY"
	o.Specification = []byte(`{"quantityAsset":"currency:USD","quantityDecimals":2}`)
	insertFixture(t, s, db, o)
	a := acceptance(1, 1)
	a.Event.Venue = ptr("broker-a")
	appendFixture(t, s, db, a)
	b := acceptance(2, 1)
	b.Event.RecordKey = []byte("accept-b")
	b.Event.ExecutionID = ptr("exec_two")
	b.Event.Venue = ptr("broker-b")
	appendFixture(t, s, db, b)
	x := fill(3, 1, 1, "fx-a", "30")
	x.Event.QuantityAssetID = ptr("currency:USD")
	x.Event.CounterAssetID = ptr("currency:JPY")
	x.Event.QuantityDecimals = ptr(uint16(2))
	x.Event.CounterDecimals = ptr(uint16(0))
	x.Event.CounterQuantity = ptr("4500")
	x.Event.FeesComplete = ptr(false)
	x.Event.SourceVersion = ptr(uint64(1))
	appendFixture(t, s, db, x)
	y := fill(4, 1, 2, "fx-b", "70")
	y.Event.ExecutionID = ptr("exec_two")
	y.Event.QuantityAssetID = x.Event.QuantityAssetID
	y.Event.CounterAssetID = x.Event.CounterAssetID
	y.Event.QuantityDecimals = x.Event.QuantityDecimals
	y.Event.CounterDecimals = x.Event.CounterDecimals
	y.Event.CounterQuantity = ptr("10500")
	y.Event.FeesComplete = ptr(true)
	if appendFixture(t, s, db, y).State.Status != OrderStatusFilled {
		t.Fatal("routed FX fills not combined")
	}
	late := fact(5, 1, 1, EventTypeFeesRecorded, "late-fees")
	late.Event.ReferenceEventID = ptr(uint64(3))
	late.Event.SourceVersion = ptr(uint64(1))
	late.Event.FeesComplete = ptr(true)
	commission := fee("broker-commission", "commission", "0.01")
	commission.SourceVersion = ptr(uint64(1))
	commission.AccountingTreatment = "included_in_input"
	tax := fee("tax-jpy", "tax", "1")
	tax.AssetID = "JPY"
	tax.AssetDecimals = 0
	late.Fees = []ExecutionFee{commission, tax}
	result := appendFixture(t, s, db, late)
	if result.State.FilledQuantity != "100" {
		t.Fatal("fees affected quantity")
	}
	row, err := s.SelectEventByKey(t.Context(), db, 1, 1, []byte("fx-a"))
	must(t, err)
	if row.FeesComplete == nil || *row.FeesComplete {
		t.Fatal("original fee assertion overwritten")
	}
	costs, err := s.ListEventFees(t.Context(), db, 1, 1, 5)
	must(t, err)
	var original uint64
	for _, f := range costs {
		if f.FeeType == "commission" {
			original = f.ID
			if f.AccountingTreatment != "included_in_input" {
				t.Fatal("inclusion lost")
			}
		}
	}
	adjustment := fact(6, 1, 1, EventTypeFeesAdjusted, "fee-adjusted")
	adjustment.Event.ReferenceEventID = ptr(uint64(3))
	delta := commission
	delta.ID = 0
	delta.RecordKey = []byte("commission-revision-2")
	delta.Amount = "-0.005"
	delta.SourceVersion = ptr(uint64(2))
	delta.AdjustmentOfFeeID = &original
	adjustment.Fees = []ExecutionFee{delta}
	appendFixture(t, s, db, adjustment)
	// An old revision cannot be added again using another record key.
	stale := adjustment
	stale.Event.ID = 7
	stale.Event.RecordKey = []byte("stale")
	stale.Fees = append([]ExecutionFee(nil), adjustment.Fees...)
	stale.Fees[0].RecordKey = []byte("stale-fee")
	stale.Fees[0].SourceVersion = ptr(uint64(1))
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, stale); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("stale fee adjustment accepted")
	}
	// Adjustments cannot move a cost to another submission.
	wrong := adjustment
	wrong.Event.ID = 7
	wrong.Event.RecordKey = []byte("wrong-owner")
	wrong.Event.SubmissionEventID = ptr(uint64(2))
	wrong.Event.ExecutionID = ptr("exec_two")
	wrong.Event.ReferenceEventID = ptr(uint64(4))
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, wrong); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("fee moved to another submission")
	}
}

// TestMySQLSuiAmounts verifies the earlier testnet amounts without sending a transaction.
//
// Version:
//   - 2026-09-26: Added.
func TestMySQLSuiAmounts(t *testing.T) {
	s, db := mysqlStore(t)
	o := testOrder()
	o.Quantity = ptr("0.1")
	insertFixture(t, s, db, o)
	a := acceptance(1, 1)
	a.Onchain = acceptanceDetail("sui")
	appendFixture(t, s, db, a)
	result := fact(2, 1, 1, EventTypeSucceeded, "result")
	result.Onchain = resultDetail("sui")
	result.Onchain.LedgerSequence = ptr(uint64(387909055))
	gas := fee("net-gas", "gas", "0.002619432")
	gas.AssetNamespace = "onchain"
	gas.AssetChain = ptr("sui")
	gas.AssetNetwork = ptr("testnet")
	gas.AssetID = "0x2::sui::SUI"
	gas.AssetDecimals = 9
	result.Fees = []ExecutionFee{gas}
	appendFixture(t, s, db, result)
	f := fill(3, 1, 1, "fill", "0.1")
	f.Event.CounterQuantity = ptr("0.421965")
	f.Onchain = resultDetail("sui")
	f.Onchain.LedgerSequence = result.Onchain.LedgerSequence
	f.Onchain.EventPosition = ptr("v1/event/0")
	got := appendFixture(t, s, db, f)
	if got.State.Status != OrderStatusFilled || got.State.FilledQuantity != "0.1" {
		t.Fatal("Sui units lost")
	}
	stored, err := s.SelectEventByKey(t.Context(), db, 1, 1, []byte("fill"))
	must(t, err)
	if *stored.CounterQuantity != "0.421965" {
		t.Fatal("USDC units lost")
	}
	fees, err := s.ListEventFees(t.Context(), db, 1, 1, 2)
	must(t, err)
	if fees[0].Amount != "0.002619432" {
		t.Fatal("gas precision lost")
	}
}

// TestMySQLLedgerAndReferenceConflicts verifies ownership and receipt/fill provenance together.
//
// Version:
//   - 2026-09-26: Added.
func TestMySQLLedgerAndReferenceConflicts(t *testing.T) {
	s, db := mysqlStore(t)
	insertFixture(t, s, db, testOrder())
	a := acceptance(1, 1)
	a.Onchain = acceptanceDetail("evm")
	appendFixture(t, s, db, a)
	result := fact(2, 1, 1, EventTypeSucceeded, "result")
	result.Onchain = resultDetail("evm")
	appendFixture(t, s, db, result)
	f := fill(3, 1, 1, "fill", "30")
	f.Onchain = resultDetail("evm")
	f.Onchain.EventPosition = ptr("v1/log/0")
	for name, change := range map[string]func(*OnchainEvent){
		"ledger":         func(r *OnchainEvent) { r.Onchain.LedgerSequence = ptr(uint64(1)) },
		"tx":             func(r *OnchainEvent) { r.Onchain.TxID = "different" },
		"submission":     func(r *OnchainEvent) { r.Event.SubmissionEventID = ptr(uint64(2)) },
		"public_id":      func(r *OnchainEvent) { r.Event.ExecutionID = ptr("other") },
		"missing_detail": func(r *OnchainEvent) { r.Onchain = nil },
	} {
		t.Run(name, func(t *testing.T) {
			bad := f
			d := *f.Onchain
			bad.Onchain = &d
			change(&bad)
			err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, bad); return err })
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("invalid evidence accepted: %v", err)
			}
		})
	}
	appendFixture(t, s, db, f)
	correction := fill(4, 1, 1, "correction", "20")
	correction.Event.EventType = EventTypeFillCorrected
	correction.Event.ReferenceEventID = ptr(uint64(3))
	d := *f.Onchain
	correction.Onchain = &d
	if appendFixture(t, s, db, correction).State.FilledQuantity != "20" {
		t.Fatal("onchain correction double counted")
	}
	// Adding a previously missing ledger ID is not a new fill.
	duplicate := f
	duplicate.Event.ID = 5
	duplicate.Event.RecordKey = []byte("duplicate")
	d = *f.Onchain
	d.LedgerID = ptr("block-hash")
	duplicate.Onchain = &d
	err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, duplicate); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("incomplete evidence bypassed duplicate check")
	}
	o := testOrder()
	o.ID = 2
	o.IdempotencyKey = []byte("second")
	insertFixture(t, s, db, o)
	wrong := fill(10, 2, 1, "cross-order", "1")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendOnchainEvent(t.Context(), tx, 1, wrong); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("cross-order submission accepted")
	}
}
