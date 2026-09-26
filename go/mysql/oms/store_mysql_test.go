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

func mysqlStore(t *testing.T) (*Store, *sql.DB) {
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
	must(t, err)
	must(t, s.CreateTables(t.Context(), db))
	t.Cleanup(func() {
		for _, name := range []string{s.feeTable, s.onchainDetailTable, s.executionTable, s.orderTable} {
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
func appendFixture(t *testing.T, s *Store, db *sql.DB, r ExecutionRecord) *AppendResult {
	t.Helper()
	var result *AppendResult
	must(t, withTx(t.Context(), db, func(tx *sql.Tx) error {
		var err error
		result, err = s.AppendExecution(t.Context(), tx, 1, r)
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
	if !duplicate.Duplicate || duplicate.ExecutionID != 2 || duplicate.Sequence != 2 {
		t.Fatal("not idempotent")
	}
	mismatch := f1
	mismatch.Execution.Quantity = ptr("31")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, mismatch); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("same key overwritten")
	}
	changedFee := f1
	changedFee.Fees = append([]ExecutionFee(nil), f1.Fees...)
	changedFee.Fees[0].Amount = "0.3"
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, changedFee); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("fee mutation accepted")
	}
	f2 := fill(3, 1, 1, "fill-b", "70")
	done := appendFixture(t, s, db, f2)
	if done.State.Status != OrderStatusFilled || done.State.CompletedAt == nil {
		t.Fatal("not filled")
	}
	correction := fill(4, 1, 1, "correction", "20")
	correction.Execution.ExecType = ExecutionTypeFillCorrected
	correction.Execution.ReferenceRecordID = ptr(uint64(2))
	corrected := appendFixture(t, s, db, correction)
	if corrected.State.FilledQuantity != "90" || corrected.State.Status != OrderStatusPartiallyFilled || corrected.State.CompletedAt != nil {
		t.Fatal("correction not projected")
	}
	original, err := s.SelectExecutionByKey(t.Context(), db, 1, 1, []byte("fill-a"))
	must(t, err)
	if *original.Quantity != "30" {
		t.Fatal("original fill changed")
	}
	fees, err := s.ListExecutionFees(t.Context(), db, 1, 1, 2)
	must(t, err)
	if len(fees) != 2 {
		t.Fatal("multiple fees missing")
	}
	adjustment := fact(5, 1, 1, ExecutionTypeFeesAdjusted, "adjust")
	adjustment.Execution.ReferenceRecordID = ptr(uint64(2))
	adjustment.Fees = []ExecutionFee{fee("refund", "commission", "-0.05")}
	var feeID uint64
	for _, f := range fees {
		if f.FeeType == "commission" {
			feeID = f.ID
		}
	}
	adjustment.Fees[0].AdjustmentOfFeeID = &feeID
	appendFixture(t, s, db, adjustment)
	originalFees, err := s.ListExecutionFees(t.Context(), db, 1, 1, 2)
	must(t, err)
	if len(originalFees) != 2 || originalFees[0].Amount != fees[0].Amount {
		t.Fatal("original fees changed")
	}
	adjustments, err := s.ListExecutionFees(t.Context(), db, 1, 1, 5)
	must(t, err)
	if len(adjustments) != 1 || adjustments[0].Amount != "-0.05" {
		t.Fatal("adjustment lost")
	}
	page, err := s.ListExecutions(t.Context(), db, 1, 1, 0, 2)
	must(t, err)
	tail, err := s.ListExecutions(t.Context(), db, 1, 1, page[1].Sequence, 200)
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
		func() error { _, err := s.SelectExecutionByKey(t.Context(), db, 2, 1, []byte("fill-a")); return err },
		func() error { _, err := s.ListExecutions(t.Context(), db, 2, 1, 0, 20); return err },
		func() error { _, err := s.ListExecutionFees(t.Context(), db, 2, 1, 2); return err },
	} {
		if !errors.Is(read(), sql.ErrNoRows) {
			t.Fatal("cross-account history read")
		}
	}
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 2, f1); return err })
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
			d, err := s.SelectOnchainDetail(t.Context(), db, 1, 1, 1)
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
			result := fact(2, 1, 1, ExecutionTypeSucceeded, "result")
			result.Onchain = resultDetail(family)
			result.Onchain.LedgerSequence = ptr(uint64(math.MaxUint64))
			result.Fees = []ExecutionFee{fee("gas", "gas", "-0.002")}
			result.Fees[0].AssetNamespace = "onchain"
			result.Fees[0].AssetChain = ptr(family)
			result.Fees[0].AssetNetwork = ptr("testnet")
			result.Fees[0].AssetID = "native"
			appendFixture(t, s, db, result)
			d, err = s.SelectOnchainDetail(t.Context(), db, 1, 1, 2)
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
			dupe.Execution.ID = 4
			dupe.Execution.RecordKey = []byte("wrong-transport-key")
			err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, dupe); return err })
			if !errors.Is(err, ErrConflict) {
				t.Fatal("same event counted twice")
			}
			leg := fill(4, 1, 1, "leg", "100")
			leg.Execution.OrderQuantity = ptr("0")
			leg.Onchain = resultDetail(family)
			leg.Onchain.LedgerSequence = ptr(uint64(math.MaxUint64))
			leg.Onchain.EventPosition = ptr(map[string]string{"evm": "v1/log/1", "solana": "v1/instruction/3/inner/2", "sui": "v1/event/1"}[family])
			if appendFixture(t, s, db, leg).State.FilledQuantity != "100" {
				t.Fatal("atomic leg overcount")
			}
			reverse := fact(5, 1, 1, ExecutionTypeReversed, "reorg")
			reverse.Execution.ReferenceRecordID = ptr(uint64(2))
			reverse.Onchain = resultDetail(family)
			state := appendFixture(t, s, db, reverse).State
			if state.FilledQuantity != "0" || state.Status != OrderStatusPending {
				t.Fatal("reorg failed")
			}
			gas, err := s.ListExecutionFees(t.Context(), db, 1, 1, 2)
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
			other.Execution.ExecutionID = ptr("exec_two")
			other.Onchain = acceptanceDetail(family)
			err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, other); return err })
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
		if _, err := s.AppendExecution(t.Context(), tx, 1, r); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	r.Execution.RecordKey = []byte("fee-failure")
	r.Fees = []ExecutionFee{fee("different", "commission", "0.2")}
	r.Fees[0].ID = 1
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, r); return err })
	if !errors.Is(err, ErrDuplicate) {
		t.Fatal("fee ID collision not detected")
	}
	stored, err := s.SelectOrder(t.Context(), db, 1, 1)
	must(t, err)
	if stored.FilledQuantity != "30" || stored.LastExecutionSequence != 2 {
		t.Fatal("snapshot survived rollback")
	}
	for _, key := range []string{"abort", "fee-failure"} {
		if _, err := s.SelectExecutionByKey(t.Context(), db, 1, 1, []byte(key)); !errors.Is(err, sql.ErrNoRows) {
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
			errs <- withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, f); return err })
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	stored, err := s.SelectOrder(t.Context(), db, 1, 1)
	must(t, err)
	if stored.FilledQuantity != "30" || stored.LastExecutionSequence != 2 {
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
	result, err := s.AppendExecution(t.Context(), tx, 1, fill(5, 1, 1, "inside", "50"))
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
	start := strings.Index(sql, "-- OMS snapshot")
	if start < 0 {
		t.Fatal("OMS schema marker missing")
	}
	if strings.ReplaceAll(sql[start:], "trade_hub_oms_", "oms_") != reviewedSchema {
		t.Fatal("Store DDL differs from TradeHub 001")
	}
}

// TestMySQLFXAndDelayedFees verifies generic routing, independent fee revisions and included costs.
//
// Version:
//   - 2026-09-26: Added.
func TestMySQLFXAndDelayedFees(t *testing.T) {
	s, db := mysqlStore(t)
	o := testOrder()
	o.AssetClass = "fx"
	o.Domain = "spot"
	o.Symbol = "USD/JPY"
	o.Specification = []byte(`{"quantityAsset":"currency:USD","quantityDecimals":2}`)
	insertFixture(t, s, db, o)
	a := acceptance(1, 1)
	a.Execution.Venue = ptr("broker-a")
	appendFixture(t, s, db, a)
	b := acceptance(2, 1)
	b.Execution.RecordKey = []byte("accept-b")
	b.Execution.ExecutionID = ptr("exec_two")
	b.Execution.Venue = ptr("broker-b")
	appendFixture(t, s, db, b)
	x := fill(3, 1, 1, "fx-a", "30")
	x.Execution.QuantityAssetID = ptr("currency:USD")
	x.Execution.CounterAssetID = ptr("currency:JPY")
	x.Execution.QuantityDecimals = ptr(uint16(2))
	x.Execution.CounterDecimals = ptr(uint16(0))
	x.Execution.CounterQuantity = ptr("4500")
	x.Execution.FeesComplete = ptr(false)
	x.Execution.SourceVersion = ptr(uint64(1))
	appendFixture(t, s, db, x)
	y := fill(4, 1, 2, "fx-b", "70")
	y.Execution.ExecutionID = ptr("exec_two")
	y.Execution.QuantityAssetID = x.Execution.QuantityAssetID
	y.Execution.CounterAssetID = x.Execution.CounterAssetID
	y.Execution.QuantityDecimals = x.Execution.QuantityDecimals
	y.Execution.CounterDecimals = x.Execution.CounterDecimals
	y.Execution.CounterQuantity = ptr("10500")
	y.Execution.FeesComplete = ptr(true)
	if appendFixture(t, s, db, y).State.Status != OrderStatusFilled {
		t.Fatal("routed FX fills not combined")
	}
	late := fact(5, 1, 1, ExecutionTypeFeesRecorded, "late-fees")
	late.Execution.ReferenceRecordID = ptr(uint64(3))
	late.Execution.SourceVersion = ptr(uint64(1))
	late.Execution.FeesComplete = ptr(true)
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
	row, err := s.SelectExecutionByKey(t.Context(), db, 1, 1, []byte("fx-a"))
	must(t, err)
	if row.FeesComplete == nil || *row.FeesComplete {
		t.Fatal("original fee assertion overwritten")
	}
	costs, err := s.ListExecutionFees(t.Context(), db, 1, 1, 5)
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
	adjustment := fact(6, 1, 1, ExecutionTypeFeesAdjusted, "fee-adjusted")
	adjustment.Execution.ReferenceRecordID = ptr(uint64(3))
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
	stale.Execution.ID = 7
	stale.Execution.RecordKey = []byte("stale")
	stale.Fees = append([]ExecutionFee(nil), adjustment.Fees...)
	stale.Fees[0].RecordKey = []byte("stale-fee")
	stale.Fees[0].SourceVersion = ptr(uint64(1))
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, stale); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("stale fee adjustment accepted")
	}
	// Adjustments cannot move a cost to another submission.
	wrong := adjustment
	wrong.Execution.ID = 7
	wrong.Execution.RecordKey = []byte("wrong-owner")
	wrong.Execution.SubmissionRecordID = ptr(uint64(2))
	wrong.Execution.ExecutionID = ptr("exec_two")
	wrong.Execution.ReferenceRecordID = ptr(uint64(4))
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, wrong); return err })
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
	result := fact(2, 1, 1, ExecutionTypeSucceeded, "result")
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
	f.Execution.CounterQuantity = ptr("0.421965")
	f.Onchain = resultDetail("sui")
	f.Onchain.LedgerSequence = result.Onchain.LedgerSequence
	f.Onchain.EventPosition = ptr("v1/event/0")
	got := appendFixture(t, s, db, f)
	if got.State.Status != OrderStatusFilled || got.State.FilledQuantity != "0.1" {
		t.Fatal("Sui units lost")
	}
	stored, err := s.SelectExecutionByKey(t.Context(), db, 1, 1, []byte("fill"))
	must(t, err)
	if *stored.CounterQuantity != "0.421965" {
		t.Fatal("USDC units lost")
	}
	fees, err := s.ListExecutionFees(t.Context(), db, 1, 1, 2)
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
	result := fact(2, 1, 1, ExecutionTypeSucceeded, "result")
	result.Onchain = resultDetail("evm")
	appendFixture(t, s, db, result)
	f := fill(3, 1, 1, "fill", "30")
	f.Onchain = resultDetail("evm")
	f.Onchain.EventPosition = ptr("v1/log/0")
	for name, change := range map[string]func(*ExecutionRecord){
		"ledger":         func(r *ExecutionRecord) { r.Onchain.LedgerSequence = ptr(uint64(1)) },
		"tx":             func(r *ExecutionRecord) { r.Onchain.TxID = "different" },
		"submission":     func(r *ExecutionRecord) { r.Execution.SubmissionRecordID = ptr(uint64(2)) },
		"public_id":      func(r *ExecutionRecord) { r.Execution.ExecutionID = ptr("other") },
		"missing_detail": func(r *ExecutionRecord) { r.Onchain = nil },
	} {
		t.Run(name, func(t *testing.T) {
			bad := f
			d := *f.Onchain
			bad.Onchain = &d
			change(&bad)
			err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, bad); return err })
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("invalid evidence accepted: %v", err)
			}
		})
	}
	appendFixture(t, s, db, f)
	correction := fill(4, 1, 1, "correction", "20")
	correction.Execution.ExecType = ExecutionTypeFillCorrected
	correction.Execution.ReferenceRecordID = ptr(uint64(3))
	d := *f.Onchain
	correction.Onchain = &d
	if appendFixture(t, s, db, correction).State.FilledQuantity != "20" {
		t.Fatal("onchain correction double counted")
	}
	// Adding a previously missing ledger ID is not a new fill.
	duplicate := f
	duplicate.Execution.ID = 5
	duplicate.Execution.RecordKey = []byte("duplicate")
	d = *f.Onchain
	d.LedgerID = ptr("block-hash")
	duplicate.Onchain = &d
	err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, duplicate); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("incomplete evidence bypassed duplicate check")
	}
	o := testOrder()
	o.ID = 2
	o.IdempotencyKey = []byte("second")
	insertFixture(t, s, db, o)
	wrong := fill(10, 2, 1, "cross-order", "1")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendExecution(t.Context(), tx, 1, wrong); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("cross-order submission accepted")
	}
}
