package oms

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func ensurePnLFixture(t *testing.T, s *Store, db *sql.DB, accountID uint64, scope PnLScope) *PnL {
	t.Helper()
	var v *PnL
	must(t, withTx(t.Context(), db, func(tx *sql.Tx) error {
		var err error
		v, err = s.EnsurePnL(t.Context(), tx, accountID, scope)
		return err
	}))
	return v
}

func readPnLFixture(t *testing.T, s *Store, db *sql.DB, id uint64) *PnL {
	t.Helper()
	v, err := s.SelectPnL(t.Context(), db, 1, id)
	must(t, err)
	return v
}

func pnlOrderFixture(t *testing.T, s *Store, db *sql.DB, id uint64, complete bool) {
	t.Helper()
	o := testOrder()
	o.ID, o.IdempotencyKey = id, []byte(fmt.Sprintf("order-%d", id))
	insertFixture(t, s, db, o)
	if !complete {
		return
	}
	pnlCompleteFixture(t, s, db, id)
}

func pnlCompleteFixture(t *testing.T, s *Store, db *sql.DB, id uint64) {
	t.Helper()
	root := id * 10
	publicID := ptr(fmt.Sprintf("exec-%d", id))
	a := acceptance(root, id)
	a.Event.ExecutionID = publicID
	appendFixture(t, s, db, a)
	f := fill(root+1, id, root, "full", "100")
	f.Event.ExecutionID, f.Event.FeesComplete = publicID, ptr(true)
	appendFixture(t, s, db, f)
	r := fact(root+2, id, root, EventTypeSucceeded, "result")
	r.Event.ExecutionID, r.Event.FeesComplete = publicID, ptr(true)
	appendFixture(t, s, db, r)
}

func savePnLFixture(t *testing.T, s *Store, db *sql.DB, id, last uint64, rebuild bool) *PnL {
	t.Helper()
	v := readPnLFixture(t, s, db, id)
	must(t, withTx(t.Context(), db, func(tx *sql.Tx) error {
		return s.SavePnL(t.Context(), tx, 1, id, v.Version, testPnLCheckpoint(last), rebuild)
	}))
	return readPnLFixture(t, s, db, id)
}

// TestMySQLPnLLifecycle verifies idempotent registration, exact state, ownership and atomic rollback.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLLifecycle(t *testing.T) {
	s, db := mysqlStore(t, true)
	v := ensurePnLFixture(t, s, db, 1, testPnLScope())
	if v.Version != 1 || !v.NeedsRebuild || v.LastOrderID != nil || v.RealizedPnL != nil || !v.CalculatedAt.IsZero() {
		t.Fatal("uncalculated values reported as known")
	}
	duplicate := ensurePnLFixture(t, s, db, 1, testPnLScope())
	if duplicate.ID != v.ID || duplicate.Version != v.Version {
		t.Fatal("ensure replaced state")
	}
	scope := testPnLScope()
	scope.AccountingAsset.AssetID = "other::usdc::USDC"
	err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, e := s.EnsurePnL(t.Context(), tx, 1, scope); return e })
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("valuation unit changed: %v", err)
	}
	scope = testPnLScope()
	scope.InventoryAsset.AssetID = "another::sui::SUI"
	otherAsset := ensurePnLFixture(t, s, db, 1, scope)
	if otherAsset.ID == v.ID {
		t.Fatal("same symbol merged different assets")
	}
	if _, err := s.SelectPnL(t.Context(), db, 2, v.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-account read")
	}
	pnlOrderFixture(t, s, db, 10, true)
	v = savePnLFixture(t, s, db, v.ID, 10, true)
	if v.NeedsRebuild || *v.LastOrderID != 10 || *v.RemainingCost != "30" || *v.RealizedPnL != "4" {
		t.Fatal("checkpoint not persisted")
	}
	var state map[string]json.RawMessage
	must(t, json.Unmarshal(v.CalculationState, &state))
	if len(state["economicBoundary"]) == 0 {
		t.Fatal("restart state lost")
	}
	err = withTx(t.Context(), db, func(tx *sql.Tx) error {
		return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version-1, testPnLCheckpoint(10), false)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("old publication accepted")
	}
	abort := errors.New("fixture abort")
	err = withTx(t.Context(), db, func(tx *sql.Tx) error {
		changed := testPnLCheckpoint(10)
		changed.RemainingCost = ptr("31")
		if err := s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, changed, false); err != nil {
			return err
		}
		if err := s.InvalidatePnL(t.Context(), tx, 1, v.ID); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if got := readPnLFixture(t, s, db, v.ID); got.Version != v.Version || got.NeedsRebuild || *got.RemainingCost != "30" {
		t.Fatal("rollback invalidated state")
	}
	// A separately composed store resumes persisted values without process-local state.
	fresh, err := NewStoreWithPnL(s.orderTable, s.executionTable, s.onchainEventTable, s.feeTable, s.pnlTable)
	must(t, err)
	page, err := fresh.ListPnL(t.Context(), db, 1, "wallet", 0, 1)
	must(t, err)
	if len(page) != 1 || page[0].ID != v.ID {
		t.Fatal("first page")
	}
	page, err = fresh.ListPnL(t.Context(), db, 1, "wallet", v.ID, 1)
	must(t, err)
	if len(page) != 1 || page[0].ID != otherAsset.ID {
		t.Fatal("next page")
	}
}

// TestMySQLPnLAdditive verifies that adding the fifth table preserves populated OMS tables.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLAdditive(t *testing.T) {
	legacy, db := mysqlStore(t)
	pnlOrderFixture(t, legacy, db, 10, true)
	before, err := legacy.SelectOrder(t.Context(), db, 1, 10)
	must(t, err)
	name := legacy.orderTable + "_pnl"
	_, ddl, found := strings.Cut(reviewedSchema, "CREATE TABLE oms_pnl (")
	if !found {
		t.Fatal("missing additive table")
	}
	ddl = strings.NewReplacer("oms_pnl", name, "REFERENCES oms_orders", "REFERENCES "+quoted(legacy.orderTable)).Replace("CREATE TABLE oms_pnl (" + ddl)
	// Do not rewrite constraint names with a potentially 64-character table name.
	ddl = strings.ReplaceAll(ddl, "fk_"+name+"_", "fk_pnl_additive_")
	ddl = strings.ReplaceAll(ddl, "ck_"+name+"_", "ck_pnl_additive_")
	_, err = db.ExecContext(t.Context(), ddl)
	must(t, err)
	t.Cleanup(func() {
		if _, err := db.Exec("DROP TABLE " + quoted(name)); err != nil {
			t.Error(err)
		}
	})
	s, err := NewStoreWithPnL(legacy.orderTable, legacy.executionTable, legacy.onchainEventTable, legacy.feeTable, name)
	must(t, err)
	ensurePnLFixture(t, s, db, 1, testPnLScope())
	after, err := s.SelectOrder(t.Context(), db, 1, 10)
	must(t, err)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("adding PnL changed existing order")
	}
	var events int
	must(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+quoted(s.onchainEventTable)).Scan(&events))
	if events != 3 {
		t.Fatal("adding PnL changed existing events")
	}
}

// TestMySQLPnLIncompleteExecution verifies finality and fee readiness before advancing a prefix.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLIncompleteExecution(t *testing.T) {
	s, db := mysqlStore(t, true)
	v := ensurePnLFixture(t, s, db, 1, testPnLScope())
	insertFixture(t, s, db, testOrder())
	appendFixture(t, s, db, acceptance(1, 1))
	f := fill(2, 1, 1, "full", "100")
	f.Event.FeesComplete = ptr(true)
	appendFixture(t, s, db, f)
	checkBlocked := func() {
		t.Helper()
		v = readPnLFixture(t, s, db, v.ID)
		err := withTx(t.Context(), db, func(tx *sql.Tx) error {
			return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, testPnLCheckpoint(1), true)
		})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("incomplete execution passed checkpoint: %v", err)
		}
	}
	checkBlocked()
	r := fact(3, 1, 1, EventTypeSucceeded, "result")
	r.Event.FeesComplete = ptr(false)
	appendFixture(t, s, db, r)
	checkBlocked()
	fees := fact(4, 1, 1, EventTypeFeesRecorded, "fees-done")
	fees.Event.ReferenceEventID, fees.Event.FeesComplete = ptr(uint64(3)), ptr(true)
	appendFixture(t, s, db, fees)
	savePnLFixture(t, s, db, v.ID, 1, true)
}

// TestMySQLPnLPrefix verifies unfinished-order blocking and re-evaluation of the whole tail.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLPrefix(t *testing.T) {
	s, db := mysqlStore(t, true)
	v := ensurePnLFixture(t, s, db, 1, testPnLScope())
	pnlOrderFixture(t, s, db, 100, true)
	pnlOrderFixture(t, s, db, 101, false)
	pnlOrderFixture(t, s, db, 102, true)
	v = savePnLFixture(t, s, db, v.ID, 100, true)
	err := withTx(t.Context(), db, func(tx *sql.Tx) error {
		orders, err := s.ListPnLOrders(t.Context(), tx, 1, v.ID, 100, 10)
		if err != nil {
			return err
		}
		if len(orders) != 2 || orders[0].ID != 101 || orders[1].ID != 102 {
			return errors.New("tail omitted unfinished order")
		}
		return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, testPnLCheckpoint(102), false)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("skipped pending order: %v", err)
	}
	if got := readPnLFixture(t, s, db, v.ID); *got.LastOrderID != 100 || got.Version != v.Version {
		t.Fatal("failed publication changed prefix")
	}
	pnlCompleteFixture(t, s, db, 101)
	v = readPnLFixture(t, s, db, v.ID)
	if v.NeedsRebuild {
		t.Fatal("new tail invalidated reusable prefix")
	}
	v = savePnLFixture(t, s, db, v.ID, 102, false)
	// A lower ID committed later must invalidate the complete prefix.
	pnlOrderFixture(t, s, db, 99, false)
	v = readPnLFixture(t, s, db, v.ID)
	if !v.NeedsRebuild {
		t.Fatal("late lower-ID order was missed")
	}
	err = withTx(t.Context(), db, func(tx *sql.Tx) error {
		return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, testPnLCheckpoint(102), false)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("invalid prefix reused")
	}
	// A rewind rebuild retains no partially applied orders.
	v = savePnLFixture(t, s, db, v.ID, 0, true)
	if v.LastOrderID != nil || v.NeedsRebuild {
		t.Fatal("rewind failed")
	}
}

// TestMySQLPnLSourceInvalidation verifies fees, duplicate delivery and source rollback.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLSourceInvalidation(t *testing.T) {
	s, db := mysqlStore(t, true)
	v := ensurePnLFixture(t, s, db, 1, testPnLScope())
	pnlOrderFixture(t, s, db, 10, true)
	v = savePnLFixture(t, s, db, v.ID, 10, true)
	r := fact(103, 10, 100, EventTypeFeesRecorded, "late-fee")
	r.Event.ExecutionID, r.Event.ReferenceEventID, r.Event.FeesComplete = ptr("exec-10"), ptr(uint64(102)), ptr(true)
	r.Fees = []ExecutionFee{fee("late-gas", "gas", "0.001")}
	r.Fees[0].AssetNamespace, r.Fees[0].AssetChain, r.Fees[0].AssetNetwork = "onchain", ptr("sui"), ptr("testnet")
	r.Fees[0].AssetID, r.Fees[0].AssetDecimals = "0x2::sui::SUI", 9
	abort := errors.New("fixture abort")
	err := withTx(t.Context(), db, func(tx *sql.Tx) error {
		if _, err := s.AppendOnchainEvent(t.Context(), tx, 1, r); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if got := readPnLFixture(t, s, db, v.ID); got.Version != v.Version || got.NeedsRebuild {
		t.Fatal("uncommitted source changed checkpoint")
	}
	// Keep an older consistent view alive while the source transaction commits.
	staleTx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	must(t, err)
	t.Cleanup(func() {
		if err := staleTx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	_, err = s.SelectPnL(t.Context(), staleTx, 1, v.ID)
	must(t, err)
	appendFixture(t, s, db, r)
	err = s.SavePnL(t.Context(), staleTx, 1, v.ID, v.Version, testPnLCheckpoint(10), true)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conditional update ignored concurrent source write: %v", err)
	}
	must(t, staleTx.Rollback())
	got := readPnLFixture(t, s, db, v.ID)
	if !got.NeedsRebuild || got.Version != v.Version+1 {
		t.Fatal("late fee not invalidated")
	}
	if !appendFixture(t, s, db, r).Duplicate {
		t.Fatal("duplicate fixture")
	}
	if again := readPnLFixture(t, s, db, v.ID); again.Version != got.Version {
		t.Fatal("duplicate advanced version")
	}
	err = withTx(t.Context(), db, func(tx *sql.Tx) error {
		return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, testPnLCheckpoint(10), true)
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("stale rebuild overwrote late fee")
	}
	other := testOrder()
	other.ID, other.AccountID, other.IdempotencyKey = 50, 2, []byte("other-account")
	insertFixture(t, s, db, other)
	if again := readPnLFixture(t, s, db, v.ID); again.Version != got.Version {
		t.Fatal("other account invalidated checkpoint")
	}
}

// TestMySQLPnLPositionAndConstraints verifies representative ownership and database shape constraints.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLPositionAndConstraints(t *testing.T) {
	s, db := mysqlStore(t, true)
	root := testOrder()
	root.ID, root.PositionOrderID = 10, ptr(uint64(10))
	insertFixture(t, s, db, root)
	pnlCompleteFixture(t, s, db, 10)
	scope := testPnLScope()
	scope.SubjectType, scope.PositionOrderID, scope.InventoryAsset = PnLSubjectPositionGroup, &root.ID, nil
	v := ensurePnLFixture(t, s, db, 1, scope)
	checkpoint := testPnLCheckpoint(10)
	checkpoint.RemainingCost, checkpoint.AverageEntryPrice = nil, ptr("5")
	checkpoint.CalculationMethod = "linear_position"
	must(t, withTx(t.Context(), db, func(tx *sql.Tx) error { return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, checkpoint, true) }))
	got := readPnLFixture(t, s, db, v.ID)
	if got.RemainingCost != nil || *got.AverageEntryPrice != "5" || *got.PositionOrderID != 10 {
		t.Fatal("position values lost")
	}
	err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.EnsurePnL(t.Context(), tx, 2, scope); return err })
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-account position accepted")
	}
	pnlOrderFixture(t, s, db, 20, true)
	nonRoot := scope
	nonRoot.PositionOrderID = ptr(uint64(20))
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.EnsurePnL(t.Context(), tx, 1, nonRoot); return err })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("unlinked order treated as position root")
	}
	got = readPnLFixture(t, s, db, v.ID)
	checkpoint.LastOrderID = ptr(uint64(20))
	err = withTx(t.Context(), db, func(tx *sql.Tx) error { return s.SavePnL(t.Context(), tx, 1, v.ID, got.Version, checkpoint, true) })
	if !errors.Is(err, ErrConflict) {
		t.Fatal("another group's boundary accepted")
	}
	for _, tt := range []struct {
		set  string
		code uint16
	}{
		{"account_id=2", 1452}, {"last_order_id=999", 1452}, {"position_order_id=NULL", 3819},
		{"version=0", 3819}, {"needs_rebuild=2", 3819}, {"definition='[]'", 3819},
		{"calculation_state='[]'", 3819}, {"remaining_cost='5'", 3819}, {"calculated_at=NULL", 3819},
	} {
		_, err := db.ExecContext(t.Context(), "UPDATE "+quoted(s.pnlTable)+" SET "+tt.set+" WHERE id=?", v.ID)
		var dbErr *mysql.MySQLError
		if !errors.As(err, &dbErr) || dbErr.Number != tt.code {
			t.Fatalf("constraint %s: expected %d got %v", tt.set, tt.code, err)
		}
	}
}

// TestMySQLPnLJSONRoundTrip verifies compact size limits and exact decimal text after MySQL normalization.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLJSONRoundTrip(t *testing.T) {
	s, db := mysqlStore(t, true)
	v := ensurePnLFixture(t, s, db, 1, testPnLScope())
	pnlOrderFixture(t, s, db, 10, true)
	v = readPnLFixture(t, s, db, v.ID)
	state := make(map[string]string, 5201)
	for i := range 5200 {
		state[fmt.Sprintf("%d", i)] = "0"
	}
	state["exact"] = "90071992547409931234567890123456789/7"
	checkpoint := testPnLCheckpoint(10)
	var err error
	checkpoint.CalculationState, err = json.Marshal(state)
	must(t, err)
	must(t, withTx(t.Context(), db, func(tx *sql.Tx) error { return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, checkpoint, true) }))
	got := readPnLFixture(t, s, db, v.ID)
	var restored map[string]string
	must(t, json.Unmarshal(got.CalculationState, &restored))
	if !reflect.DeepEqual(state, restored) {
		t.Fatal("restart state changed across database normalization")
	}
}

// TestMySQLPnLConcurrentPublish verifies that a shared expected version permits only one writer.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLPnLConcurrentPublish(t *testing.T) {
	s, db := mysqlStore(t, true)
	v := ensurePnLFixture(t, s, db, 1, testPnLScope())
	pnlOrderFixture(t, s, db, 10, true)
	v = readPnLFixture(t, s, db, v.ID)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- withTx(t.Context(), db, func(tx *sql.Tx) error {
				return s.SavePnL(t.Context(), tx, 1, v.ID, v.Version, testPnLCheckpoint(10), true)
			})
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	succeeded, conflicted := 0, 0
	for err := range results {
		if err == nil {
			succeeded++
		} else if errors.Is(err, ErrConflict) {
			conflicted++
		} else {
			t.Fatal(err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatal("concurrent publication was not fenced")
	}
	if got := readPnLFixture(t, s, db, v.ID); got.Version != v.Version+1 || *got.RemainingCost != "30" {
		t.Fatal("duplicate publication")
	}
}
