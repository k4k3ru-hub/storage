package oms

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func mysqlPerpetualStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("K4K3RU_OMS_TEST_DSN")
	if dsn == "" {
		t.Skip("set K4K3RU_OMS_TEST_DSN to an isolated MySQL database")
	}
	db, err := sql.Open("mysql", dsn)
	must(t, err)
	t.Cleanup(func() { must(t, db.Close()) })
	prefix := fmt.Sprintf("perp_test_%d", GenerateOrderID())
	s, err := NewStoreWithPerpetual(prefix, prefix+"_exec", prefix+"_chain", prefix+"_fee", prefix+"_pnl", prefix+"_perp")
	must(t, err)
	must(t, s.CreateTables(t.Context(), db))
	t.Cleanup(func() {
		for _, name := range []string{s.pnlTable, s.feeTable, s.perpetualEventTable, s.onchainEventTable, s.executionTable, s.orderTable} {
			_, err := db.Exec("DROP TABLE " + quoted(name))
			must(t, err)
		}
	})
	return s, db
}
func perpetualOrder() Order {
	o := testOrder()
	o.Domain = DomainPerpetual
	o.Venue = ptr("hyperliquid")
	o.Side = "buy"
	o.OrderType = "limit"
	o.LimitPrice = ptr("1.2")
	o.Specification = json.RawMessage(`{"quantityAsset":{"namespace":"venue","assetId":"hyperliquid:testnet:perp:default:SUI:base","symbol":"SUI","decimals":1},"counterQuantityAsset":{"namespace":"venue","assetId":"hyperliquid:testnet:token:USDC","symbol":"USDC","decimals":6}}`)
	return o
}
func perpetualAcceptance() PerpetualEvent {
	a := acceptance(1, 1)
	a.Event.Venue = ptr("hyperliquid")
	return PerpetualEvent{Event: a.Event, Perpetual: PerpetualEvidence{Network: "testnet", TradingAccount: "synthetic-account", MarketID: "perp:default:SUI", ClientOrderID: ptr("synthetic-client-order"), ObservedAt: fixtureTime, ProtocolVersion: 1, ProtocolData: json.RawMessage(`{"b":2,"a":1}`)}}
}
func perpetualFill() PerpetualEvent {
	f := fill(2, 1, 1, "perp-fill", "100")
	f.Event.Venue = ptr("hyperliquid")
	f.Event.QuantityAssetID = ptr("hyperliquid:testnet:perp:default:SUI:base")
	f.Event.CounterAssetID = ptr("hyperliquid:testnet:token:USDC")
	f.Event.QuantityDecimals = ptr(uint16(1))
	f.Event.CounterQuantity = ptr("120")
	f.Event.OrderCounterQuantity = ptr("120")
	f.Event.Price = ptr("1.2")
	f.Event.FeesComplete = ptr(true)
	d := perpetualAcceptance().Perpetual
	d.VenueOrderID = ptr("9007199254740993")
	d.FillID = ptr("9007199254740995")
	hash := sha256.Sum256([]byte("synthetic-fill"))
	d.FillIdentityHash = hash[:]
	d.FillTimeMS = ptr(uint64(fixtureTime.UnixMilli()))
	d.Direction = ptr("Open Long")
	d.ClosedPnL = ptr("-0.000000000000000001")
	d.ClosedPnLAssetID = f.Event.CounterAssetID
	d.ClosedPnLDecimals = ptr(uint16(18))
	fee := fee("perp-fee", "trading", "-0.000000000000000001")
	fee.AssetNamespace = "venue"
	fee.AssetID = *f.Event.CounterAssetID
	fee.AssetDecimals = 18
	return PerpetualEvent{Event: f.Event, Perpetual: d, Fees: []ExecutionFee{fee}}
}

// TestMySQLPerpetualStore verifies custom DDL, replay, idempotency, precision and family isolation.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLPerpetualStore(t *testing.T) {
	s, db := mysqlPerpetualStore(t)
	insertFixture(t, s, db, perpetualOrder())
	appendRecord := func(r PerpetualEvent) *AppendResult {
		t.Helper()
		var result *AppendResult
		must(t, withTx(t.Context(), db, func(tx *sql.Tx) error {
			var err error
			result, err = s.AppendPerpetualEvent(t.Context(), tx, 1, r)
			return err
		}))
		return result
	}
	appendRecord(perpetualAcceptance())
	f := perpetualFill()
	result := appendRecord(f)
	if result.Execution.FilledQuantity != "100" || result.Execution.FilledCounterQuantity == nil || *result.Execution.FilledCounterQuantity != "120" || result.Execution.FeesComplete {
		t.Fatal("invalid fill snapshot")
	}
	duplicate := f
	duplicate.Perpetual.ObservedAt = fixtureTime.Add(time.Hour)
	duplicate.Perpetual.ProtocolData = json.RawMessage(`{ "a": 1, "b": 2 }`)
	if got := appendRecord(duplicate); !got.Duplicate || got.EventID != result.EventID || got.Sequence != 2 {
		t.Fatal("duplicate changed accounting")
	}
	costs, err := s.ListEventFees(t.Context(), db, 1, 1, result.EventID)
	must(t, err)
	if len(costs) != 1 || costs[0].Amount != "-0.000000000000000001" {
		t.Fatal("rebate precision lost")
	}
	for _, mutate := range []func(*PerpetualEvent){
		func(r *PerpetualEvent) { r.Perpetual.ClosedPnL = ptr("0") },
		func(r *PerpetualEvent) { r.Perpetual.Network = "mainnet" },
		func(r *PerpetualEvent) { r.Event.RecordKey = []byte("another-key"); r.Event.ID = 3 },
	} {
		r := f
		mutate(&r)
		err := withTx(t.Context(), db, func(tx *sql.Tx) error { _, err := s.AppendPerpetualEvent(t.Context(), tx, 1, r); return err })
		if !errors.Is(err, ErrConflict) {
			t.Fatal("conflicting duplicate accepted", err)
		}
	}
	for _, read := range []func() error{
		func() error { _, err := s.ListPerpetualEvents(t.Context(), db, 2, 1, 0, 10); return err },
		func() error {
			_, err := s.SelectPerpetualEventByKey(t.Context(), db, 2, 1, f.Event.RecordKey)
			return err
		},
		func() error { _, err := s.ListEventFees(t.Context(), db, 2, 1, result.EventID); return err },
	} {
		if err := read(); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal("foreign history read", err)
		}
	}
	err = withTx(t.Context(), db, func(tx *sql.Tx) error {
		_, err := s.AppendOnchainEvent(t.Context(), tx, 1, acceptance(3, 1))
		return err
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatal("onchain event inserted in perpetual order", err)
	}
	completion := perpetualAcceptance()
	completion.Event = fact(3, 1, 1, EventTypeEvidenceRecorded, "complete").Event
	completion.Event.Venue = ptr("hyperliquid")
	completion.Event.ReferenceEventID = ptr(uint64(1))
	completion.Perpetual.ProtocolData = json.RawMessage(`{"completion":{"expectedQuantity":"100","fillsComplete":true,"feesComplete":true}}`)
	if !appendRecord(completion).Execution.FeesComplete {
		t.Fatal("completion proof missing")
	}
	history, err := s.ListPerpetualEvents(t.Context(), db, 1, 1, 0, 200)
	must(t, err)
	facts := make([]Event, len(history))
	for i, r := range history {
		facts[i] = r.Event
	}
	o, err := s.SelectOrder(t.Context(), db, 1, 1)
	must(t, err)
	replayed, err := ReplayOrder(*o, facts)
	must(t, err)
	if !orderStateEqual(o.OrderState, replayed.State) {
		t.Fatal("replay diverged")
	}
	if onchain, err := s.ListOnchainEvents(t.Context(), db, 1, 1, 0, 20); err != nil || len(onchain) != 0 {
		t.Fatal("perpetual history mixed with onchain", err)
	}
}

// TestPerpetualCompositionAndLabels verifies explicit composition and printable venue directions.
//
// Version:
//   - 2026-09-29: Added.
func TestPerpetualCompositionAndLabels(t *testing.T) {
	s, err := NewStoreWithPerpetual("o", "e", "c", "f", "p", "v")
	must(t, err)
	if s.perpetualEventTable != "v" || s.pnlTable != "p" {
		t.Fatal("incomplete composition")
	}
	for _, name := range []string{"", "o", "e", "c", "f", "p", "P", "bad;table"} {
		if _, err := NewStoreWithPerpetual("o", "e", "c", "f", "p", name); err == nil {
			t.Fatal("invalid table accepted")
		}
	}
	for _, label := range []string{"Open Long", "Close Short", "Long > Short"} {
		f := perpetualFill()
		f.Perpetual.Direction = &label
		_, err := normalizePerpetualRecord(f, 2, nil)
		must(t, err)
	}
	for _, label := range []string{"", " Open Long", "Open Long ", "Open\nLong", "Open\tLong", "方向"} {
		f := perpetualFill()
		f.Perpetual.Direction = &label
		if _, err := normalizePerpetualRecord(f, 2, nil); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("invalid direction accepted", err)
		}
	}
}
