package oms

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func testPnLScope() PnLScope {
	return PnLScope{AccountRef: "wallet", SubjectType: PnLSubjectSpotInventory,
		InventoryAsset:  &QuantityAsset{Namespace: "onchain", Chain: "sui", Network: "testnet", AssetID: "0x2::sui::SUI", Symbol: "SUI", Decimals: 9},
		AccountingAsset: QuantityAsset{Namespace: "onchain", Chain: "sui", Network: "testnet", AssetID: "test::usdc::USDC", Symbol: "USDC", Decimals: 6}}
}

func testPnLCheckpoint(last uint64) PnLCheckpoint {
	v := PnLCheckpoint{RemainingQuantity: ptr("6"), RemainingCost: ptr("30"), RealizedPnL: ptr("4"),
		CalculationMethod: "moving_average", CalculationVersion: 1,
		CalculationState: []byte(`{"remainingCost":{"numerator":"30","denominator":"1"},"economicBoundary":{"checkpoint":"50"}}`), CalculatedAt: fixtureTime}
	if last > 0 {
		v.LastOrderID = &last
	} else {
		v.RemainingQuantity, v.RemainingCost, v.RealizedPnL = ptr("0"), ptr("0"), ptr("0")
		v.CalculationState = []byte(`{"empty":true}`)
	}
	return v
}

// TestPnLComposition verifies explicit opt-in, identifier isolation and dependency guards.
//
// Version:
//   - 2026-09-28: Added.
func TestPnLComposition(t *testing.T) {
	s, err := NewStoreWithPnL("a", "b", "c", "d", "p")
	must(t, err)
	if s.pnlTable != "p" || s.orderTable != "a" || s.executionTable != "b" || s.onchainEventTable != "c" || s.feeTable != "d" {
		t.Fatal("incomplete composition")
	}
	for _, name := range []string{"", "A", "b", "c", "D", "bad;name", strings.Repeat("x", 65)} {
		if _, err := NewStoreWithPnL("a", "b", "c", "d", name); !errors.Is(err, ErrInvalidParameter) {
			t.Fatalf("invalid name accepted: %q", name)
		}
	}
	var tx *sql.Tx
	if _, err := s.EnsurePnL(context.Background(), tx, 1, testPnLScope()); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil transaction accepted")
	}
	if err := s.SavePnL(context.Background(), tx, 1, 1, 1, testPnLCheckpoint(1), true); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil save transaction accepted")
	}
	var nilStore *Store
	if _, err := nilStore.SelectPnL(context.Background(), tx, 1, 1); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("nil store accepted")
	}
}

// TestPnLIdentity verifies exact asset separation without display-symbol or method-based splitting.
//
// Version:
//   - 2026-09-28: Added.
func TestPnLIdentity(t *testing.T) {
	a := testPnLScope()
	must(t, a.validate())
	key, err := a.key()
	must(t, err)
	b := testPnLScope()
	b.InventoryAsset.Symbol = "another label"
	k, err := b.key()
	must(t, err)
	if key != k || !samePnLScope(a, b) {
		t.Fatal("label split inventory")
	}
	b.AccountingAsset.AssetID = "other::usdc::USDC"
	k, err = b.key()
	must(t, err)
	if key != k || samePnLScope(a, b) {
		t.Fatal("accounting unit did not require explicit scope conflict")
	}
	for _, mutate := range []func(*PnLScope){
		func(v *PnLScope) { v.InventoryAsset.AssetID = "other::sui::SUI" },
		func(v *PnLScope) { v.InventoryAsset.Network = "mainnet" },
		func(v *PnLScope) { v.AccountRef = "another-wallet" },
	} {
		b := testPnLScope()
		mutate(&b)
		k, err := b.key()
		must(t, err)
		if key == k {
			t.Fatal("distinct inventory merged")
		}
	}
	b = testPnLScope()
	b.InventoryAsset.Network = "custom-network"
	must(t, b.validate())
}

// TestPnLValidation verifies nullable amounts, exact decimal text and bounded restart state.
//
// Version:
//   - 2026-09-28: Added.
func TestPnLValidation(t *testing.T) {
	v := testPnLCheckpoint(1)
	v.RemainingCost, v.RealizedPnL = nil, ptr("-0.0000000000000000001")
	must(t, v.validate(PnLSubjectSpotInventory))
	for _, mutate := range []func(*PnLCheckpoint){
		func(v *PnLCheckpoint) { v.LastOrderID = ptr(uint64(0)) },
		func(v *PnLCheckpoint) { v.CalculationVersion = 0 },
		func(v *PnLCheckpoint) { v.CalculationState = []byte(`[]`) },
		func(v *PnLCheckpoint) {
			v.CalculationState = []byte(`{"x":"` + strings.Repeat("x", maxPnLStateBytes) + `"}`)
		},
		func(v *PnLCheckpoint) { v.RemainingQuantity = ptr("-1") },
		func(v *PnLCheckpoint) { v.RealizedPnL = ptr("-0") },
		func(v *PnLCheckpoint) { v.RealizedPnL = ptr("1e3") },
		func(v *PnLCheckpoint) { v.AverageEntryPrice = ptr("5") },
	} {
		v := testPnLCheckpoint(1)
		mutate(&v)
		if !errors.Is(v.validate(PnLSubjectSpotInventory), ErrInvalidParameter) {
			t.Fatal("invalid checkpoint accepted")
		}
	}
	for _, mutate := range []func(*PnLScope){
		func(v *PnLScope) { v.PositionOrderID = ptr(uint64(1)) },
		func(v *PnLScope) { v.InventoryAsset = nil },
		func(v *PnLScope) { v.AccountingAsset.Network = "" },
		func(v *PnLScope) { v.SubjectType = "unknown" },
	} {
		v := testPnLScope()
		mutate(&v)
		if !errors.Is(v.validate(), ErrInvalidParameter) {
			t.Fatal("invalid scope accepted")
		}
	}
}

// TestRoundTripPnLIdentity separates funding units while retaining exact held assets.
//
// Version:
//   - 2026-09-29: Added.
func TestRoundTripPnLIdentity(t *testing.T) {
	a := testPnLScope()
	a.SubjectType = PnLSubjectSpotRoundTrip
	must(t, a.validate())
	key, err := a.key()
	must(t, err)
	b := a
	b.AccountingAsset = QuantityAsset{Namespace: "currency", AssetID: "USDC", Symbol: "USDC", Decimals: 6}
	must(t, b.validate())
	other, err := b.key()
	must(t, err)
	if key == other {
		t.Fatal("funding units merged")
	}
	b.InventoryAsset = &QuantityAsset{Namespace: "onchain", Chain: "sui", Network: "testnet", AssetID: "another-token", Decimals: 9}
	third, err := b.key()
	must(t, err)
	if third == other {
		t.Fatal("held assets merged")
	}
	cp := testPnLCheckpoint(1)
	must(t, cp.validate(PnLSubjectSpotRoundTrip))
	cp.AverageEntryPrice = ptr("1")
	if !errors.Is(cp.validate(PnLSubjectSpotRoundTrip), ErrInvalidParameter) {
		t.Fatal("inventory accepted position price")
	}
}
