package ammpool

import (
	"encoding/json"
	"testing"
	"time"
)

func TestIdentityPreservesChainIdentifiers(t *testing.T) {
	a := Identity{"solana", "solana", "mainnet", "raydium", "AbC123"}
	b := a
	b.PoolID = "abc123"
	if a.ID() == b.ID() {
		t.Fatal("case-sensitive identifiers collided")
	}
	b = a
	b.Network = "devnet"
	if a.ID() == b.ID() {
		t.Fatal("networks collided")
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	a = Identity{"sui", "sui", "mainnet", "cetus", "0x123"}
	s := Snapshot{Identity: a, Token0ID: "0x2::sui::SUI", Token1ID: "0x123::coin::COIN", CreatedAt: time.Now(), UpdatedAt: time.Now(), State: json.RawMessage(`{}`)}
	source := Source{"sui", "sui", "mainnet", "cetus", "package::module"}
	batch := Batch{Cursor: Cursor{Source: source, Position: json.RawMessage(`{"checkpoint":"123","eventSeq":"0"}`), UpdatedAt: time.Now()}, Snapshots: []Snapshot{s}}
	if err := batch.Validate(); err != nil {
		t.Fatal(err)
	}
	batch.Snapshots[0].Identity.Network = "testnet"
	if err := batch.Validate(); err == nil {
		t.Fatal("cross-network snapshot accepted")
	}
}

func TestEventIdentityReorgAndReplay(t *testing.T) {
	e := Event{Pool: Identity{"evm", "base", "mainnet", "uniswap-v3", "0x123"}, PositionID: "block-a", TransactionID: "tx", Index: "0"}
	replay := e
	replay.ObservedAt = time.Now()
	replay.Canonical = false
	if e.ID() != replay.ID() {
		t.Fatal("replay is not idempotent")
	}
	replay.PositionID = "block-b"
	if e.ID() == replay.ID() {
		t.Fatal("fork occurrence collided")
	}
}

func TestCompositionRequiresDatabase(t *testing.T) {
	if _, err := NewStore(nil); err == nil {
		t.Fatal("nil database accepted")
	}
}

// TestVerificationValidation verifies NewPair confirmation behavior.
//
// Version:
//   - 2026-09-18: Added.
func TestVerificationValidation(t *testing.T) {
	number := uint64(0)
	id, kind := "0xabc", "block"
	now := time.Now()
	good := Verification{PositionKind: &kind, FirstLiquidityPositionNumber: &number, FirstLiquidityPositionID: &id, FirstSwapPositionNumber: &number, FirstSwapPositionID: &id, EventScanFromPosition: &number, EventScanThroughPosition: &number, EventScanThroughPositionID: &id, ConfirmedAt: &now}
	if err := good.Validate(); err != nil {
		t.Fatal("position zero must be valid", err)
	}
	if err := (Verification{}).Validate(); err != nil {
		t.Fatal("legacy empty verification must be valid", err)
	}
	for _, mutate := range []func(*Verification){
		func(v *Verification) { v.PositionKind = nil },
		func(v *Verification) { v.FirstSwapPositionID = nil },
		func(v *Verification) { v.EventScanFromPosition = nil },
		func(v *Verification) { n := uint64(1); v.EventScanFromPosition = &n },
		func(v *Verification) { n := uint64(1); v.FirstSwapPositionNumber = &n },
	} {
		v := good
		mutate(&v)
		if err := v.Validate(); err == nil {
			t.Fatal("invalid verification accepted", v)
		}
	}
}

// TestAbandonedVerificationRejectsConfirmation checks mutually exclusive terminal states.
//
// Version:
//   - 2026-09-18: Added.
func TestAbandonedVerificationRejectsConfirmation(t *testing.T) {
	now := time.Now()
	if err := (Verification{BackfillAbandonedAt: &now}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Verification{BackfillAbandonedAt: &now, ConfirmedAt: &now}).Validate(); err == nil {
		t.Fatal("confirmed and abandoned accepted")
	}
	zero := time.Time{}
	if err := (Verification{BackfillAbandonedAt: &zero}).Validate(); err == nil {
		t.Fatal("zero abandonment accepted")
	}
}
