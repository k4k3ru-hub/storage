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

// TestVerificationValidation verifies observation confirmation without historical scans.
//
// Version:
//   - 2026-09-19: Cover chain-neutral positions and confirmation prerequisites.
func TestVerificationValidation(t *testing.T) {
	number := uint64(0)
	id, kind := "0xabc", "block"
	now := time.Now()
	good := Verification{PositionKind: &kind, SwapObservedPositionNumber: &number, SwapObservedPositionID: &id, ConfirmedAt: &now}
	if err := good.Validate(); err != nil {
		t.Fatal("position zero must be valid", err)
	}
	if err := (Verification{}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"block", "slot", "checkpoint"} {
		v := good
		v.PositionKind = &kind
		if err := v.Validate(); err != nil {
			t.Fatal(kind, err)
		}
	}
	for _, mutate := range []func(*Verification){
		func(v *Verification) { v.PositionKind = nil },
		func(v *Verification) { v.SwapObservedPositionID = nil },
		func(v *Verification) { v.SwapObservedPositionNumber = nil },
		func(v *Verification) { empty := ""; v.SwapObservedPositionID = &empty },
		func(v *Verification) { zero := time.Time{}; v.ConfirmedAt = &zero },
		func(v *Verification) {
			v.SwapObservedPositionNumber = nil
			v.SwapObservedPositionID = nil
			v.PositionKind = nil
		},
	} {
		v := good
		mutate(&v)
		if err := v.Validate(); err == nil {
			t.Fatal("invalid verification accepted", v)
		}
	}
}

// TestBatchObservationAndEvaluation verifies nullable evidence and stale-value retention.
//
// Version:
//   - 2026-09-19: Added.
func TestBatchObservationAndEvaluation(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-time.Hour)
	id, kind, usd := "block-hash", "block", "2000.000000000000000000"
	number := uint64(123)
	good := Snapshot{
		Identity: Identity{"evm", "base", "mainnet", "uniswap-v3", "pool"},
		Token0ID: "token0", Token1ID: "token1", CreatedAt: old, UpdatedAt: now, State: json.RawMessage(`{}`), Canonical: true,
		Verification:   Verification{PositionKind: &kind, SwapObservedPositionNumber: &number, SwapObservedPositionID: &id, ConfirmedAt: &now},
		SwapObservedAt: &old, LiquidityUSD: &usd, LiquidityEvaluatedAt: &old,
	}
	validate := func(s Snapshot) error {
		return (Batch{Cursor: Cursor{Source: Source{"evm", "base", "mainnet", "uniswap-v3", "factory"}, Position: json.RawMessage(`{}`), UpdatedAt: now}, Snapshots: []Snapshot{s}}).Validate()
	}
	// A stale successful valuation must remain persistable; listing policy owns freshness.
	if err := validate(good); err != nil {
		t.Fatal(err)
	}
	unknown := good
	unknown.Verification = Verification{}
	unknown.SwapObservedAt = nil
	unknown.LiquidityUSD = nil
	unknown.LiquidityEvaluatedAt = nil
	if err := validate(unknown); err != nil {
		t.Fatal(err)
	}
	zero := "0"
	knownZero := good
	knownZero.LiquidityUSD = &zero
	if err := validate(knownZero); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.SwapObservedAt = nil },
		func(s *Snapshot) { s.Verification = Verification{} },
		func(s *Snapshot) { s.Canonical = false },
		func(s *Snapshot) { s.LiquidityUSD = nil },
		func(s *Snapshot) { s.LiquidityEvaluatedAt = nil },
		func(s *Snapshot) { z := time.Time{}; s.LiquidityEvaluatedAt = &z },
		func(s *Snapshot) { z := time.Time{}; s.SwapObservedAt = &z },
	} {
		invalid := good
		mutate(&invalid)
		if err := validate(invalid); err == nil {
			t.Fatal("inconsistent evidence accepted", invalid)
		}
	}
}
