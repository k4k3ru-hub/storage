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
