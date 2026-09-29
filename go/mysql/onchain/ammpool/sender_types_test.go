package ammpool

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func senderFixture() Batch {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	pool := Identity{"evm", "base", "mainnet", "uniswap-v4", "pool"}
	source := Source{"evm", "base", "mainnet", "uniswap-v4", "factory"}
	key := SenderTransactionKey{"evm", "base", "mainnet", "tx"}
	e := Event{Pool: pool, Source: source, PositionNumber: 10, PositionID: "block", TransactionID: "tx", Index: "0", Type: "swap", OccurredAt: now, ObservedAt: now, Payload: json.RawMessage(`{}`), Canonical: true}
	return Batch{
		Cursor:             Cursor{Source: source, Position: json.RawMessage(`{"block":10}`), UpdatedAt: now},
		Snapshots:          []Snapshot{{Identity: pool, Token0ID: "a", Token1ID: "b", CreatedAt: now, State: json.RawMessage(`{"v":1,"source":"factory"}`), Canonical: true, UpdatedAt: now}},
		Events:             []Event{e},
		ActivityMinutes:    []ActivityMinute{{Pool: pool, Start: now, Totals: json.RawMessage(`{"swapCount":"1"}`), UpdatedAt: now}},
		SenderSnapshots:    []SenderSnapshot{{Pool: pool, CreationEventID: digest("creation"), Generation: 1, InitializedAt: now, UpdatedAt: now}},
		SenderTransactions: []SenderTransaction{{Key: key, Status: SenderPending, NextAttemptAt: &now, DeadlineAt: now.Add(2 * time.Minute), ExpiresAt: now.Add(20 * time.Minute), CreatedAt: now, UpdatedAt: now}},
		SenderEvents:       []SenderEvent{{Pool: pool, Generation: 1, Transaction: key, PositionNumber: 10, PositionID: "block", Index: "0", MinuteStartedAt: &now, Direction: SenderToken0ToToken1, Canonical: true, ObservedAt: now, ExpiresAt: now.Add(16 * time.Minute), UpdatedAt: now}},
		AdmitSenderEvents:  [][32]byte{e.ID()},
	}
}

// TestSenderIdentity verifies fork and network identities preserve case and existing digest semantics.
//
// Version:
//   - 2026-09-28: Added.
func TestSenderIdentity(t *testing.T) {
	b := senderFixture()
	if b.SenderEvents[0].ID() != b.Events[0].ID() {
		t.Fatal("event identity diverged")
	}
	a := b.SenderTransactions[0].Key
	c := a
	c.Network = "other"
	if a.ID() == c.ID() {
		t.Fatal("networks collided")
	}
	c = a
	c.TransactionID = "TX"
	if a.ID() == c.ID() {
		t.Fatal("case lost")
	}
	if digest("ab", "c") == digest("a", "bc") {
		t.Fatal("ambiguous digest")
	}
	e := b.SenderEvents[0]
	e.PositionID = "other-fork"
	if e.ID() == b.Events[0].ID() {
		t.Fatal("fork identity lost")
	}
}

// TestSenderBatchValidation checks nullable state, bounded inputs and atomic parent ownership.
//
// Version:
//   - 2026-09-28: Added.
//   - 2026-09-29: Validate explicit admission and reacceptance intents.
func TestSenderBatchValidation(t *testing.T) {
	if err := senderFixture().Validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Batch){
		"missing parent":     func(b *Batch) { b.Snapshots = nil },
		"different network":  func(b *Batch) { b.SenderEvents[0].Transaction.Network = "other" },
		"zero creation":      func(b *Batch) { b.SenderSnapshots[0].CreationEventID = [32]byte{} },
		"zero generation":    func(b *Batch) { b.SenderSnapshots[0].Generation = 0 },
		"invalid range pair": func(b *Batch) { b.SenderSnapshots[0].InvalidFrom = &b.Cursor.UpdatedAt },
		"empty sender": func(b *Batch) {
			v := ""
			b.SenderTransactions[0].Status = SenderResolved
			b.SenderTransactions[0].NextAttemptAt = nil
			b.SenderTransactions[0].SenderID = &v
		},
		"pending sender":                func(b *Batch) { v := "address"; b.SenderTransactions[0].SenderID = &v },
		"too many attempts":             func(b *Batch) { b.SenderTransactions[0].Attempts = 5 },
		"extended deadline":             func(b *Batch) { b.SenderTransactions[0].DeadlineAt = b.Cursor.UpdatedAt.Add(3 * time.Minute) },
		"extended cache":                func(b *Batch) { b.SenderTransactions[0].ExpiresAt = b.Cursor.UpdatedAt.Add(21 * time.Minute) },
		"direction":                     func(b *Batch) { b.SenderEvents[0].Direction = 3 },
		"minute":                        func(b *Batch) { v := b.Cursor.UpdatedAt.Add(time.Second); b.SenderEvents[0].MinuteStartedAt = &v },
		"extended event":                func(b *Batch) { b.SenderEvents[0].ExpiresAt = b.Cursor.UpdatedAt.Add(17 * time.Minute) },
		"duplicate event":               func(b *Batch) { b.SenderEvents = append(b.SenderEvents, b.SenderEvents[0]) },
		"duplicate state":               func(b *Batch) { b.SenderSnapshots = append(b.SenderSnapshots, b.SenderSnapshots[0]) },
		"unused transaction":            func(b *Batch) { b.SenderEvents = nil },
		"duplicate transaction":         func(b *Batch) { b.SenderTransactions = append(b.SenderTransactions, b.SenderTransactions[0]) },
		"reset without state":           func(b *Batch) { b.ResetSenders = []Identity{b.Snapshots[0].Identity}; b.SenderSnapshots = nil },
		"oversized identifier":          func(b *Batch) { b.SenderTransactions[0].Key.TransactionID = strings.Repeat("a", 129) },
		"unknown admission":             func(b *Batch) { b.AdmitSenderEvents[0] = digest("other") },
		"cancel admission":              func(b *Batch) { b.SenderEvents[0].Canonical = false },
		"duplicate intent":              func(b *Batch) { b.ReacceptSenderEvents = append(b.ReacceptSenderEvents, b.AdmitSenderEvents[0]) },
		"transaction without admission": func(b *Batch) { b.AdmitSenderEvents = nil },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			b := senderFixture()
			edit(&b)
			if err := b.Validate(); err == nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
	b := senderFixture()
	b.SenderEvents[0].MinuteStartedAt = nil
	b.SenderEvents[0].ExpiresAt = b.Cursor.UpdatedAt.Add(2 * time.Minute)
	if err := b.Validate(); err != nil {
		t.Fatal("unknown minute rejected", err)
	}
	b = senderFixture()
	address := "MixedCaseAddress"
	b.SenderTransactions[0].Status = SenderResolved
	b.SenderTransactions[0].SenderID = &address
	b.SenderTransactions[0].NextAttemptAt = nil
	if err := b.Validate(); err != nil {
		t.Fatal("already verified sender rejected", err)
	}
	b = senderFixture()
	b.ReacceptSenderEvents = b.AdmitSenderEvents
	b.AdmitSenderEvents = nil
	b.SenderTransactions = nil
	if err := b.Validate(); err != nil {
		t.Fatal("explicit reacceptance rejected", err)
	}
	b.ReacceptSenderEvents = nil
	if err := b.Validate(); err != nil {
		t.Fatal("update-only batch rejected", err)
	}
}

// TestSenderRestoreValidation rejects unbounded or incomplete restore consumers before database access.
//
// Version:
//   - 2026-09-28: Added.
func TestSenderRestoreValidation(t *testing.T) {
	s, err := NewStore(new(sql.DB))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WalkSenderState(context.Background(), SenderRestoreLimits{}, SenderStateConsumer{}); err == nil {
		t.Fatal("empty limits accepted")
	}
	if err := s.WalkSenderState(context.Background(), SenderRestoreLimits{1, 1, 1, 1}, SenderStateConsumer{}); err == nil {
		t.Fatal("missing consumers accepted")
	}
}
