package ammpool

import (
	"errors"
	"fmt"
	"time"
)

const (
	SenderPending               = "pending"
	SenderResolved              = "resolved"
	SenderAbandoned             = "abandoned"
	SenderToken0ToToken1  uint8 = 0
	SenderToken1ToToken0  uint8 = 1
	SenderUndirected      uint8 = 2
	MaxSenderAttempts           = 4
	MaxSenderPools              = 1024
	MaxSenderTransactions       = 4096
	MaxSenderEvents             = 65536
	MaxSenderPoolEvents         = 4096
)

var (
	ErrSenderConflict = errors.New("sender state conflict")
	ErrSenderCapacity = errors.New("sender restore capacity exceeded")
)

type SenderSnapshot struct {
	Pool            Identity
	CreationEventID [32]byte
	Generation      uint64
	InitializedAt   time.Time
	InvalidFrom     *time.Time
	InvalidTo       *time.Time
	UpdatedAt       time.Time
}

type SenderInvalidation struct {
	Pool      Identity
	From      time.Time
	To        time.Time
	UpdatedAt time.Time
}

type SenderTransactionKey struct {
	ChainFamily   string
	Chain         string
	Network       string
	TransactionID string
}

type SenderTransaction struct {
	Key           SenderTransactionKey
	SenderID      *string
	Status        string
	Attempts      uint8
	NextAttemptAt *time.Time
	DeadlineAt    time.Time
	ExpiresAt     time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type SenderEvent struct {
	Pool            Identity
	Generation      uint64
	Transaction     SenderTransactionKey
	PositionNumber  uint64
	PositionID      string
	Index           string
	MinuteStartedAt *time.Time
	Direction       uint8
	Canonical       bool
	ObservedAt      time.Time
	ExpiresAt       time.Time
	UpdatedAt       time.Time
}

type SenderAttemptResult struct {
	Status        string
	SenderID      *string
	NextAttemptAt *time.Time
	UpdatedAt     time.Time
}

type SenderRestoreLimits struct {
	Pools         int
	Transactions  int
	Events        int
	EventsPerPool int
}

type SenderStateConsumer struct {
	Pool        func(Snapshot, SenderSnapshot) error
	Transaction func(SenderTransaction) error
	Event       func(SenderEvent) error
}

// ID returns the network-scoped transaction digest without a venue or pool.
//
// Version:
//   - 2026-09-28: Added.
func (k SenderTransactionKey) ID() [32]byte {
	return digest(k.ChainFamily, k.Chain, k.Network, k.TransactionID)
}

// Validate checks opaque transaction identifiers without chain-specific normalization.
//
// Version:
//   - 2026-09-28: Added.
func (k SenderTransactionKey) Validate() error {
	for _, item := range []struct {
		name, value string
		max         int
	}{
		{"chain_family", k.ChainFamily, 16}, {"chain", k.Chain, 64},
		{"network", k.Network, 64}, {"transaction_id", k.TransactionID, 128},
	} {
		if err := senderText(item.name, item.value, item.max); err != nil {
			return err
		}
	}
	return nil
}

// ID returns the same fork-aware occurrence digest as Event.ID.
//
// Version:
//   - 2026-09-28: Added.
func (e SenderEvent) ID() [32]byte {
	return (Event{Pool: e.Pool, PositionID: e.PositionID, TransactionID: e.Transaction.TransactionID, Index: e.Index}).ID()
}

// Validate checks sender collection generation and invalid interval shape.
//
// Version:
//   - 2026-09-28: Added.
func (p SenderSnapshot) Validate() error {
	if err := p.Pool.Validate(); err != nil {
		return fmt.Errorf("failed to validate sender snapshot: %w", err)
	}
	if p.CreationEventID == ([32]byte{}) || p.Generation == 0 {
		return fmt.Errorf("failed to validate sender snapshot: creation_or_generation=empty")
	}
	if !senderTime(p.InitializedAt) || !senderTime(p.UpdatedAt) || dbTime(p.UpdatedAt).Before(dbTime(p.InitializedAt)) {
		return fmt.Errorf("failed to validate sender snapshot: timestamp=invalid")
	}
	if (p.InvalidFrom == nil) != (p.InvalidTo == nil) {
		return fmt.Errorf("failed to validate sender snapshot: invalid_interval=invalid")
	}
	if p.InvalidFrom != nil && (!senderMinute(*p.InvalidFrom) || !senderMinute(*p.InvalidTo) || !p.InvalidTo.After(*p.InvalidFrom)) {
		return fmt.Errorf("failed to validate sender snapshot: invalid_interval=invalid")
	}
	return nil
}

// Validate checks durable resolution shape and bounded timestamps, including expired records.
//
// Version:
//   - 2026-09-28: Added.
func (t SenderTransaction) Validate() error {
	if err := t.Key.Validate(); err != nil {
		return fmt.Errorf("failed to validate sender transaction: %w", err)
	}
	if t.Attempts > MaxSenderAttempts {
		return fmt.Errorf("failed to validate sender transaction: attempts=out_of_range max_value=4")
	}
	if !senderTime(t.CreatedAt) || !senderTime(t.UpdatedAt) || !senderTime(t.DeadlineAt) || !senderTime(t.ExpiresAt) ||
		dbTime(t.UpdatedAt).Before(dbTime(t.CreatedAt)) || !dbTime(t.DeadlineAt).After(dbTime(t.CreatedAt)) || t.DeadlineAt.Sub(t.CreatedAt) > 2*time.Minute ||
		dbTime(t.ExpiresAt).Before(dbTime(t.DeadlineAt)) || t.ExpiresAt.Sub(t.CreatedAt) > 20*time.Minute {
		return fmt.Errorf("failed to validate sender transaction: timestamp=invalid")
	}
	switch t.Status {
	case SenderPending:
		if t.SenderID != nil || t.NextAttemptAt == nil {
			return fmt.Errorf("failed to validate sender transaction: pending_state=invalid")
		}
		if !senderTime(*t.NextAttemptAt) || dbTime(*t.NextAttemptAt).Before(dbTime(t.UpdatedAt)) || dbTime(*t.NextAttemptAt).After(dbTime(t.DeadlineAt)) {
			return fmt.Errorf("failed to validate sender transaction: next_attempt_at=out_of_range")
		}
	case SenderResolved:
		if t.SenderID == nil || t.NextAttemptAt != nil {
			return fmt.Errorf("failed to validate sender transaction: resolved_state=invalid")
		}
		if err := senderText("sender_id", *t.SenderID, 128); err != nil {
			return err
		}
	case SenderAbandoned:
		if t.SenderID != nil || t.NextAttemptAt != nil {
			return fmt.Errorf("failed to validate sender transaction: abandoned_state=invalid")
		}
	default:
		return fmt.Errorf("failed to validate sender transaction: status=invalid")
	}
	return nil
}

// Validate checks occurrence identity, network ownership and fixed event expiry.
//
// Version:
//   - 2026-09-28: Added.
func (e SenderEvent) Validate() error {
	if err := e.Pool.Validate(); err != nil {
		return fmt.Errorf("failed to validate sender event: %w", err)
	}
	if err := e.Transaction.Validate(); err != nil {
		return fmt.Errorf("failed to validate sender event: %w", err)
	}
	if e.Transaction.ChainFamily != e.Pool.ChainFamily || e.Transaction.Chain != e.Pool.Chain || e.Transaction.Network != e.Pool.Network {
		return fmt.Errorf("failed to validate sender event: transaction_scope=invalid")
	}
	if err := senderText("position_id", e.PositionID, 128); err != nil {
		return err
	}
	if err := senderText("event_index", e.Index, 128); err != nil {
		return err
	}
	if e.Generation == 0 {
		return fmt.Errorf("failed to validate sender event: generation=empty")
	}
	if e.Direction > SenderUndirected {
		return fmt.Errorf("failed to validate sender event: direction=out_of_range")
	}
	if !senderTime(e.ObservedAt) || !senderTime(e.UpdatedAt) || !senderTime(e.ExpiresAt) || dbTime(e.UpdatedAt).Before(dbTime(e.ObservedAt)) {
		return fmt.Errorf("failed to validate sender event: timestamp=invalid")
	}
	expiry := e.ObservedAt.Add(2 * time.Minute)
	if e.MinuteStartedAt != nil {
		if !senderMinute(*e.MinuteStartedAt) {
			return fmt.Errorf("failed to validate sender event: minute_started_at=invalid")
		}
		expiry = e.MinuteStartedAt.Add(16 * time.Minute)
	}
	if !sameTime(e.ExpiresAt, expiry) {
		return fmt.Errorf("failed to validate sender event: expires_at=invalid")
	}
	return nil
}

func (b Batch) validateSenders() error {
	if len(b.SenderSnapshots)+len(b.SenderTransactions)+len(b.SenderEvents)+len(b.SenderEvidence)+len(b.SenderInvalidations)+len(b.ResetSenders)+len(b.AdmitSenderEvents)+len(b.ReacceptSenderEvents) == 0 {
		return nil
	}
	if len(b.SenderSnapshots) > MaxSenderPools || len(b.SenderTransactions) > MaxSenderTransactions || len(b.SenderEvents) > MaxSenderEvents || len(b.SenderEvidence) > len(b.SenderEvents) || len(b.ResetSenders) > MaxSenderPools || len(b.AdmitSenderEvents)+len(b.ReacceptSenderEvents) > len(b.SenderEvents) {
		return fmt.Errorf("failed to validate sender batch: size=too_long")
	}
	parents := make(map[Identity]bool, len(b.Snapshots))
	for _, p := range b.Snapshots {
		parents[p.Identity] = true
	}
	states := make(map[Identity]bool)
	invalidations := make(map[Identity]bool)
	for _, v := range b.SenderInvalidations {
		if !parents[v.Pool] || invalidations[v.Pool] {
			return fmt.Errorf("failed to validate sender invalidation: parent=invalid")
		}
		if !senderMinute(v.From) || !senderMinute(v.To) || !v.To.After(v.From) || !senderTime(v.UpdatedAt) {
			return fmt.Errorf("failed to validate sender invalidation: interval=invalid")
		}
		invalidations[v.Pool] = true
	}
	for _, p := range b.SenderSnapshots {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("failed to validate sender batch: %w", err)
		}
		if !parents[p.Pool] || states[p.Pool] || invalidations[p.Pool] {
			return fmt.Errorf("failed to validate sender batch: parent=invalid")
		}
		states[p.Pool] = true
	}
	resets := make(map[Identity]bool)
	for _, p := range b.ResetSenders {
		if !states[p] || resets[p] {
			return fmt.Errorf("failed to validate sender batch: reset=invalid")
		}
		resets[p] = true
	}
	events := make(map[[32]byte]SenderEvent)
	perPool := make(map[Identity]int)
	for _, e := range b.SenderEvents {
		if err := e.Validate(); err != nil {
			return fmt.Errorf("failed to validate sender batch: %w", err)
		}
		if _, duplicate := events[e.ID()]; !parents[e.Pool] || duplicate || invalidations[e.Pool] {
			return fmt.Errorf("failed to validate sender batch: event_parent_or_identity=invalid")
		}
		perPool[e.Pool]++
		if perPool[e.Pool] > MaxSenderPoolEvents {
			return fmt.Errorf("failed to validate sender batch: pool_events=too_long")
		}
		events[e.ID()] = e
	}
	proofs := make(map[[32]byte]bool)
	for _, proof := range b.SenderEvidence {
		e, ok := events[proof.ID()]
		if !ok || proofs[proof.ID()] || proof.Source != b.Cursor.Source || proof.Pool != e.Pool || proof.PositionNumber != e.PositionNumber || proof.Canonical != e.Canonical || !senderTime(proof.ObservedAt) {
			return fmt.Errorf("failed to validate sender batch: evidence=invalid")
		}
		proofs[proof.ID()] = true
	}
	intents := make(map[[32]byte]bool)
	used := make(map[SenderTransactionKey]bool)
	for _, group := range []struct {
		ids       [][32]byte
		admission bool
	}{{b.AdmitSenderEvents, true}, {b.ReacceptSenderEvents, false}} {
		for _, id := range group.ids {
			e, ok := events[id]
			if !ok || !e.Canonical || intents[id] {
				return fmt.Errorf("failed to validate sender batch: event_intent=invalid")
			}
			intents[id] = true
			if group.admission {
				used[e.Transaction] = true
			}
		}
	}
	seen := make(map[SenderTransactionKey]bool)
	for _, t := range b.SenderTransactions {
		if err := t.Validate(); err != nil {
			return fmt.Errorf("failed to validate sender batch: %w", err)
		}
		if seen[t.Key] || !used[t.Key] || t.Attempts != 0 || t.Status == SenderAbandoned || !sameTime(t.CreatedAt, t.UpdatedAt) || (t.NextAttemptAt != nil && !sameTime(*t.NextAttemptAt, t.CreatedAt)) {
			return fmt.Errorf("failed to validate sender batch: initial_transaction=invalid")
		}
		seen[t.Key] = true
	}
	return nil
}

func senderText(name, value string, max int) error {
	state := "invalid"
	if value == "" {
		state = "empty"
	} else if len(value) > max {
		state = "too_long"
	} else if validText(value, max) {
		return nil
	}
	return fmt.Errorf("failed to validate sender identifier: %s=%s", name, state)
}

func senderTime(t time.Time) bool   { return !t.IsZero() && t.Year() >= 1000 && t.Year() <= 9999 }
func senderMinute(t time.Time) bool { return senderTime(t) && t.Equal(t.Truncate(time.Minute)) }
func dbTime(t time.Time) time.Time  { return t.UTC().Truncate(time.Microsecond) }
func sameTime(a, b time.Time) bool  { return dbTime(a).Equal(dbTime(b)) }
func sameOptionalTime(a, b *time.Time) bool {
	return a == nil && b == nil || a != nil && b != nil && sameTime(*a, *b)
}
func senderUTC(t *time.Time) any {
	if t == nil {
		return nil
	}
	return dbTime(*t)
}
