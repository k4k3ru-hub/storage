package ammpool

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Identity struct {
	ChainFamily string
	Chain       string
	Network     string
	Venue       string
	PoolID      string
}

type Source struct {
	ChainFamily string
	Chain       string
	Network     string
	Venue       string
	Key         string
}

type Snapshot struct {
	Identity         Identity
	Token0ID         string
	Token1ID         string
	CreatedAt        time.Time
	FirstLiquidityAt *time.Time
	FirstSwapAt      *time.Time
	LiquidityUSD     *string
	State            json.RawMessage
	Revision         uint64
	Canonical        bool
	UpdatedAt        time.Time
}

type Event struct {
	Pool           Identity
	Source         Source
	PositionNumber uint64
	PositionID     string
	TransactionID  string
	Index          string
	Type           string
	OccurredAt     time.Time
	ObservedAt     time.Time
	Payload        json.RawMessage
	Canonical      bool
}

type Cursor struct {
	Source    Source
	Position  json.RawMessage
	Revision  uint64
	UpdatedAt time.Time
}

type Batch struct {
	Cursor    Cursor
	Snapshots []Snapshot
	Events    []Event
}

// ID returns the case-sensitive, length-delimited pool identity digest.
//
// Version:
//   - 2026-09-16: Added.
func (i Identity) ID() [32]byte { return digest(i.ChainFamily, i.Chain, i.Network, i.Venue, i.PoolID) }

// ID returns the case-sensitive source identity digest.
//
// Version:
//   - 2026-09-16: Added.
func (s Source) ID() [32]byte { return digest(s.ChainFamily, s.Chain, s.Network, s.Venue, s.Key) }

// ID returns a stable event identity including its fork position and pool.
//
// Version:
//   - 2026-09-16: Added.
func (e Event) ID() [32]byte {
	p := e.Pool.ID()
	return digest(string(p[:]), e.PositionID, e.TransactionID, e.Index)
}

func digest(values ...string) [32]byte {
	var b strings.Builder
	for _, v := range values {
		fmt.Fprintf(&b, "%d:%s", len(v), v)
	}
	return sha256.Sum256([]byte(b.String()))
}

func validText(s string, max int) bool {
	if s == "" || len(s) > max {
		return false
	}
	for _, c := range s {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

// Validate checks identity bounds without applying EVM address rules.
//
// Version:
//   - 2026-09-16: Added.
func (i Identity) Validate() error {
	if !validText(i.ChainFamily, 16) || !validText(i.Chain, 64) || !validText(i.Network, 64) || !validText(i.Venue, 64) || !validText(i.PoolID, 128) {
		return fmt.Errorf("failed to validate amm pool identity: identifier=invalid")
	}
	return nil
}

// Validate checks source identifier bounds.
//
// Version:
//   - 2026-09-16: Added.
func (s Source) Validate() error {
	if err := (Identity{s.ChainFamily, s.Chain, s.Network, s.Venue, "source"}).Validate(); err != nil {
		return fmt.Errorf("failed to validate amm pool source: %w", err)
	}
	if !validText(s.Key, 255) {
		return fmt.Errorf("failed to validate amm pool source: key=invalid")
	}
	return nil
}

// Validate checks an atomic batch before opening a transaction.
//
// Version:
//   - 2026-09-16: Added.
func (b Batch) Validate() error {
	if err := b.Cursor.Source.Validate(); err != nil {
		return fmt.Errorf("failed to validate amm pool batch: %w", err)
	}
	if !json.Valid(b.Cursor.Position) || string(b.Cursor.Position) == "null" || b.Cursor.UpdatedAt.IsZero() || b.Cursor.Revision == ^uint64(0) {
		return fmt.Errorf("failed to validate amm pool batch: cursor=invalid")
	}
	for _, s := range b.Snapshots {
		if err := s.Identity.Validate(); err != nil {
			return fmt.Errorf("failed to validate amm pool batch: %w", err)
		}
		if !sameScope(s.Identity, b.Cursor.Source) || !validText(s.Token0ID, 65535) || !validText(s.Token1ID, 65535) || !json.Valid(s.State) || string(s.State) == "null" || s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
			return fmt.Errorf("failed to validate amm pool batch: snapshot=invalid")
		}
	}
	for _, e := range b.Events {
		if err := e.Pool.Validate(); err != nil {
			return fmt.Errorf("failed to validate amm pool batch: %w", err)
		}
		if e.Source != b.Cursor.Source || !sameScope(e.Pool, e.Source) || !validText(e.PositionID, 128) || !validText(e.TransactionID, 128) || !validText(e.Index, 128) || !validText(e.Type, 64) || !json.Valid(e.Payload) || e.OccurredAt.IsZero() || e.ObservedAt.IsZero() {
			return fmt.Errorf("failed to validate amm pool batch: event=invalid")
		}
	}
	return nil
}
func sameScope(i Identity, s Source) bool {
	return i.ChainFamily == s.ChainFamily && i.Chain == s.Chain && i.Network == s.Network && i.Venue == s.Venue
}
