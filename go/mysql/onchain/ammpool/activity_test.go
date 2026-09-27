package ammpool

import (
	"encoding/json"
	"testing"
	"time"
)

// TestActivityBatchValidation requires atomic parent ownership and bounded unique minute values.
//
// Version:
//   - 2026-09-27: Added.
func TestActivityBatchValidation(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	pool := Identity{"evm", "base", "mainnet", "uniswap-v4", "pool"}
	parent := Snapshot{Identity: pool, Token0ID: "a", Token1ID: "b", CreatedAt: now, UpdatedAt: now, State: json.RawMessage(`{}`)}
	m := ActivityMinute{Pool: pool, Start: now, UpdatedAt: now, Totals: json.RawMessage(` {"count":"9007199254740993"}`)}
	base := Batch{Cursor: Cursor{Source: Source{"evm", "base", "mainnet", "uniswap-v4", "factory"}, Position: json.RawMessage(`{}`), UpdatedAt: now}, Snapshots: []Snapshot{parent}, ActivityMinutes: []ActivityMinute{m}, ResetActivity: []Identity{pool}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*Batch){
		func(b *Batch) { b.Snapshots = nil },
		func(b *Batch) { b.ActivityMinutes = append(b.ActivityMinutes, m) },
		func(b *Batch) { b.ActivityMinutes[0].Start = now.Add(time.Second) },
		func(b *Batch) { b.ActivityMinutes[0].Totals = json.RawMessage(`null`) },
		func(b *Batch) { b.ActivityMinutes[0].Pool.Network = "other" },
		func(b *Batch) { b.ResetActivity = append(b.ResetActivity, pool) },
	} {
		b := base
		b.ActivityMinutes = append([]ActivityMinute(nil), base.ActivityMinutes...)
		edit(&b)
		if err := b.Validate(); err == nil {
			t.Fatal("invalid activity batch accepted")
		}
	}
}
