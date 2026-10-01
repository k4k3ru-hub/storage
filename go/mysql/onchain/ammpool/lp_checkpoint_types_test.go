package ammpool

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func lpCheckpointExample() LPCheckpointSaveParams {
	return LPCheckpointSaveParams{
		Source:                 Source{"evm", "base", "mainnet", "uniswap-v4", "manager"},
		ExpectedCursorRevision: 1, ExpectedParentRevision: 1,
		Checkpoint: LPCheckpoint{
			Pool:            Identity{"evm", "base", "mainnet", "uniswap-v4", "pool"},
			CreationEventID: digest("creation"), FormatVersion: 1,
			Position: LPCheckpointPosition{Kind: "block", ID: "block-hash"},
			Payload:  json.RawMessage(`{"amount":"340282366920938463463374607431768211455"}`), UpdatedAt: time.Now().UTC(),
		},
	}
}

// TestLPCheckpointValidation verifies bounded payloads, opaque positions and source ownership.
//
// Version:
//   - 2026-10-01: Added.
func TestLPCheckpointValidation(t *testing.T) {
	p := lpCheckpointExample()
	zero := "0"
	p.Checkpoint.Position.Index = &zero
	if err := validateLPCheckpointSave(p); err != nil {
		t.Fatal("zero block and first event are valid", err)
	}
	for _, mutate := range []func(*LPCheckpointSaveParams){
		func(p *LPCheckpointSaveParams) { p.Checkpoint.Payload = nil },
		func(p *LPCheckpointSaveParams) { p.Checkpoint.Payload = json.RawMessage(`[]`) },
		func(p *LPCheckpointSaveParams) { p.Checkpoint.Payload = json.RawMessage(`null`) },
		func(p *LPCheckpointSaveParams) { p.Checkpoint.Payload = json.RawMessage(`{"n":`) },
		func(p *LPCheckpointSaveParams) { p.Checkpoint.CreationEventID = [32]byte{} },
		func(p *LPCheckpointSaveParams) { p.Checkpoint.FormatVersion = 0 },
		func(p *LPCheckpointSaveParams) { p.Checkpoint.Position.Index = new(string) },
		func(p *LPCheckpointSaveParams) { p.Checkpoint.Pool.Network = "other" },
		func(p *LPCheckpointSaveParams) { p.ExpectedCursorRevision = ^uint64(0) },
		func(p *LPCheckpointSaveParams) { p.ExpectedParentRevision = 0 },
	} {
		bad := p
		mutate(&bad)
		if err := validateLPCheckpointSave(bad); err == nil {
			t.Fatal("invalid save accepted")
		}
	}
	p.Checkpoint.Payload = json.RawMessage(`{"v":"` + strings.Repeat("x", MaxLPCheckpointBytes) + `"}`)
	if err := validateLPCheckpointSave(p); !errors.Is(err, ErrLPCheckpointCapacity) {
		t.Fatal("capacity error not inspectable", err)
	}
}

// TestLPCheckpointReferenceValidation rejects ambiguous reads and oversized explicit resets.
//
// Version:
//   - 2026-10-01: Added.
func TestLPCheckpointReferenceValidation(t *testing.T) {
	p := lpCheckpointExample()
	ref := LPCheckpointRef{Pool: p.Checkpoint.Pool, Revision: 2}
	if err := validateLPCheckpointRefs(p.Source, []LPCheckpointRef{ref}); err != nil {
		t.Fatal(err)
	}
	for _, refs := range [][]LPCheckpointRef{nil, {ref, ref}, {{Pool: ref.Pool}}, make([]LPCheckpointRef, MaxLPCheckpointBatch+1)} {
		if err := validateLPCheckpointRefs(p.Source, refs); err == nil {
			t.Fatal("invalid read accepted")
		}
	}
	b := Batch{Cursor: Cursor{Source: p.Source}, ResetLPCheckpoints: []Identity{ref.Pool}}
	if err := b.validateLPCheckpointResets(); err != nil {
		t.Fatal("standalone reset rejected", err)
	}
	b.ResetLPCheckpoints = append(b.ResetLPCheckpoints, ref.Pool)
	if err := b.validateLPCheckpointResets(); err == nil {
		t.Fatal("duplicate reset accepted")
	}
	b.ResetLPCheckpoints = make([]Identity, MaxLPCheckpointBatch+1)
	if err := b.validateLPCheckpointResets(); err == nil {
		t.Fatal("unbounded reset accepted")
	}
}
