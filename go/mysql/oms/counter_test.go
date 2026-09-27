package oms

import (
	"encoding/json"
	"errors"
	"testing"
)

func counterOrder() Order {
	o := testOrder()
	o.SpecificationVersion = 2
	o.Specification = json.RawMessage(`{"counterQuantityAsset":{"namespace":"onchain","chain":"sui","network":"testnet","assetId":"sui:testnet:USDC","symbol":"USDC","decimals":6}}`)
	return o
}

// TestCounterReplay verifies precision, unknown values, corrections and reversals.
//
// Version:
//   - 2026-09-27: Added.
func TestCounterReplay(t *testing.T) {
	o := counterOrder()
	a := acceptance(1, 1)
	f := fill(2, 1, 1, "fill", "30")
	f.Event.OrderCounterQuantity = ptr("9007199254740993.000001")
	check := func(records []OnchainEvent, want *string) {
		t.Helper()
		p, err := ReplayOrder(o, sequence(records...))
		must(t, err)
		if !sameString(p.State.FilledCounterQuantity, want) {
			t.Fatalf("counter=%v want=%v", p.State.FilledCounterQuantity, want)
		}
		for i := range records {
			records[i].Event.Sequence = uint64(i + 1)
		}
		s, err := projectExecution(1, records, p)
		must(t, err)
		if !sameString(s.FilledCounterQuantity, want) {
			t.Fatalf("execution counter=%v want=%v", s.FilledCounterQuantity, want)
		}
	}
	check([]OnchainEvent{a}, ptr("0"))
	check([]OnchainEvent{a, f}, f.Event.OrderCounterQuantity)
	u := fill(3, 1, 1, "unknown", "10")
	check([]OnchainEvent{a, f, u}, nil)
	c := u
	c.Event.ID = 4
	c.Event.RecordKey = []byte("correct")
	c.Event.EventType = EventTypeFillCorrected
	c.Event.ReferenceEventID = &u.Event.ID
	c.Event.OrderCounterQuantity = ptr("0.999999")
	check([]OnchainEvent{a, f, u, c}, ptr("9007199254740994"))
	r := fact(5, 1, 1, EventTypeFillReversed, "reverse")
	r.Event.ReferenceEventID = &f.Event.ID
	check([]OnchainEvent{a, f, u, c, r}, c.Event.OrderCounterQuantity)
	r2 := fact(6, 1, 1, EventTypeFillReversed, "reverse2")
	r2.Event.ReferenceEventID = &c.Event.ID
	check([]OnchainEvent{a, f, u, c, r, r2}, ptr("0"))
	// Scope comes from immutable metadata, never the previous snapshot value.
	o.FilledCounterQuantity = nil
	check([]OnchainEvent{a}, ptr("0"))
	o.Specification = json.RawMessage(`{"counterQuantityAsset":null}`)
	check([]OnchainEvent{a}, nil)
	f.Event.OrderCounterQuantity = nil
	check([]OnchainEvent{a, f}, nil)
	f.Event.OrderCounterQuantity = ptr("1")
	if _, err := ReplayOrder(o, sequence(a, f)); !errors.Is(err, ErrConflict) {
		t.Fatal("unsupported contribution accepted", err)
	}
	o = counterOrder()
	f.Event.CounterAssetID = ptr("another-token")
	if _, err := ReplayOrder(o, sequence(a, f)); !errors.Is(err, ErrConflict) {
		t.Fatal("mixed assets accepted", err)
	}
	o = counterOrder()
	f.Event.CounterAssetID = ptr("sui:testnet:USDC")
	f.Event.OrderCounterQuantity = ptr("0.0000001")
	if _, err := ReplayOrder(o, sequence(a, f)); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("excess precision accepted", err)
	}
}
