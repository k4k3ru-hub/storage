package oms

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestExecutionIdentifierEvidence verifies optional identifiers, strict values and immutable evidence construction.
//
// Version:
//   - 2026-09-28: Added.
func TestExecutionIdentifierEvidence(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`{}`), json.RawMessage(`{"executionIdentifiers":null}`)} {
		ids, err := (OnchainEvidence{ProtocolData: raw}).ExecutionIdentifiers()
		must(t, err)
		if ids != nil {
			t.Fatal("missing identifiers became a value")
		}
	}
	d := OnchainEvidence{ProtocolData: json.RawMessage(`{"sourceVersion":18446744073709551615,"adapter":{"value":"preserved"}}`)}
	before := bytes.Clone(d.ProtocolData)
	ids := ExecutionIdentifiers{VenueOrderID: ptr(strings.Repeat("v", 255)), ClientOrderID: ptr("CaseSensitive-Client")}
	encoded, err := d.WithExecutionIdentifiers(ids)
	must(t, err)
	if !bytes.Equal(before, d.ProtocolData) {
		t.Fatal("input evidence mutated")
	}
	var envelope map[string]json.RawMessage
	must(t, json.Unmarshal(encoded.ProtocolData, &envelope))
	if string(envelope["sourceVersion"]) != "18446744073709551615" || string(envelope["adapter"]) != `{"value":"preserved"}` {
		t.Fatal("adapter evidence lost precision or contents")
	}
	got, err := encoded.ExecutionIdentifiers()
	must(t, err)
	if !sameString(got.VenueOrderID, ids.VenueOrderID) || !sameString(got.ClientOrderID, ids.ClientOrderID) {
		t.Fatal("identifiers lost")
	}
	_, err = encoded.WithExecutionIdentifiers(ids)
	must(t, err)
	if _, err = encoded.WithExecutionIdentifiers(ExecutionIdentifiers{ClientOrderID: ptr("different")}); !errors.Is(err, ErrConflict) {
		t.Fatal("existing evidence overwritten")
	}
	for _, value := range []string{"", " leading", "trailing ", "internal space", "\n", "日本語", strings.Repeat("x", 256)} {
		for _, candidate := range []ExecutionIdentifiers{{VenueOrderID: &value}, {ClientOrderID: &value}} {
			if _, err = d.WithExecutionIdentifiers(candidate); !errors.Is(err, ErrInvalidParameter) {
				t.Fatal("invalid external identifier accepted")
			}
		}
	}
	for _, raw := range []string{
		`null`, `[]`, `{"executionIdentifiers":{}}`,
		`{"executionIdentifiers":{"version":2,"venueOrderId":"v"}}`,
		`{"executionIdentifiers":{"version":1}}`,
		`{"executionIdentifiers":{"version":1,"venueOrderId":""}}`,
	} {
		if _, err = (OnchainEvidence{ProtocolData: json.RawMessage(raw)}).ExecutionIdentifiers(); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal("invalid identifier evidence accepted")
		}
	}
	for _, raw := range []string{`{"executionIdentifiers":5}`, `{"executionIdentifiers":{"version":1,"venueOrderId":5}}`} {
		_, err = (OnchainEvidence{ProtocolData: json.RawMessage(raw)}).ExecutionIdentifiers()
		var typed *json.UnmarshalTypeError
		if !errors.As(err, &typed) {
			t.Fatal("underlying JSON type error lost")
		}
	}
}

// TestPositionOrderValidation verifies that group roots may self-reference without relaxing parent validation.
//
// Version:
//   - 2026-09-28: Added.
func TestPositionOrderValidation(t *testing.T) {
	o := testOrder()
	o.PositionOrderID = &o.ID
	must(t, o.Validate())
	o.PositionOrderID = ptr(uint64(0))
	if !errors.Is(o.Validate(), ErrInvalidParameter) {
		t.Fatal("zero representative accepted")
	}
	o.PositionOrderID = nil
	o.ParentOrderID = &o.ID
	if !errors.Is(o.Validate(), ErrInvalidParameter) {
		t.Fatal("parent self-reference accepted")
	}
}

// TestExecutionIdentifierProjection verifies client acceptance, stable venue identity and omission semantics.
//
// Version:
//   - 2026-09-28: Added.
func TestExecutionIdentifierProjection(t *testing.T) {
	makeRecord := func(kind EventType, ids ExecutionIdentifiers) OnchainEvent {
		d, err := (OnchainEvidence{}).WithExecutionIdentifiers(ids)
		must(t, err)
		return OnchainEvent{Event: Event{EventType: kind}, Onchain: &d}
	}
	snapshot := Execution{ExecutionID: "public-execution"}
	client := makeRecord(EventTypeSubmissionAccepted, ExecutionIdentifiers{ClientOrderID: ptr("client")})
	must(t, projectExecutionIdentifiers(&snapshot, client))
	venue := makeRecord(EventTypeSubmitted, ExecutionIdentifiers{VenueOrderID: ptr("venue")})
	must(t, projectExecutionIdentifiers(&snapshot, venue))
	must(t, projectExecutionIdentifiers(&snapshot, OnchainEvent{}))
	must(t, projectExecutionIdentifiers(&snapshot, venue))
	if snapshot.ExecutionID != "public-execution" || *snapshot.ClientOrderID != "client" || *snapshot.VenueOrderID != "venue" {
		t.Fatal("identifier projection changed public identity or cleared optional values")
	}
	for _, ids := range []ExecutionIdentifiers{{VenueOrderID: ptr("other")}, {ClientOrderID: ptr("other")}} {
		if err := projectExecutionIdentifiers(&snapshot, makeRecord(EventTypeSubmitted, ids)); !errors.Is(err, ErrConflict) {
			t.Fatal("identifier reassignment accepted")
		}
	}
	if err := projectExecutionIdentifiers(&Execution{}, makeRecord(EventTypeSubmitted, ExecutionIdentifiers{ClientOrderID: ptr("late")})); !errors.Is(err, ErrConflict) {
		t.Fatal("client ID first recorded after submission")
	}
}
