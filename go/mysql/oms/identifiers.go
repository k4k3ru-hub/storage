package oms

import (
	"encoding/json"
	"fmt"
)

// ExecutionIdentifiers identifies an external order independently of the public execution ID.
type ExecutionIdentifiers struct {
	VenueOrderID  *string `json:"venueOrderId,omitempty"`
	ClientOrderID *string `json:"clientOrderId,omitempty"`
}

type executionIdentifierEvidence struct {
	Version uint16 `json:"version"`
	ExecutionIdentifiers
}

func (v ExecutionIdentifiers) validate() error {
	if v.VenueOrderID == nil && v.ClientOrderID == nil {
		return invalid("execution_identifiers", "empty")
	}
	if err := optionalText("venue_order_id", v.VenueOrderID, 255); err != nil {
		return err
	}
	return optionalText("client_order_id", v.ClientOrderID, 255)
}

// ExecutionIdentifiers reads optional typed identifiers from immutable protocol evidence.
// Missing or null executionIdentifiers metadata leaves both snapshot identifiers unset.
//
// Version:
//   - 2026-09-28: Added.
func (d OnchainEvidence) ExecutionIdentifiers() (*ExecutionIdentifiers, error) {
	const op = "failed to read oms execution identifiers"
	if err := jsonObject("protocol_data", d.ProtocolData, false); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if len(d.ProtocolData) == 0 {
		return nil, nil
	}
	var envelope struct {
		Identifiers *executionIdentifierEvidence `json:"executionIdentifiers"`
	}
	if err := json.Unmarshal(d.ProtocolData, &envelope); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if envelope.Identifiers == nil {
		return nil, nil
	}
	v := envelope.Identifiers
	if v.Version != 1 {
		return nil, fmt.Errorf("%s: %w", op, invalid("execution_identifiers_version", "invalid"))
	}
	if err := v.ExecutionIdentifiers.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return &v.ExecutionIdentifiers, nil
}

// WithExecutionIdentifiers copies evidence with typed external order identifiers.
// It preserves unrelated protocol fields and does not mutate the receiver or its bytes.
// Existing identifier evidence must match; new observations belong in a new event.
//
// Version:
//   - 2026-09-28: Added.
func (d OnchainEvidence) WithExecutionIdentifiers(ids ExecutionIdentifiers) (OnchainEvidence, error) {
	const op = "failed to encode oms execution identifiers"
	if err := ids.validate(); err != nil {
		return d, fmt.Errorf("%s: %w", op, err)
	}
	existing, err := d.ExecutionIdentifiers()
	if err != nil {
		return d, fmt.Errorf("%s: %w", op, err)
	}
	if existing != nil && (!sameString(existing.VenueOrderID, ids.VenueOrderID) || !sameString(existing.ClientOrderID, ids.ClientOrderID)) {
		return d, fmt.Errorf("%s: %w: execution_identifiers=invalid", op, ErrConflict)
	}
	envelope := map[string]json.RawMessage{}
	if len(d.ProtocolData) > 0 {
		if err := json.Unmarshal(d.ProtocolData, &envelope); err != nil {
			return d, fmt.Errorf("%s: %w", op, err)
		}
	}
	encoded, err := json.Marshal(executionIdentifierEvidence{Version: 1, ExecutionIdentifiers: ids})
	if err != nil {
		return d, fmt.Errorf("%s: %w", op, err)
	}
	envelope["executionIdentifiers"] = encoded
	d.ProtocolData, err = json.Marshal(envelope)
	if err != nil {
		return d, fmt.Errorf("%s: %w", op, err)
	}
	return d, nil
}

func projectExecutionIdentifiers(snapshot *Execution, record OnchainEvent) error {
	if record.Onchain == nil {
		return nil
	}
	ids, err := record.Onchain.ExecutionIdentifiers()
	if err != nil {
		return err
	}
	if ids == nil {
		return nil
	}
	if ids.ClientOrderID != nil {
		if record.Event.EventType != EventTypeSubmissionAccepted && !sameString(snapshot.ClientOrderID, ids.ClientOrderID) {
			return fmt.Errorf("failed to project oms execution identifiers: %w: client_order_id=invalid", ErrConflict)
		}
		snapshot.ClientOrderID = ids.ClientOrderID
	}
	if ids.VenueOrderID != nil {
		if snapshot.VenueOrderID != nil && !sameString(snapshot.VenueOrderID, ids.VenueOrderID) {
			return fmt.Errorf("failed to project oms execution identifiers: %w: venue_order_id=invalid", ErrConflict)
		}
		snapshot.VenueOrderID = ids.VenueOrderID
	}
	return nil
}
