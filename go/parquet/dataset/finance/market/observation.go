package market

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ObservationContext identifies an explicitly scoped collection lifetime.
// Nil fields represent legacy inputs only; new observation writers require all fields.
type ObservationContext struct {
	Network         *string
	SourceID        *string
	CollectionRunID *string
}

// Validate validates complete context for a new observation.
//
// Version:
//   - 2026-11-02: Added.
func (c ObservationContext) Validate() error {
	if c.Network == nil || c.SourceID == nil || c.CollectionRunID == nil {
		return fmt.Errorf("failed to validate observation context: context=null")
	}
	if *c.Network == "" || len(*c.Network) > 16 || strings.TrimSpace(*c.Network) != *c.Network || strings.ContainsAny(*c.Network, "\x00\r\n\t ") {
		return fmt.Errorf("failed to validate observation context: network=invalid")
	}
	decoded, err := hex.DecodeString(*c.SourceID)
	if err != nil || len(decoded) != 32 || strings.ToLower(*c.SourceID) != *c.SourceID {
		return fmt.Errorf("failed to validate observation context: source_id=invalid")
	}
	if *c.CollectionRunID == "" || len(*c.CollectionRunID) > 128 || strings.TrimSpace(*c.CollectionRunID) != *c.CollectionRunID || strings.ContainsAny(*c.CollectionRunID, "/\\\x00\r\n\t ") {
		return fmt.Errorf("failed to validate observation context: collection_run_id=invalid")
	}
	return nil
}

type FundingRateObservation struct {
	Value FundingRate
	ObservationContext
}

// Validate validates a funding observation without changing its economic values.
//
// Version:
//   - 2026-11-02: Added.
func (r FundingRateObservation) Validate() error {
	if err := r.Value.Validate(); err != nil {
		return fmt.Errorf("failed to validate funding observation: %w", err)
	}
	if err := r.ObservationContext.Validate(); err != nil {
		return fmt.Errorf("failed to validate funding observation: %w", err)
	}
	return nil
}

type OpenInterestObservation struct {
	Value OpenInterest
	ObservationContext
}

// Validate validates an open-interest observation without changing its economic values.
//
// Version:
//   - 2026-11-02: Added.
func (r OpenInterestObservation) Validate() error {
	if err := r.Value.Validate(); err != nil {
		return fmt.Errorf("failed to validate open interest observation: %w", err)
	}
	if err := r.ObservationContext.Validate(); err != nil {
		return fmt.Errorf("failed to validate open interest observation: %w", err)
	}
	return nil
}
