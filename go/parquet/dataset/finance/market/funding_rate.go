// funding_rate.go
package market

import (
	"fmt"
	"math"
	"time"
)

type FundingRateKind string

const (
	FundingRateKindCurrentEstimate FundingRateKind = "current_estimate"
	FundingRateKindPredicted       FundingRateKind = "predicted"
	FundingRateKindSettled         FundingRateKind = "settled"
)

// FundingRate is one normalized perpetual funding observation.
type FundingRate struct {
	// EventTimestamp is when the venue generated or published the observation.
	EventTimestamp time.Time
	// ReceivedTimestamp is when K4K3RU received the observation.
	ReceivedTimestamp time.Time
	// FundingTimestamp is the settlement time targeted by an estimate or the
	// settlement time at which a settled rate was applied.
	FundingTimestamp time.Time
	// Rate is a decimal rate, where 0.0001 represents 0.01%.
	Rate float64
	// Kind identifies whether the rate is estimated, predicted, or settled.
	Kind FundingRateKind
	// IntervalMinutes is the funding interval in minutes.
	IntervalMinutes int32
	// MarkPrice is the venue-reported mark price accompanying the observation.
	MarkPrice *float64
	// IndexPrice is the venue-reported index price accompanying the observation.
	IndexPrice *float64
	// PremiumRate is the venue-reported premium as a decimal rate.
	PremiumRate *float64
}

// Validate validates a normalized perpetual funding observation.
//
// Returns:
//   - Validation error when a required value is missing or invalid.
//
// Version:
//   - 2026-08-26: Added.
func (r FundingRate) Validate() error {
	if r.EventTimestamp.IsZero() {
		return fmt.Errorf("failed to validate funding rate: event_timestamp=empty")
	}
	if r.ReceivedTimestamp.IsZero() {
		return fmt.Errorf("failed to validate funding rate: received_timestamp=empty")
	}
	if r.FundingTimestamp.IsZero() {
		return fmt.Errorf("failed to validate funding rate: funding_timestamp=empty")
	}
	if math.IsNaN(r.Rate) || math.IsInf(r.Rate, 0) {
		return fmt.Errorf("failed to validate funding rate: rate=invalid")
	}
	switch r.Kind {
	case FundingRateKindCurrentEstimate, FundingRateKindPredicted, FundingRateKindSettled:
	default:
		return fmt.Errorf("failed to validate funding rate: kind=invalid")
	}
	if r.IntervalMinutes <= 0 {
		return fmt.Errorf("failed to validate funding rate: interval_minutes=out_of_range min_value=1")
	}
	if r.MarkPrice != nil && (math.IsNaN(*r.MarkPrice) || math.IsInf(*r.MarkPrice, 0) || *r.MarkPrice <= 0) {
		return fmt.Errorf("failed to validate funding rate: mark_price=out_of_range min_value=0")
	}
	if r.IndexPrice != nil && (math.IsNaN(*r.IndexPrice) || math.IsInf(*r.IndexPrice, 0) || *r.IndexPrice <= 0) {
		return fmt.Errorf("failed to validate funding rate: index_price=out_of_range min_value=0")
	}
	if r.PremiumRate != nil && (math.IsNaN(*r.PremiumRate) || math.IsInf(*r.PremiumRate, 0)) {
		return fmt.Errorf("failed to validate funding rate: premium_rate=invalid")
	}
	return nil
}
