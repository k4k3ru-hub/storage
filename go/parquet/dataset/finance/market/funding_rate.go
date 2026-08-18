// funding_rate.go
package market

import "time"

type FundingRateKind string

const (
	FundingRateKindCurrentEstimate FundingRateKind = "current_estimate"
	FundingRateKindPredicted       FundingRateKind = "predicted"
	FundingRateKindSettled         FundingRateKind = "settled"
)

// FundingRate is one normalized perpetual funding observation.
type FundingRate struct {
	EventTimestamp     time.Time
	ReceivedTimestamp  time.Time
	EffectiveTimestamp time.Time
	Rate               float64
	Kind               FundingRateKind
	IntervalMinutes    int32
	MarkPrice          *float64
	IndexPrice         *float64
	PremiumRate        *float64
}
