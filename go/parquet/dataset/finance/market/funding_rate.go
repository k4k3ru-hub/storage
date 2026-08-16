// funding_rate.go
package market

import "time"

// FundingRate is one normalized perpetual funding observation.
type FundingRate struct {
	EventTimestamp    time.Time
	ReceivedTimestamp time.Time
	FundingTimestamp  time.Time
	Rate              float64
	PredictedRate     float64
	MarkPrice         float64
	IndexPrice        float64
}
