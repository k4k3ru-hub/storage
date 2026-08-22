package market

import "time"

// AMMExecutableQuote is one quantity-dependent executable quote observed from an AMM pool.
type AMMExecutableQuote struct {
	EventTimestamp      time.Time
	ReceivedTimestamp   time.Time
	Chain               string
	PoolID              string
	StateReferenceType  *string
	StateReferenceValue *string
	BidPrice            float64
	BidQuantity         float64
	AskPrice            float64
	AskQuantity         float64
	EffectiveFeeRate    *float64
	FeeIncluded         bool
}
