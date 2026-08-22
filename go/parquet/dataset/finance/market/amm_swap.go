package market

import "time"

// AMMSwap is one normalized spot swap executed against an AMM pool.
type AMMSwap struct {
	EventTimestamp      time.Time
	ReceivedTimestamp   time.Time
	SwapID              string
	Chain               string
	PoolID              string
	TransactionID       string
	EventIndex          string
	StateReferenceType  *string
	StateReferenceValue *string
	Side                string
	Price               float64
	BaseQuantity        float64
	QuoteQuantity       float64
	EffectiveFeeRate    *float64
}
