package market

import "time"

// AMMSwap is a normalized live swap observation or its cancellation.
// Removed records retain the original amounts; they are not new executions.
type AMMSwap struct {
	EventTimestamp      time.Time
	ReceivedTimestamp   time.Time
	SwapID              string
	Chain               string
	Network             *string
	BlockHash           *string
	Removed             bool
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
