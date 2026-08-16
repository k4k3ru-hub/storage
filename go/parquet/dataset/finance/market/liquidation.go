// liquidation.go
package market

import "time"

// Liquidation is one normalized public forced-liquidation execution.
type Liquidation struct {
	EventTimestamp    time.Time
	ReceivedTimestamp time.Time
	LiquidationID     string
	Side              string
	Price             float64
	Quantity          float64
}
