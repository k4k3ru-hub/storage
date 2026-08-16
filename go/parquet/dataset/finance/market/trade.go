// trade.go
package market

import "time"

// Trade is one normalized public market execution. Side is the aggressor side.
type Trade struct {
	EventTimestamp    time.Time
	ReceivedTimestamp time.Time
	TradeID           string
	Side              string
	Price             float64
	Quantity          float64
}
