// bbo.go
package market

import "time"

type BBO struct {
	EventTimestamp    time.Time
	ReceivedTimestamp time.Time
	BidPrice          float64
	BidQuantity       float64
	AskPrice          float64
	AskQuantity       float64
}
