package market

import "time"

type OrderBookLevel struct {
	Price    string
	Quantity string
}

type OrderBook struct {
	EventTimestamp     time.Time
	ReceivedTimestamp  time.Time
	PublishedTimestamp time.Time
	Chain              string
	VenueSymbol        string
	VenueSequence      string
	Version            uint64
	Depth              uint32
	Bids               []OrderBookLevel
	Asks               []OrderBookLevel
}
