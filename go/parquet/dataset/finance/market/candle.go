// candle.go
package market

import "time"

type Candle struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	// Optional execution aggregates distinguish unavailable legacy values from zero.
	QuoteVolume       *float64
	BuyBaseVolume     *float64
	SellBaseVolume    *float64
	UnknownBaseVolume *float64
	TradeCount        *uint64
	Quality           *uint64
	VenueSymbol       string
	Chain             string
	Network           string
	PoolID            string
	RunID             string
	Version           uint64
}

// CandleSnapshotFilePrefix reserves mutable Candle files from immutable compaction.
const CandleSnapshotFilePrefix = "snapshot-"

func timeFromUnixMicro(value int64) time.Time {
	return time.UnixMicro(value).UTC()
}
