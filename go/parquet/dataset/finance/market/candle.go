//
// candle.go
//
package market

import "time"

type Candle struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

func timeFromUnixMicro(value int64) time.Time {
	return time.UnixMicro(value).UTC()
}
