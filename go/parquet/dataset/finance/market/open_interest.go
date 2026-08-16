// open_interest.go
package market

import "time"

// OpenInterest is one normalized open-interest observation.
type OpenInterest struct {
	EventTimestamp    time.Time
	ReceivedTimestamp time.Time
	Quantity          float64
	NotionalValue     float64
}
