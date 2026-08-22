// open_interest.go
package market

import "time"

type OpenInterestUnit string

const (
	OpenInterestUnitBaseAsset     OpenInterestUnit = "base_asset"
	OpenInterestUnitContracts     OpenInterestUnit = "contracts"
	OpenInterestUnitQuoteNotional OpenInterestUnit = "quote_notional"
)

type OpenInterestPriceType string

const (
	OpenInterestPriceTypeMark          OpenInterestPriceType = "mark"
	OpenInterestPriceTypeIndex         OpenInterestPriceType = "index"
	OpenInterestPriceTypeOracle        OpenInterestPriceType = "oracle"
	OpenInterestPriceTypeVenueReported OpenInterestPriceType = "venue_reported"
)

// OpenInterest is one normalized open-interest observation.
type OpenInterest struct {
	EventTimestamp      time.Time
	ReceivedTimestamp   time.Time
	RawQuantity         float64
	RawUnit             OpenInterestUnit
	Quantity            float64
	NotionalValue       float64
	NotionalCurrency    string
	ConversionPrice     *float64
	ConversionPriceType OpenInterestPriceType
	ContractSize        *float64
}
