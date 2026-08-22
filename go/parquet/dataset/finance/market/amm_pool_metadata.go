package market

import "time"

// AMMPoolMetadata is one append-only revision of an AMM pool configuration.
type AMMPoolMetadata struct {
	MetadataID         string
	SupersedesID       *string
	ObservedTimestamp  time.Time
	EffectiveTimestamp *time.Time
	Chain              string
	PoolID             string
	BaseAssetID        string
	QuoteAssetID       string
	BaseDecimals       uint8
	QuoteDecimals      uint8
	FeeModel           string
	FeeRate            *float64
	PriceGridType      string
	TickSpacing        *uint32
	BinStep            *uint32
	Hooks              *string
	ConfigurationID    *string
	Fingerprint        string
}
