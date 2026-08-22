// partition.go
package market

const (
	AssetClassCrypto    = "crypto"
	AssetClassFX        = "fx"
	AssetClassCommodity = "commodity"
)

const (
	InstrumentTypeSpot      = "spot"
	InstrumentTypeCFD       = "cfd"
	InstrumentTypePerpetual = "perpetual"
	InstrumentTypeFuture    = "future"
	InstrumentTypeForward   = "forward"
)

func intradayPartitionColumns() []string {
	return []string{"asset_class", "venue", "instrument_type", "symbol", "date", "hour"}
}

func ammPoolMetadataPartitionColumns() []string {
	return []string{"asset_class", "venue"}
}
