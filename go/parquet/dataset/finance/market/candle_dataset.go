// candle_dataset.go
package market

import (
	"context"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type CandleDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}

type CandleWriteParams = dataset.WriteParams[Candle]
type CandleReadParams = dataset.ReadParams
type CandleCompactParams = dataset.CompactParams
type CandleWriteResult = dataset.WriteResult
type CandleReadResult = dataset.ReadResult[Candle]
type CandleCompactResult = dataset.CompactResult

type CandleDataset struct {
	dataset *dataset.Dataset[Candle]
}

// NewCandleDataset creates a standard OHLCV Candle dataset.
//
// Version:
//   - 2026-08-14: Added.
func NewCandleDataset(c *client.Client, params CandleDatasetParams) (*CandleDataset, error) {
	value, err := dataset.New(c, NewCandleCodec(), dataset.Params{
		Root:             params.Root,
		PartitionColumns: []string{"asset_class", "venue", "instrument_type", "symbol", "timeframe", "date"},
		FileName:         params.FileName,
		WriteMode:        params.WriteMode,
	})
	if err != nil {
		return nil, err
	}
	return &CandleDataset{dataset: value}, nil
}

// Write writes one Candle Parquet part.
//
// Version:
//   - 2026-08-14: Added.
func (d *CandleDataset) Write(ctx context.Context, params CandleWriteParams) (CandleWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads Candle Parquet parts matching a partition.
//
// Version:
//   - 2026-08-14: Added.
func (d *CandleDataset) Read(ctx context.Context, params CandleReadParams) (CandleReadResult, error) {
	return d.dataset.Read(ctx, params)
}

// Compact compacts immutable Candle Parquet parts in one partition.
//
// Parameters:
//   - ctx: Context for the operation.
//   - params: Partition and target output size.
//
// Returns:
//   - Compaction result.
//
// Version:
//   - 2026-08-18: Added.
func (d *CandleDataset) Compact(ctx context.Context, params CandleCompactParams) (CandleCompactResult, error) {
	return d.dataset.Compact(ctx, params)
}
