// liquidation_dataset.go
package market

import (
	"context"
	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type LiquidationDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}
type LiquidationWriteParams = dataset.WriteParams[Liquidation]
type LiquidationReadParams = dataset.ReadParams
type LiquidationWriteResult = dataset.WriteResult
type LiquidationReadResult = dataset.ReadResult[Liquidation]
type LiquidationDataset struct{ dataset *dataset.Dataset[Liquidation] }

// NewLiquidationDataset creates a forced-liquidation event dataset.
//
// Version:
//   - 2026-08-16: Added.
func NewLiquidationDataset(c *client.Client, params LiquidationDatasetParams) (*LiquidationDataset, error) {
	value, err := dataset.New(c, NewLiquidationCodec(), dataset.Params{Root: params.Root, PartitionColumns: intradayPartitionColumns(), FileName: params.FileName, WriteMode: params.WriteMode})
	if err != nil {
		return nil, err
	}
	return &LiquidationDataset{dataset: value}, nil
}

// Write writes one Liquidation Parquet part.
//
// Version:
//   - 2026-08-16: Added.
func (d *LiquidationDataset) Write(ctx context.Context, params LiquidationWriteParams) (LiquidationWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads Liquidation Parquet parts matching a partition.
//
// Version:
//   - 2026-08-16: Added.
func (d *LiquidationDataset) Read(ctx context.Context, params LiquidationReadParams) (LiquidationReadResult, error) {
	return d.dataset.Read(ctx, params)
}
