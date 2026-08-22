package market

import (
	"context"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type AMMSwapDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}

type AMMSwapWriteParams = dataset.WriteParams[AMMSwap]
type AMMSwapReadParams = dataset.ReadParams
type AMMSwapCompactParams = dataset.CompactParams
type AMMSwapWriteResult = dataset.WriteResult
type AMMSwapReadResult = dataset.ReadResult[AMMSwap]
type AMMSwapCompactResult = dataset.CompactResult

type AMMSwapDataset struct{ dataset *dataset.Dataset[AMMSwap] }

// NewAMMSwapDataset creates a spot AMM swap dataset.
//
// Parameters:
//   - c: Parquet client.
//   - params: Dataset parameters.
//
// Returns:
//   - AMM swap dataset.
//   - Creation error.
//
// Version:
//   - 2026-08-22: Added.
func NewAMMSwapDataset(c *client.Client, params AMMSwapDatasetParams) (*AMMSwapDataset, error) {
	value, err := dataset.NewWithCompactionPolicy(c, NewAMMSwapCodec(), dataset.Params{
		Root: params.Root, PartitionColumns: intradayPartitionColumns(), FileName: params.FileName, WriteMode: params.WriteMode,
	}, ammSwapCompactionPolicy{})
	if err != nil {
		return nil, err
	}
	return &AMMSwapDataset{dataset: value}, nil
}

// Write writes one AMM swap Parquet part.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMSwapDataset) Write(ctx context.Context, params AMMSwapWriteParams) (AMMSwapWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads AMM swap Parquet parts matching a partition.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMSwapDataset) Read(ctx context.Context, params AMMSwapReadParams) (AMMSwapReadResult, error) {
	return d.dataset.Read(ctx, params)
}

// Compact compacts immutable AMM swap parts in one partition.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMSwapDataset) Compact(ctx context.Context, params AMMSwapCompactParams) (AMMSwapCompactResult, error) {
	return d.dataset.Compact(ctx, params)
}
