// trade_dataset.go
package market

import (
	"context"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type TradeDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}
type TradeWriteParams = dataset.WriteParams[Trade]
type TradeReadParams = dataset.ReadParams
type TradeCompactParams = dataset.CompactParams
type TradeWriteResult = dataset.WriteResult
type TradeReadResult = dataset.ReadResult[Trade]
type TradeCompactResult = dataset.CompactResult
type TradeDataset struct{ dataset *dataset.Dataset[Trade] }

// NewTradeDataset creates a public market trade dataset.
//
// Version:
//   - 2026-08-16: Added.
func NewTradeDataset(c *client.Client, params TradeDatasetParams) (*TradeDataset, error) {
	value, err := dataset.New(c, NewTradeCodec(), dataset.Params{Root: params.Root, PartitionColumns: intradayPartitionColumns(), FileName: params.FileName, WriteMode: params.WriteMode})
	if err != nil {
		return nil, err
	}
	return &TradeDataset{dataset: value}, nil
}

// Write writes one Trade Parquet part.
//
// Version:
//   - 2026-08-16: Added.
func (d *TradeDataset) Write(ctx context.Context, params TradeWriteParams) (TradeWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads Trade Parquet parts matching a partition.
//
// Version:
//   - 2026-08-16: Added.
func (d *TradeDataset) Read(ctx context.Context, params TradeReadParams) (TradeReadResult, error) {
	return d.dataset.Read(ctx, params)
}

// Compact compacts immutable Trade Parquet parts in one partition.
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
func (d *TradeDataset) Compact(ctx context.Context, params TradeCompactParams) (TradeCompactResult, error) {
	return d.dataset.Compact(ctx, params)
}
