package market

import (
	"context"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type OrderBookDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}

type OrderBookWriteParams = dataset.WriteParams[OrderBook]
type OrderBookReadParams = dataset.ReadParams
type OrderBookCompactParams = dataset.CompactParams
type OrderBookWriteResult = dataset.WriteResult
type OrderBookReadResult = dataset.ReadResult[OrderBook]
type OrderBookCompactResult = dataset.CompactResult

type OrderBookDataset struct {
	dataset *dataset.Dataset[OrderBook]
}

// NewOrderBookDataset creates a venue OrderBook snapshot dataset.
//
// Version:
//   - 2026-09-05: Added.
func NewOrderBookDataset(c *client.Client, params OrderBookDatasetParams) (*OrderBookDataset, error) {
	value, err := dataset.NewWithCompactionPolicy(c, NewOrderBookCodec(), dataset.Params{
		Root: params.Root, PartitionColumns: intradayPartitionColumns(), FileName: params.FileName, WriteMode: params.WriteMode,
	}, orderBookCompactionPolicy{})
	if err != nil {
		return nil, err
	}
	return &OrderBookDataset{dataset: value}, nil
}

// Write writes one OrderBook Parquet part.
//
// Version:
//   - 2026-09-05: Added.
func (d *OrderBookDataset) Write(ctx context.Context, params OrderBookWriteParams) (OrderBookWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads OrderBook Parquet parts matching a partition.
//
// Version:
//   - 2026-09-05: Added.
func (d *OrderBookDataset) Read(ctx context.Context, params OrderBookReadParams) (OrderBookReadResult, error) {
	return d.dataset.Read(ctx, params)
}

// Compact compacts immutable OrderBook Parquet parts in one partition.
//
// Version:
//   - 2026-09-05: Added.
func (d *OrderBookDataset) Compact(ctx context.Context, params OrderBookCompactParams) (OrderBookCompactResult, error) {
	return d.dataset.Compact(ctx, params)
}
