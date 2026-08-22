package market

import (
	"context"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type AMMExecutableQuoteDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}

type AMMExecutableQuoteWriteParams = dataset.WriteParams[AMMExecutableQuote]
type AMMExecutableQuoteReadParams = dataset.ReadParams
type AMMExecutableQuoteCompactParams = dataset.CompactParams
type AMMExecutableQuoteWriteResult = dataset.WriteResult
type AMMExecutableQuoteReadResult = dataset.ReadResult[AMMExecutableQuote]
type AMMExecutableQuoteCompactResult = dataset.CompactResult

type AMMExecutableQuoteDataset struct {
	dataset *dataset.Dataset[AMMExecutableQuote]
}

// NewAMMExecutableQuoteDataset creates a quantity-dependent AMM quote dataset.
//
// Parameters:
//   - c: Parquet client.
//   - params: Dataset parameters.
//
// Returns:
//   - AMM executable quote dataset.
//   - Creation error.
//
// Version:
//   - 2026-08-22: Added.
func NewAMMExecutableQuoteDataset(c *client.Client, params AMMExecutableQuoteDatasetParams) (*AMMExecutableQuoteDataset, error) {
	value, err := dataset.NewWithCompactionPolicy(c, NewAMMExecutableQuoteCodec(), dataset.Params{
		Root:             params.Root,
		PartitionColumns: intradayPartitionColumns(),
		FileName:         params.FileName,
		WriteMode:        params.WriteMode,
	}, ammExecutableQuoteCompactionPolicy{})
	if err != nil {
		return nil, err
	}
	return &AMMExecutableQuoteDataset{dataset: value}, nil
}

// Write writes one AMM executable quote Parquet part.
//
// Version:
//   - 2026-08-22: Applied chronological ordering and exact-record deduplication during compaction.
//   - 2026-08-22: Added.
func (d *AMMExecutableQuoteDataset) Write(ctx context.Context, params AMMExecutableQuoteWriteParams) (AMMExecutableQuoteWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads AMM executable quote Parquet parts matching a partition.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMExecutableQuoteDataset) Read(ctx context.Context, params AMMExecutableQuoteReadParams) (AMMExecutableQuoteReadResult, error) {
	return d.dataset.Read(ctx, params)
}

// Compact compacts immutable AMM executable quote parts in one partition.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMExecutableQuoteDataset) Compact(ctx context.Context, params AMMExecutableQuoteCompactParams) (AMMExecutableQuoteCompactResult, error) {
	return d.dataset.Compact(ctx, params)
}
