// funding_rate_dataset.go
package market

import (
	"context"
	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type FundingRateDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}
type FundingRateWriteParams = dataset.WriteParams[FundingRate]
type FundingRateReadParams = dataset.ReadParams
type FundingRateWriteResult = dataset.WriteResult
type FundingRateReadResult = dataset.ReadResult[FundingRate]
type FundingRateDataset struct{ dataset *dataset.Dataset[FundingRate] }

// NewFundingRateDataset creates a perpetual funding-rate dataset.
//
// Version:
//   - 2026-08-16: Added.
func NewFundingRateDataset(c *client.Client, params FundingRateDatasetParams) (*FundingRateDataset, error) {
	value, err := dataset.New(c, NewFundingRateCodec(), dataset.Params{Root: params.Root, PartitionColumns: intradayPartitionColumns(), FileName: params.FileName, WriteMode: params.WriteMode})
	if err != nil {
		return nil, err
	}
	return &FundingRateDataset{dataset: value}, nil
}

// Write writes one FundingRate Parquet part.
//
// Version:
//   - 2026-08-16: Added.
func (d *FundingRateDataset) Write(ctx context.Context, params FundingRateWriteParams) (FundingRateWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads FundingRate Parquet parts matching a partition.
//
// Version:
//   - 2026-08-16: Added.
func (d *FundingRateDataset) Read(ctx context.Context, params FundingRateReadParams) (FundingRateReadResult, error) {
	return d.dataset.Read(ctx, params)
}
