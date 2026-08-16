// open_interest_dataset.go
package market

import (
	"context"
	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type OpenInterestDatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}
type OpenInterestWriteParams = dataset.WriteParams[OpenInterest]
type OpenInterestReadParams = dataset.ReadParams
type OpenInterestWriteResult = dataset.WriteResult
type OpenInterestReadResult = dataset.ReadResult[OpenInterest]
type OpenInterestDataset struct {
	dataset *dataset.Dataset[OpenInterest]
}

// NewOpenInterestDataset creates an open-interest observation dataset.
//
// Version:
//   - 2026-08-16: Added.
func NewOpenInterestDataset(c *client.Client, params OpenInterestDatasetParams) (*OpenInterestDataset, error) {
	value, err := dataset.New(c, NewOpenInterestCodec(), dataset.Params{Root: params.Root, PartitionColumns: intradayPartitionColumns(), FileName: params.FileName, WriteMode: params.WriteMode})
	if err != nil {
		return nil, err
	}
	return &OpenInterestDataset{dataset: value}, nil
}

// Write writes one OpenInterest Parquet part.
//
// Version:
//   - 2026-08-16: Added.
func (d *OpenInterestDataset) Write(ctx context.Context, params OpenInterestWriteParams) (OpenInterestWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads OpenInterest Parquet parts matching a partition.
//
// Version:
//   - 2026-08-16: Added.
func (d *OpenInterestDataset) Read(ctx context.Context, params OpenInterestReadParams) (OpenInterestReadResult, error) {
	return d.dataset.Read(ctx, params)
}
