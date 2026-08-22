// bbo_dataset.go
package market

import (
	"context"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type BBODatasetParams struct {
	Root      string
	FileName  string
	WriteMode dataset.WriteMode
}

type BBOWriteParams = dataset.WriteParams[BBO]
type BBOReadParams = dataset.ReadParams
type BBOCompactParams = dataset.CompactParams
type BBOWriteResult = dataset.WriteResult
type BBOReadResult = dataset.ReadResult[BBO]
type BBOCompactResult = dataset.CompactResult

type BBODataset struct {
	dataset *dataset.Dataset[BBO]
}

// NewBBODataset creates a best bid and offer dataset.
//
// Version:
//   - 2026-08-15: Added.
func NewBBODataset(c *client.Client, params BBODatasetParams) (*BBODataset, error) {
	value, err := dataset.NewWithCompactionPolicy(c, NewBBOCodec(), dataset.Params{
		Root:             params.Root,
		PartitionColumns: intradayPartitionColumns(),
		FileName:         params.FileName,
		WriteMode:        params.WriteMode,
	}, bboCompactionPolicy{})
	if err != nil {
		return nil, err
	}
	return &BBODataset{dataset: value}, nil
}

// Write writes one BBO Parquet part.
//
// Version:
//   - 2026-08-15: Added.
func (d *BBODataset) Write(ctx context.Context, params BBOWriteParams) (BBOWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads BBO Parquet parts matching a partition.
//
// Version:
//   - 2026-08-15: Added.
func (d *BBODataset) Read(ctx context.Context, params BBOReadParams) (BBOReadResult, error) {
	return d.dataset.Read(ctx, params)
}

// Compact compacts immutable BBO Parquet parts in one partition.
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
func (d *BBODataset) Compact(ctx context.Context, params BBOCompactParams) (BBOCompactResult, error) {
	return d.dataset.Compact(ctx, params)
}
