package market

import (
	"context"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type AMMPoolMetadataDatasetParams struct {
	Root string
}

type AMMPoolMetadataWriteParams = dataset.WriteParams[AMMPoolMetadata]
type AMMPoolMetadataReadParams = dataset.ReadParams
type AMMPoolMetadataCompactParams = dataset.CompactParams
type AMMPoolMetadataWriteResult = dataset.WriteResult
type AMMPoolMetadataReadResult = dataset.ReadResult[AMMPoolMetadata]
type AMMPoolMetadataCompactResult = dataset.CompactResult

type AMMPoolMetadataDataset struct {
	dataset *dataset.Dataset[AMMPoolMetadata]
}

// NewAMMPoolMetadataDataset creates an append-only AMM pool metadata dataset.
//
// Parameters:
//   - c: Parquet client.
//   - params: Dataset parameters.
//
// Returns:
//   - AMM pool metadata dataset.
//   - Creation error.
//
// Version:
//   - 2026-08-22: Added.
func NewAMMPoolMetadataDataset(c *client.Client, params AMMPoolMetadataDatasetParams) (*AMMPoolMetadataDataset, error) {
	value, err := dataset.New(c, NewAMMPoolMetadataCodec(), dataset.Params{
		Root:             params.Root,
		PartitionColumns: ammPoolMetadataPartitionColumns(),
		WriteMode:        dataset.WriteModeCreate,
	})
	if err != nil {
		return nil, err
	}
	return &AMMPoolMetadataDataset{dataset: value}, nil
}

// Write writes one immutable AMM pool metadata Parquet part.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMPoolMetadataDataset) Write(ctx context.Context, params AMMPoolMetadataWriteParams) (AMMPoolMetadataWriteResult, error) {
	return d.dataset.Write(ctx, params)
}

// Read reads AMM pool metadata Parquet parts matching a partition.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMPoolMetadataDataset) Read(ctx context.Context, params AMMPoolMetadataReadParams) (AMMPoolMetadataReadResult, error) {
	return d.dataset.Read(ctx, params)
}

// Compact compacts immutable AMM pool metadata parts in one partition.
//
// Version:
//   - 2026-08-22: Added.
func (d *AMMPoolMetadataDataset) Compact(ctx context.Context, params AMMPoolMetadataCompactParams) (AMMPoolMetadataCompactResult, error) {
	return d.dataset.Compact(ctx, params)
}
