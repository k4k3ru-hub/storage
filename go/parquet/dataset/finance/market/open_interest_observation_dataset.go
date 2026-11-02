package market

import (
	"context"
	"fmt"
	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type OpenInterestObservationDatasetParams struct {
	Root                          string
	BeforeCompactionInputDeletion func(context.Context, dataset.CompactResult) error
}
type OpenInterestObservationDataset struct {
	dataset *dataset.Dataset[OpenInterestObservation]
}

// NewOpenInterestObservationDataset composes isolated v2 storage with provenance-aware exact deduplication.
//
// Version:
//   - 2026-11-02: Added.
func NewOpenInterestObservationDataset(c *client.Client, p OpenInterestObservationDatasetParams) (*OpenInterestObservationDataset, error) {
	d, err := dataset.NewWithCompactionPolicy(c, NewOpenInterestObservationCodec(), dataset.Params{Root: p.Root, PartitionColumns: intradayPartitionColumns(), BeforeCompactionInputDeletion: p.BeforeCompactionInputDeletion}, openInterestObservationCompactionPolicy{})
	if err != nil {
		return nil, err
	}
	return &OpenInterestObservationDataset{dataset: d}, nil
}

// Write validates and stores a v2 observation part.
//
// Version:
//   - 2026-11-02: Added.
func (d *OpenInterestObservationDataset) Write(ctx context.Context, p dataset.WriteParams[OpenInterestObservation]) (dataset.WriteResult, error) {
	for _, r := range p.Records {
		if err := r.Validate(); err != nil {
			return dataset.WriteResult{}, fmt.Errorf("failed to write observation: %w", err)
		}
	}
	return d.dataset.Write(ctx, p)
}

// Read reads v2 observations from the composed root.
//
// Version:
//   - 2026-11-02: Added.
func (d *OpenInterestObservationDataset) Read(ctx context.Context, p dataset.ReadParams) (dataset.ReadResult[OpenInterestObservation], error) {
	return d.dataset.Read(ctx, p)
}

// Compact compacts only the composed v2 root and publishes provenance before deleting inputs.
//
// Version:
//   - 2026-11-02: Added.
func (d *OpenInterestObservationDataset) Compact(ctx context.Context, p dataset.CompactParams) (dataset.CompactResult, error) {
	return d.dataset.Compact(ctx, p)
}

type openInterestObservationCompactionPolicy struct{}

// CompactSingleObject permits complete low-frequency v2 hours with one input.
//
// Version:
//   - 2026-11-02: Added.
func (openInterestObservationCompactionPolicy) CompactSingleObject() bool { return true }

// Compare orders source context before economic values.
//
// Version:
//   - 2026-11-02: Added.
func (openInterestObservationCompactionPolicy) Compare(a, b OpenInterestObservation) int {
	if v := compareObservationContext(a.ObservationContext, b.ObservationContext); v != 0 {
		return v
	}
	return (openInterestCompactionPolicy{}).Compare(a.Value, b.Value)
}

// DeduplicationKey preserves different sources, networks and collection runs.
//
// Version:
//   - 2026-11-02: Added.
func (openInterestObservationCompactionPolicy) DeduplicationKey(r OpenInterestObservation) (string, bool) {
	key, _ := (openInterestCompactionPolicy{}).DeduplicationKey(r.Value)
	return observationContextKey(r.ObservationContext) + ":" + key, true
}
