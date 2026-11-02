package market

import (
	"context"
	"fmt"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	pq "github.com/parquet-go/parquet-go"
	"io"
)

type fundingRateObservationRow struct {
	EventTimestamp    int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	FundingTimestamp  *int64   `parquet:"funding_timestamp,optional,timestamp(microsecond)"`
	Rate              float64  `parquet:"rate"`
	Kind              string   `parquet:"kind"`
	IntervalMinutes   int32    `parquet:"interval_minutes"`
	MarkPrice         *float64 `parquet:"mark_price,optional"`
	IndexPrice        *float64 `parquet:"index_price,optional"`
	PremiumRate       *float64 `parquet:"premium_rate,optional"`
	Network           *string  `parquet:"network,optional"`
	SourceID          *string  `parquet:"source_id,optional"`
	CollectionRunID   *string  `parquet:"collection_run_id,optional"`
}

type FundingRateObservationCodec struct{}

// NewFundingRateObservationCodec creates the explicitly versioned observation codec; legacy codecs are unchanged.
//
// Version:
//   - 2026-11-02: Added.
func NewFundingRateObservationCodec() *FundingRateObservationCodec {
	return &FundingRateObservationCodec{}
}

// NewBatchReader reads v2 or recognized legacy records, preserving unknown legacy context.
//
// Version:
//   - 2026-11-02: Added.
func (c *FundingRateObservationCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[FundingRateObservation], error) {
	if err := validateObservationSchema(ctx, source, size, pq.SchemaOf(new(fundingRateObservationRow)), pq.SchemaOf(new(fundingRateRow)), true); err != nil {
		return nil, err
	}
	return newBatchReader(ctx, source, size, fundingRateObservationFromRow)
}

// NewBatchWriter validates new observations before encoding optional provenance columns.
//
// Version:
//   - 2026-11-02: Added.
func (c *FundingRateObservationCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[FundingRateObservation], error) {
	inner, err := newBatchWriter(ctx, destination, fundingRateObservationToRow)
	if err != nil {
		return nil, err
	}
	return &validatedObservationWriter[FundingRateObservation]{inner: inner, validate: func(v FundingRateObservation) error { return v.Validate() }}, nil
}

// Encode writes only complete v2 observations.
//
// Version:
//   - 2026-11-02: Added.
func (c *FundingRateObservationCodec) Encode(ctx context.Context, destination io.Writer, records []FundingRateObservation) error {
	w, err := c.NewBatchWriter(ctx, destination)
	if err != nil {
		return err
	}
	if err = w.Write(ctx, records); err != nil {
		if closeErr := w.Close(); closeErr != nil {
			return fmt.Errorf("failed to encode observation: %w: close_error=%w", err, closeErr)
		}
		return err
	}
	return w.Close()
}

// Decode decodes v2 or recognized legacy observations without inventing provenance.
//
// Version:
//   - 2026-11-02: Added.
func (c *FundingRateObservationCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]FundingRateObservation, error) {
	r, err := c.NewBatchReader(ctx, source, size)
	if err != nil {
		return nil, err
	}
	return readObservationRecords(ctx, r)
}

func fundingRateObservationToRow(r FundingRateObservation) fundingRateObservationRow {
	v := fundingRateToRow(r.Value)
	return fundingRateObservationRow{
		EventTimestamp:    v.EventTimestamp,
		ReceivedTimestamp: v.ReceivedTimestamp,
		FundingTimestamp:  v.FundingTimestamp,
		Rate:              v.Rate,
		Kind:              v.Kind,
		IntervalMinutes:   v.IntervalMinutes,
		MarkPrice:         v.MarkPrice,
		IndexPrice:        v.IndexPrice,
		PremiumRate:       v.PremiumRate,
		Network:           r.Network, SourceID: r.SourceID, CollectionRunID: r.CollectionRunID,
	}
}
func fundingRateObservationFromRow(r fundingRateObservationRow) FundingRateObservation {
	v := fundingRateRow{
		EventTimestamp:    r.EventTimestamp,
		ReceivedTimestamp: r.ReceivedTimestamp,
		FundingTimestamp:  r.FundingTimestamp,
		Rate:              r.Rate,
		Kind:              r.Kind,
		IntervalMinutes:   r.IntervalMinutes,
		MarkPrice:         r.MarkPrice,
		IndexPrice:        r.IndexPrice,
		PremiumRate:       r.PremiumRate,
	}
	return FundingRateObservation{Value: fundingRateFromRow(v), ObservationContext: ObservationContext{Network: r.Network, SourceID: r.SourceID, CollectionRunID: r.CollectionRunID}}
}
