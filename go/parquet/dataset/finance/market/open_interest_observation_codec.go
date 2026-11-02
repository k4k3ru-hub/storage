package market

import (
	"context"
	"fmt"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	pq "github.com/parquet-go/parquet-go"
	"io"
)

type openInterestObservationRow struct {
	EventTimestamp           int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp        int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	RawQuantity              float64  `parquet:"raw_quantity"`
	RawUnit                  string   `parquet:"raw_unit"`
	Quantity                 float64  `parquet:"quantity"`
	NotionalValue            float64  `parquet:"notional_value"`
	NotionalCurrency         string   `parquet:"notional_currency"`
	ConversionPrice          *float64 `parquet:"conversion_price,optional"`
	ConversionPriceType      string   `parquet:"conversion_price_type"`
	ConversionPriceTimestamp *int64   `parquet:"conversion_price_timestamp,timestamp(microsecond),optional"`
	ContractSize             *float64 `parquet:"contract_size,optional"`
	ContractSizeUnit         *string  `parquet:"contract_size_unit,optional"`
	ContractSizeCurrency     *string  `parquet:"contract_size_currency,optional"`
	Network                  *string  `parquet:"network,optional"`
	SourceID                 *string  `parquet:"source_id,optional"`
	CollectionRunID          *string  `parquet:"collection_run_id,optional"`
}

type OpenInterestObservationCodec struct{}

// NewOpenInterestObservationCodec creates the explicitly versioned observation codec; legacy codecs are unchanged.
//
// Version:
//   - 2026-11-02: Added.
func NewOpenInterestObservationCodec() *OpenInterestObservationCodec {
	return &OpenInterestObservationCodec{}
}

// NewBatchReader reads v2 or recognized legacy records, preserving unknown legacy context.
//
// Version:
//   - 2026-11-02: Added.
func (c *OpenInterestObservationCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[OpenInterestObservation], error) {
	if err := validateObservationSchema(ctx, source, size, pq.SchemaOf(new(openInterestObservationRow)), pq.SchemaOf(new(openInterestRow)), false); err != nil {
		return nil, err
	}
	return newBatchReader(ctx, source, size, openInterestObservationFromRow)
}

// NewBatchWriter validates new observations before encoding optional provenance columns.
//
// Version:
//   - 2026-11-02: Added.
func (c *OpenInterestObservationCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[OpenInterestObservation], error) {
	inner, err := newBatchWriter(ctx, destination, openInterestObservationToRow)
	if err != nil {
		return nil, err
	}
	return &validatedObservationWriter[OpenInterestObservation]{inner: inner, validate: func(v OpenInterestObservation) error { return v.Validate() }}, nil
}

// Encode writes only complete v2 observations.
//
// Version:
//   - 2026-11-02: Added.
func (c *OpenInterestObservationCodec) Encode(ctx context.Context, destination io.Writer, records []OpenInterestObservation) error {
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
func (c *OpenInterestObservationCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]OpenInterestObservation, error) {
	r, err := c.NewBatchReader(ctx, source, size)
	if err != nil {
		return nil, err
	}
	return readObservationRecords(ctx, r)
}

func openInterestObservationToRow(r OpenInterestObservation) openInterestObservationRow {
	v := openInterestToRow(r.Value)
	return openInterestObservationRow{
		EventTimestamp:           v.EventTimestamp,
		ReceivedTimestamp:        v.ReceivedTimestamp,
		RawQuantity:              v.RawQuantity,
		RawUnit:                  v.RawUnit,
		Quantity:                 v.Quantity,
		NotionalValue:            v.NotionalValue,
		NotionalCurrency:         v.NotionalCurrency,
		ConversionPrice:          v.ConversionPrice,
		ConversionPriceType:      v.ConversionPriceType,
		ConversionPriceTimestamp: v.ConversionPriceTimestamp,
		ContractSize:             v.ContractSize,
		ContractSizeUnit:         v.ContractSizeUnit,
		ContractSizeCurrency:     v.ContractSizeCurrency,
		Network:                  r.Network, SourceID: r.SourceID, CollectionRunID: r.CollectionRunID,
	}
}
func openInterestObservationFromRow(r openInterestObservationRow) OpenInterestObservation {
	v := openInterestRow{
		EventTimestamp:           r.EventTimestamp,
		ReceivedTimestamp:        r.ReceivedTimestamp,
		RawQuantity:              r.RawQuantity,
		RawUnit:                  r.RawUnit,
		Quantity:                 r.Quantity,
		NotionalValue:            r.NotionalValue,
		NotionalCurrency:         r.NotionalCurrency,
		ConversionPrice:          r.ConversionPrice,
		ConversionPriceType:      r.ConversionPriceType,
		ConversionPriceTimestamp: r.ConversionPriceTimestamp,
		ContractSize:             r.ContractSize,
		ContractSizeUnit:         r.ContractSizeUnit,
		ContractSizeCurrency:     r.ContractSizeCurrency,
	}
	return OpenInterestObservation{Value: openInterestFromRow(v), ObservationContext: ObservationContext{Network: r.Network, SourceID: r.SourceID, CollectionRunID: r.CollectionRunID}}
}
