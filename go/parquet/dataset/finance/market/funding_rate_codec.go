// funding_rate_codec.go
package market

import (
	"context"
	"errors"
	"fmt"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	parquetgo "github.com/parquet-go/parquet-go"
	"io"
	"time"
)

type FundingRateCodec struct{}
type fundingRateRow struct {
	EventTimestamp    int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	FundingTimestamp  *int64   `parquet:"funding_timestamp,optional,timestamp(microsecond)"`
	Rate              float64  `parquet:"rate"`
	Kind              string   `parquet:"kind"`
	IntervalMinutes   int32    `parquet:"interval_minutes"`
	MarkPrice         *float64 `parquet:"mark_price,optional"`
	IndexPrice        *float64 `parquet:"index_price,optional"`
	PremiumRate       *float64 `parquet:"premium_rate,optional"`
}

// NewFundingRateCodec creates a FundingRate Parquet codec.
//
// Version:
//   - 2026-10-22: Preserve absent funding times as null and read legacy required timestamps.
//   - 2026-08-16: Added.
//   - 2026-08-19: Distinguished rate kinds and funding intervals.
func NewFundingRateCodec() *FundingRateCodec { return &FundingRateCodec{} }

// NewBatchReader opens a bounded FundingRate Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - FundingRate batch reader.
//
// Version:
//   - 2026-10-22: Preserve absent funding times as null and read legacy required timestamps.
//   - 2026-08-19: Added funding kind, interval, and optional market context.
//   - 2026-08-18: Added.
func (*FundingRateCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[FundingRate], error) {
	return newBatchReaderWithSchemaFingerprint(ctx, source, size, fundingRateFromRow, fundingRateSchemaFingerprint)
}

// NewBatchWriter creates a bounded FundingRate Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - FundingRate batch writer.
//
// Version:
//   - 2026-10-22: Preserve absent funding times as null and read legacy required timestamps.
//   - 2026-08-19: Added funding kind, interval, and optional market context.
//   - 2026-08-18: Added.
func (*FundingRateCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[FundingRate], error) {
	return newBatchWriter(ctx, destination, fundingRateToRow)
}

// Encode encodes FundingRate records as Apache Parquet.
//
// Version:
//   - 2026-10-22: Preserve absent funding times as null and read legacy required timestamps.
//   - 2026-08-19: Added funding kind, interval, and optional market context.
//   - 2026-08-16: Added.
func (*FundingRateCodec) Encode(ctx context.Context, destination io.Writer, records []FundingRate) error {
	rows := make([]fundingRateRow, len(records))
	for i, r := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[i] = fundingRateToRow(r)
	}
	writer := parquetgo.NewGenericWriter[fundingRateRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write FundingRate rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close FundingRate Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into FundingRate records.
//
// Version:
//   - 2026-10-22: Preserve absent funding times as null and read legacy required timestamps.
//   - 2026-08-19: Added funding kind, interval, and optional market context.
//   - 2026-08-16: Added.
func (*FundingRateCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]FundingRate, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open FundingRate Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[fundingRateRow](file)
	defer reader.Close()
	rows := make([]fundingRateRow, reader.NumRows())
	read := 0
	for read < len(rows) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := reader.Read(rows[read:])
		read += n
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read FundingRate rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d FundingRate rows, want %d", read, len(rows))
	}
	records := make([]FundingRate, len(rows))
	for i, r := range rows {
		records[i] = fundingRateFromRow(r)
	}
	return records, nil
}

func fundingRateToRow(record FundingRate) fundingRateRow {
	var fundingTimestamp *int64
	if !record.FundingTimestamp.IsZero() {
		value := record.FundingTimestamp.UnixMicro()
		fundingTimestamp = &value
	}
	return fundingRateRow{
		EventTimestamp:    record.EventTimestamp.UnixMicro(),
		ReceivedTimestamp: record.ReceivedTimestamp.UnixMicro(),
		FundingTimestamp:  fundingTimestamp,
		Rate:              record.Rate,
		Kind:              string(record.Kind),
		IntervalMinutes:   record.IntervalMinutes,
		MarkPrice:         record.MarkPrice,
		IndexPrice:        record.IndexPrice,
		PremiumRate:       record.PremiumRate,
	}
}

func fundingRateFromRow(row fundingRateRow) FundingRate {
	var fundingTimestamp time.Time
	if row.FundingTimestamp != nil {
		fundingTimestamp = timeFromUnixMicro(*row.FundingTimestamp)
	}
	return FundingRate{
		EventTimestamp:    timeFromUnixMicro(row.EventTimestamp),
		ReceivedTimestamp: timeFromUnixMicro(row.ReceivedTimestamp),
		FundingTimestamp:  fundingTimestamp,
		Rate:              row.Rate,
		Kind:              FundingRateKind(row.Kind),
		IntervalMinutes:   row.IntervalMinutes,
		MarkPrice:         row.MarkPrice,
		IndexPrice:        row.IndexPrice,
		PremiumRate:       row.PremiumRate,
	}
}

// Only the approved required-to-optional funding time change is compatible.
// Keep type, logical unit, field set and all other nullability checks intact.
func fundingRateSchemaFingerprint(schema *parquetgo.Schema) string {
	actual, expected := parquetgo.Group{}, parquetgo.Group{}
	for _, field := range schema.Fields() {
		var node parquetgo.Node = field
		if field.Name() == "funding_timestamp" && field.Required() {
			node = parquetgo.Optional(field)
		}
		actual[field.Name()] = node
	}
	for _, field := range parquetgo.SchemaOf(new(fundingRateRow)).Fields() {
		expected[field.Name()] = field
	}
	normalized := parquetgo.NewSchema("funding_rate", actual).String()
	target := parquetgo.NewSchema("funding_rate", expected).String()
	if normalized == target {
		return fingerprint(target)
	}
	return fingerprint(schema.String())
}
