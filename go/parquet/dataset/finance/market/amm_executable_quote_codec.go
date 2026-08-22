package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type AMMExecutableQuoteCodec struct{}

type ammExecutableQuoteRow struct {
	EventTimestamp      int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp   int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	Chain               string   `parquet:"chain"`
	PoolID              string   `parquet:"pool_id"`
	StateReferenceType  *string  `parquet:"state_reference_type,optional"`
	StateReferenceValue *string  `parquet:"state_reference_value,optional"`
	BidPrice            float64  `parquet:"bid_price"`
	BidQuantity         float64  `parquet:"bid_quantity"`
	AskPrice            float64  `parquet:"ask_price"`
	AskQuantity         float64  `parquet:"ask_quantity"`
	EffectiveFeeRate    *float64 `parquet:"effective_fee_rate,optional"`
	FeeIncluded         bool     `parquet:"fee_included"`
}

// NewAMMExecutableQuoteCodec creates an AMM executable quote Parquet codec.
//
// Version:
//   - 2026-08-22: Added.
func NewAMMExecutableQuoteCodec() *AMMExecutableQuoteCodec {
	return &AMMExecutableQuoteCodec{}
}

// NewBatchReader opens a bounded AMM executable quote Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - AMM executable quote batch reader.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMExecutableQuoteCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[AMMExecutableQuote], error) {
	return newBatchReader(ctx, source, size, ammExecutableQuoteFromRow)
}

// NewBatchWriter creates a bounded AMM executable quote Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - AMM executable quote batch writer.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMExecutableQuoteCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[AMMExecutableQuote], error) {
	return newBatchWriter(ctx, destination, ammExecutableQuoteToRow)
}

// Encode encodes AMM executable quote records as Apache Parquet.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMExecutableQuoteCodec) Encode(ctx context.Context, destination io.Writer, records []AMMExecutableQuote) error {
	rows := make([]ammExecutableQuoteRow, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[index] = ammExecutableQuoteToRow(record)
	}
	writer := parquetgo.NewGenericWriter[ammExecutableQuoteRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write AMM executable quote rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close AMM executable quote Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into AMM executable quote records.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMExecutableQuoteCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]AMMExecutableQuote, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open AMM executable quote Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[ammExecutableQuoteRow](file)
	defer reader.Close()
	rows := make([]ammExecutableQuoteRow, reader.NumRows())
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
			return nil, fmt.Errorf("read AMM executable quote rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d AMM executable quote rows, want %d", read, len(rows))
	}
	records := make([]AMMExecutableQuote, len(rows))
	for index, row := range rows {
		records[index] = ammExecutableQuoteFromRow(row)
	}
	return records, nil
}

func ammExecutableQuoteToRow(record AMMExecutableQuote) ammExecutableQuoteRow {
	return ammExecutableQuoteRow{
		EventTimestamp:      record.EventTimestamp.UnixMicro(),
		ReceivedTimestamp:   record.ReceivedTimestamp.UnixMicro(),
		Chain:               record.Chain,
		PoolID:              record.PoolID,
		StateReferenceType:  record.StateReferenceType,
		StateReferenceValue: record.StateReferenceValue,
		BidPrice:            record.BidPrice,
		BidQuantity:         record.BidQuantity,
		AskPrice:            record.AskPrice,
		AskQuantity:         record.AskQuantity,
		EffectiveFeeRate:    record.EffectiveFeeRate,
		FeeIncluded:         record.FeeIncluded,
	}
}

func ammExecutableQuoteFromRow(row ammExecutableQuoteRow) AMMExecutableQuote {
	return AMMExecutableQuote{
		EventTimestamp:      timeFromUnixMicro(row.EventTimestamp),
		ReceivedTimestamp:   timeFromUnixMicro(row.ReceivedTimestamp),
		Chain:               row.Chain,
		PoolID:              row.PoolID,
		StateReferenceType:  row.StateReferenceType,
		StateReferenceValue: row.StateReferenceValue,
		BidPrice:            row.BidPrice,
		BidQuantity:         row.BidQuantity,
		AskPrice:            row.AskPrice,
		AskQuantity:         row.AskQuantity,
		EffectiveFeeRate:    row.EffectiveFeeRate,
		FeeIncluded:         row.FeeIncluded,
	}
}
