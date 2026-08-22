package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type AMMSwapCodec struct{}

type ammSwapRow struct {
	EventTimestamp      int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp   int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	SwapID              string   `parquet:"swap_id"`
	Chain               string   `parquet:"chain"`
	PoolID              string   `parquet:"pool_id"`
	TransactionID       string   `parquet:"transaction_id"`
	EventIndex          string   `parquet:"event_index"`
	StateReferenceType  *string  `parquet:"state_reference_type,optional"`
	StateReferenceValue *string  `parquet:"state_reference_value,optional"`
	Side                string   `parquet:"side"`
	Price               float64  `parquet:"price"`
	BaseQuantity        float64  `parquet:"base_quantity"`
	QuoteQuantity       float64  `parquet:"quote_quantity"`
	EffectiveFeeRate    *float64 `parquet:"effective_fee_rate,optional"`
}

// NewAMMSwapCodec creates an AMM swap Parquet codec.
//
// Version:
//   - 2026-08-22: Added.
func NewAMMSwapCodec() *AMMSwapCodec { return &AMMSwapCodec{} }

// NewBatchReader opens a bounded AMM swap Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - AMM swap batch reader.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMSwapCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[AMMSwap], error) {
	return newBatchReader(ctx, source, size, ammSwapFromRow)
}

// NewBatchWriter creates a bounded AMM swap Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - AMM swap batch writer.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMSwapCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[AMMSwap], error) {
	return newBatchWriter(ctx, destination, ammSwapToRow)
}

// Encode encodes AMM swap records as Apache Parquet.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMSwapCodec) Encode(ctx context.Context, destination io.Writer, records []AMMSwap) error {
	rows := make([]ammSwapRow, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[index] = ammSwapToRow(record)
	}
	writer := parquetgo.NewGenericWriter[ammSwapRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write AMM swap rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close AMM swap Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into AMM swap records.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMSwapCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]AMMSwap, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open AMM swap Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[ammSwapRow](file)
	defer reader.Close()
	rows := make([]ammSwapRow, reader.NumRows())
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
			return nil, fmt.Errorf("read AMM swap rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d AMM swap rows, want %d", read, len(rows))
	}
	records := make([]AMMSwap, len(rows))
	for index, row := range rows {
		records[index] = ammSwapFromRow(row)
	}
	return records, nil
}

func ammSwapToRow(record AMMSwap) ammSwapRow {
	return ammSwapRow{
		EventTimestamp: record.EventTimestamp.UnixMicro(), ReceivedTimestamp: record.ReceivedTimestamp.UnixMicro(),
		SwapID: record.SwapID, Chain: record.Chain, PoolID: record.PoolID,
		TransactionID: record.TransactionID, EventIndex: record.EventIndex,
		StateReferenceType: record.StateReferenceType, StateReferenceValue: record.StateReferenceValue,
		Side: record.Side, Price: record.Price, BaseQuantity: record.BaseQuantity, QuoteQuantity: record.QuoteQuantity,
		EffectiveFeeRate: record.EffectiveFeeRate,
	}
}

func ammSwapFromRow(row ammSwapRow) AMMSwap {
	return AMMSwap{
		EventTimestamp: timeFromUnixMicro(row.EventTimestamp), ReceivedTimestamp: timeFromUnixMicro(row.ReceivedTimestamp),
		SwapID: row.SwapID, Chain: row.Chain, PoolID: row.PoolID,
		TransactionID: row.TransactionID, EventIndex: row.EventIndex,
		StateReferenceType: row.StateReferenceType, StateReferenceValue: row.StateReferenceValue,
		Side: row.Side, Price: row.Price, BaseQuantity: row.BaseQuantity, QuoteQuantity: row.QuoteQuantity,
		EffectiveFeeRate: row.EffectiveFeeRate,
	}
}
