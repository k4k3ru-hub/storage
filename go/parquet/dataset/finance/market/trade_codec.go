// trade_codec.go
package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type TradeCodec struct{}

type tradeRow struct {
	EventTimestamp    int64   `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp int64   `parquet:"received_timestamp,timestamp(microsecond)"`
	TradeID           string  `parquet:"trade_id"`
	Side              string  `parquet:"side"`
	Price             float64 `parquet:"price"`
	Quantity          float64 `parquet:"quantity"`
}

// NewTradeCodec creates a Trade Parquet codec.
//
// Version:
//   - 2026-08-16: Added.
func NewTradeCodec() *TradeCodec { return &TradeCodec{} }

// NewBatchReader opens a bounded Trade Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - Trade batch reader.
//
// Version:
//   - 2026-08-18: Added.
func (*TradeCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[Trade], error) {
	return newBatchReader(ctx, source, size, tradeFromRow)
}

// NewBatchWriter creates a bounded Trade Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - Trade batch writer.
//
// Version:
//   - 2026-08-18: Added.
func (*TradeCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[Trade], error) {
	return newBatchWriter(ctx, destination, tradeToRow)
}

// Encode encodes Trade records as Apache Parquet.
//
// Version:
//   - 2026-08-16: Added.
func (*TradeCodec) Encode(ctx context.Context, destination io.Writer, records []Trade) error {
	rows := make([]tradeRow, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[index] = tradeToRow(record)
	}
	writer := parquetgo.NewGenericWriter[tradeRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write Trade rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close Trade Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into Trade records.
//
// Version:
//   - 2026-08-16: Added.
func (*TradeCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]Trade, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open Trade Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[tradeRow](file)
	defer reader.Close()
	rows := make([]tradeRow, reader.NumRows())
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
			return nil, fmt.Errorf("read Trade rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d Trade rows, want %d", read, len(rows))
	}
	records := make([]Trade, len(rows))
	for index, row := range rows {
		records[index] = tradeFromRow(row)
	}
	return records, nil
}

func tradeToRow(record Trade) tradeRow {
	return tradeRow{
		EventTimestamp:    record.EventTimestamp.UnixMicro(),
		ReceivedTimestamp: record.ReceivedTimestamp.UnixMicro(),
		TradeID:           record.TradeID,
		Side:              record.Side,
		Price:             record.Price,
		Quantity:          record.Quantity,
	}
}

func tradeFromRow(row tradeRow) Trade {
	return Trade{
		EventTimestamp:    timeFromUnixMicro(row.EventTimestamp),
		ReceivedTimestamp: timeFromUnixMicro(row.ReceivedTimestamp),
		TradeID:           row.TradeID,
		Side:              row.Side,
		Price:             row.Price,
		Quantity:          row.Quantity,
	}
}
