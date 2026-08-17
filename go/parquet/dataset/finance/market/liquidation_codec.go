// liquidation_codec.go
package market

import (
	"context"
	"errors"
	"fmt"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	parquetgo "github.com/parquet-go/parquet-go"
	"io"
)

type LiquidationCodec struct{}
type liquidationRow struct {
	EventTimestamp    int64   `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp int64   `parquet:"received_timestamp,timestamp(microsecond)"`
	LiquidationID     string  `parquet:"liquidation_id"`
	Side              string  `parquet:"side"`
	Price             float64 `parquet:"price"`
	Quantity          float64 `parquet:"quantity"`
}

// NewLiquidationCodec creates a Liquidation Parquet codec.
//
// Version:
//   - 2026-08-16: Added.
func NewLiquidationCodec() *LiquidationCodec { return &LiquidationCodec{} }

// NewBatchReader opens a bounded Liquidation Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - Liquidation batch reader.
//
// Version:
//   - 2026-08-18: Added.
func (*LiquidationCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[Liquidation], error) {
	return newBatchReader(ctx, source, size, liquidationFromRow)
}

// NewBatchWriter creates a bounded Liquidation Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - Liquidation batch writer.
//
// Version:
//   - 2026-08-18: Added.
func (*LiquidationCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[Liquidation], error) {
	return newBatchWriter(ctx, destination, liquidationToRow)
}

// Encode encodes Liquidation records as Apache Parquet.
//
// Version:
//   - 2026-08-16: Added.
func (*LiquidationCodec) Encode(ctx context.Context, destination io.Writer, records []Liquidation) error {
	rows := make([]liquidationRow, len(records))
	for i, r := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[i] = liquidationToRow(r)
	}
	writer := parquetgo.NewGenericWriter[liquidationRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write Liquidation rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close Liquidation Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into Liquidation records.
//
// Version:
//   - 2026-08-16: Added.
func (*LiquidationCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]Liquidation, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open Liquidation Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[liquidationRow](file)
	defer reader.Close()
	rows := make([]liquidationRow, reader.NumRows())
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
			return nil, fmt.Errorf("read Liquidation rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d Liquidation rows, want %d", read, len(rows))
	}
	records := make([]Liquidation, len(rows))
	for i, r := range rows {
		records[i] = liquidationFromRow(r)
	}
	return records, nil
}

func liquidationToRow(record Liquidation) liquidationRow {
	return liquidationRow{
		EventTimestamp:    record.EventTimestamp.UnixMicro(),
		ReceivedTimestamp: record.ReceivedTimestamp.UnixMicro(),
		LiquidationID:     record.LiquidationID,
		Side:              record.Side,
		Price:             record.Price,
		Quantity:          record.Quantity,
	}
}

func liquidationFromRow(row liquidationRow) Liquidation {
	return Liquidation{
		EventTimestamp:    timeFromUnixMicro(row.EventTimestamp),
		ReceivedTimestamp: timeFromUnixMicro(row.ReceivedTimestamp),
		LiquidationID:     row.LiquidationID,
		Side:              row.Side,
		Price:             row.Price,
		Quantity:          row.Quantity,
	}
}
