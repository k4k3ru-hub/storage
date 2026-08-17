// candle_codec.go
package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type CandleCodec struct{}

type candleRow struct {
	Timestamp int64   `parquet:"timestamp,timestamp(microsecond)"`
	Open      float64 `parquet:"open"`
	High      float64 `parquet:"high"`
	Low       float64 `parquet:"low"`
	Close     float64 `parquet:"close"`
	Volume    float64 `parquet:"volume"`
}

// NewCandleCodec creates a Candle Parquet codec.
//
// Version:
//   - 2026-08-14: Added.
func NewCandleCodec() *CandleCodec {
	return &CandleCodec{}
}

// NewBatchReader opens a bounded Candle Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - Candle batch reader.
//
// Version:
//   - 2026-08-18: Added.
func (*CandleCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[Candle], error) {
	return newBatchReader(ctx, source, size, candleFromRow)
}

// NewBatchWriter creates a bounded Candle Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - Candle batch writer.
//
// Version:
//   - 2026-08-18: Added.
func (*CandleCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[Candle], error) {
	return newBatchWriter(ctx, destination, candleToRow)
}

// Encode encodes Candle records as Apache Parquet.
//
// Version:
//   - 2026-08-14: Added.
func (*CandleCodec) Encode(ctx context.Context, destination io.Writer, records []Candle) error {
	rows := make([]candleRow, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[index] = candleToRow(record)
	}

	writer := parquetgo.NewGenericWriter[candleRow](
		destination,
		parquetgo.Compression(&parquetgo.Zstd),
	)
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write Candle rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close Candle Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into Candle records.
//
// Version:
//   - 2026-08-14: Added.
func (*CandleCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]Candle, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open Candle Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[candleRow](file)
	defer reader.Close()

	rows := make([]candleRow, reader.NumRows())
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
			return nil, fmt.Errorf("read Candle rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d Candle rows, want %d", read, len(rows))
	}

	records := make([]Candle, len(rows))
	for index, row := range rows {
		records[index] = candleFromRow(row)
	}
	return records, nil
}

func candleToRow(record Candle) candleRow {
	return candleRow{
		Timestamp: record.Timestamp.UnixMicro(),
		Open:      record.Open,
		High:      record.High,
		Low:       record.Low,
		Close:     record.Close,
		Volume:    record.Volume,
	}
}

func candleFromRow(row candleRow) Candle {
	return Candle{
		Timestamp: timeFromUnixMicro(row.Timestamp),
		Open:      row.Open,
		High:      row.High,
		Low:       row.Low,
		Close:     row.Close,
		Volume:    row.Volume,
	}
}
