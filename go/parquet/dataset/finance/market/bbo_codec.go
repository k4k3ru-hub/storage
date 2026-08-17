// bbo_codec.go
package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type BBOCodec struct{}

type bboRow struct {
	EventTimestamp    int64   `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp int64   `parquet:"received_timestamp,timestamp(microsecond)"`
	BidPrice          float64 `parquet:"bid_price"`
	BidQuantity       float64 `parquet:"bid_quantity"`
	AskPrice          float64 `parquet:"ask_price"`
	AskQuantity       float64 `parquet:"ask_quantity"`
}

// NewBBOCodec creates a BBO Parquet codec.
//
// Version:
//   - 2026-08-15: Added.
func NewBBOCodec() *BBOCodec {
	return &BBOCodec{}
}

// NewBatchReader opens a bounded BBO Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - BBO batch reader.
//
// Version:
//   - 2026-08-18: Added.
func (*BBOCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[BBO], error) {
	return newBatchReader(ctx, source, size, bboFromRow)
}

// NewBatchWriter creates a bounded BBO Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - BBO batch writer.
//
// Version:
//   - 2026-08-18: Added.
func (*BBOCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[BBO], error) {
	return newBatchWriter(ctx, destination, bboToRow)
}

// Encode encodes BBO records as Apache Parquet.
//
// Version:
//   - 2026-08-15: Added.
func (*BBOCodec) Encode(ctx context.Context, destination io.Writer, records []BBO) error {
	rows := make([]bboRow, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[index] = bboToRow(record)
	}

	writer := parquetgo.NewGenericWriter[bboRow](
		destination,
		parquetgo.Compression(&parquetgo.Zstd),
	)
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write BBO rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close BBO Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into BBO records.
//
// Version:
//   - 2026-08-15: Added.
func (*BBOCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]BBO, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open BBO Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[bboRow](file)
	defer reader.Close()

	rows := make([]bboRow, reader.NumRows())
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
			return nil, fmt.Errorf("read BBO rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d BBO rows, want %d", read, len(rows))
	}

	records := make([]BBO, len(rows))
	for index, row := range rows {
		records[index] = bboFromRow(row)
	}
	return records, nil
}

func bboToRow(record BBO) bboRow {
	return bboRow{
		EventTimestamp:    record.EventTimestamp.UnixMicro(),
		ReceivedTimestamp: record.ReceivedTimestamp.UnixMicro(),
		BidPrice:          record.BidPrice,
		BidQuantity:       record.BidQuantity,
		AskPrice:          record.AskPrice,
		AskQuantity:       record.AskQuantity,
	}
}

func bboFromRow(row bboRow) BBO {
	return BBO{
		EventTimestamp:    timeFromUnixMicro(row.EventTimestamp),
		ReceivedTimestamp: timeFromUnixMicro(row.ReceivedTimestamp),
		BidPrice:          row.BidPrice,
		BidQuantity:       row.BidQuantity,
		AskPrice:          row.AskPrice,
		AskQuantity:       row.AskQuantity,
	}
}
