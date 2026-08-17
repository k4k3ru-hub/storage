// open_interest_codec.go
package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	parquetgo "github.com/parquet-go/parquet-go"
)

type OpenInterestCodec struct{}
type openInterestRow struct {
	EventTimestamp    int64   `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp int64   `parquet:"received_timestamp,timestamp(microsecond)"`
	Quantity          float64 `parquet:"quantity"`
	NotionalValue     float64 `parquet:"notional_value"`
}

// NewOpenInterestCodec creates an OpenInterest Parquet codec.
//
// Version:
//   - 2026-08-16: Added.
func NewOpenInterestCodec() *OpenInterestCodec { return &OpenInterestCodec{} }

// NewBatchReader opens a bounded OpenInterest Parquet reader.
//
// Parameters:
//   - ctx: Context for the operation.
//   - source: Parquet source.
//   - size: Source size in bytes.
//
// Returns:
//   - OpenInterest batch reader.
//
// Version:
//   - 2026-08-18: Added.
func (*OpenInterestCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[OpenInterest], error) {
	return newBatchReader(ctx, source, size, openInterestFromRow)
}

// NewBatchWriter creates a bounded OpenInterest Parquet writer.
//
// Parameters:
//   - ctx: Context for the operation.
//   - destination: Parquet destination.
//
// Returns:
//   - OpenInterest batch writer.
//
// Version:
//   - 2026-08-18: Added.
func (*OpenInterestCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[OpenInterest], error) {
	return newBatchWriter(ctx, destination, openInterestToRow)
}

// Encode encodes OpenInterest records as Apache Parquet.
//
// Version:
//   - 2026-08-16: Added.
func (*OpenInterestCodec) Encode(ctx context.Context, destination io.Writer, records []OpenInterest) error {
	rows := make([]openInterestRow, len(records))
	for i, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[i] = openInterestToRow(record)
	}
	writer := parquetgo.NewGenericWriter[openInterestRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write OpenInterest rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close OpenInterest Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into OpenInterest records.
//
// Version:
//   - 2026-08-16: Added.
func (*OpenInterestCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]OpenInterest, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open OpenInterest Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[openInterestRow](file)
	defer reader.Close()
	rows := make([]openInterestRow, reader.NumRows())
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
			return nil, fmt.Errorf("read OpenInterest rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d OpenInterest rows, want %d", read, len(rows))
	}
	records := make([]OpenInterest, len(rows))
	for i, row := range rows {
		records[i] = openInterestFromRow(row)
	}
	return records, nil
}

func openInterestToRow(record OpenInterest) openInterestRow {
	return openInterestRow{
		EventTimestamp:    record.EventTimestamp.UnixMicro(),
		ReceivedTimestamp: record.ReceivedTimestamp.UnixMicro(),
		Quantity:          record.Quantity,
		NotionalValue:     record.NotionalValue,
	}
}

func openInterestFromRow(row openInterestRow) OpenInterest {
	return OpenInterest{
		EventTimestamp:    timeFromUnixMicro(row.EventTimestamp),
		ReceivedTimestamp: timeFromUnixMicro(row.ReceivedTimestamp),
		Quantity:          row.Quantity,
		NotionalValue:     row.NotionalValue,
	}
}
