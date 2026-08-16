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
		rows[i] = openInterestRow{record.EventTimestamp.UnixMicro(), record.ReceivedTimestamp.UnixMicro(), record.Quantity, record.NotionalValue}
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
		records[i] = OpenInterest{timeFromUnixMicro(row.EventTimestamp), timeFromUnixMicro(row.ReceivedTimestamp), row.Quantity, row.NotionalValue}
	}
	return records, nil
}
