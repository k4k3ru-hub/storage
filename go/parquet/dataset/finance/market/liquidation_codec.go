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
		rows[i] = liquidationRow{r.EventTimestamp.UnixMicro(), r.ReceivedTimestamp.UnixMicro(), r.LiquidationID, r.Side, r.Price, r.Quantity}
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
		records[i] = Liquidation{timeFromUnixMicro(r.EventTimestamp), timeFromUnixMicro(r.ReceivedTimestamp), r.LiquidationID, r.Side, r.Price, r.Quantity}
	}
	return records, nil
}
