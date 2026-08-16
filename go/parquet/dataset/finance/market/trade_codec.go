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
		rows[index] = tradeRow{record.EventTimestamp.UnixMicro(), record.ReceivedTimestamp.UnixMicro(), record.TradeID, record.Side, record.Price, record.Quantity}
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
		records[index] = Trade{timeFromUnixMicro(row.EventTimestamp), timeFromUnixMicro(row.ReceivedTimestamp), row.TradeID, row.Side, row.Price, row.Quantity}
	}
	return records, nil
}
