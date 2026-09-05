package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type OrderBookCodec struct{}

type orderBookLevelRow struct {
	Price    string `parquet:"price"`
	Quantity string `parquet:"quantity"`
}

type orderBookRow struct {
	EventTimestamp     int64               `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp  int64               `parquet:"received_timestamp,timestamp(microsecond)"`
	PublishedTimestamp int64               `parquet:"published_timestamp,timestamp(microsecond)"`
	Chain              string              `parquet:"chain"`
	VenueSymbol        string              `parquet:"venue_symbol"`
	VenueSequence      string              `parquet:"venue_sequence"`
	Version            uint64              `parquet:"version"`
	Depth              uint32              `parquet:"depth"`
	Bids               []orderBookLevelRow `parquet:"bids"`
	Asks               []orderBookLevelRow `parquet:"asks"`
}

// NewOrderBookCodec creates an OrderBook Parquet codec.
//
// Version:
//   - 2026-09-05: Added.
func NewOrderBookCodec() *OrderBookCodec {
	return &OrderBookCodec{}
}

// NewBatchReader opens a bounded OrderBook Parquet reader.
//
// Version:
//   - 2026-09-05: Added.
func (*OrderBookCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[OrderBook], error) {
	return newBatchReader(ctx, source, size, orderBookFromRow)
}

// NewBatchWriter creates a bounded OrderBook Parquet writer.
//
// Version:
//   - 2026-09-05: Added.
func (*OrderBookCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[OrderBook], error) {
	return newBatchWriter(ctx, destination, orderBookToRow)
}

// Encode encodes OrderBook records as Apache Parquet.
//
// Version:
//   - 2026-09-05: Added.
func (*OrderBookCodec) Encode(ctx context.Context, destination io.Writer, records []OrderBook) error {
	rows := make([]orderBookRow, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[index] = orderBookToRow(record)
	}
	writer := parquetgo.NewGenericWriter[orderBookRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("failed to write order book rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close order book parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into OrderBook records.
//
// Version:
//   - 2026-09-05: Added.
func (*OrderBookCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]OrderBook, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("failed to open order book parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[orderBookRow](file)
	defer reader.Close()
	rows := make([]orderBookRow, reader.NumRows())
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
			return nil, fmt.Errorf("failed to read order book rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("failed to read order book rows: row count mismatch: actual_rows=%d expected_rows=%d", read, len(rows))
	}
	records := make([]OrderBook, len(rows))
	for index, row := range rows {
		records[index] = orderBookFromRow(row)
	}
	return records, nil
}

func orderBookToRow(record OrderBook) orderBookRow {
	return orderBookRow{
		EventTimestamp: record.EventTimestamp.UnixMicro(), ReceivedTimestamp: record.ReceivedTimestamp.UnixMicro(),
		PublishedTimestamp: record.PublishedTimestamp.UnixMicro(), Chain: record.Chain, VenueSymbol: record.VenueSymbol,
		VenueSequence: record.VenueSequence, Version: record.Version, Depth: record.Depth,
		Bids: orderBookLevelsToRows(record.Bids), Asks: orderBookLevelsToRows(record.Asks),
	}
}

func orderBookFromRow(row orderBookRow) OrderBook {
	return OrderBook{
		EventTimestamp: timeFromUnixMicro(row.EventTimestamp), ReceivedTimestamp: timeFromUnixMicro(row.ReceivedTimestamp),
		PublishedTimestamp: timeFromUnixMicro(row.PublishedTimestamp), Chain: row.Chain, VenueSymbol: row.VenueSymbol,
		VenueSequence: row.VenueSequence, Version: row.Version, Depth: row.Depth,
		Bids: orderBookLevelsFromRows(row.Bids), Asks: orderBookLevelsFromRows(row.Asks),
	}
}

func orderBookLevelsToRows(levels []OrderBookLevel) []orderBookLevelRow {
	rows := make([]orderBookLevelRow, len(levels))
	for index, level := range levels {
		rows[index] = orderBookLevelRow{Price: level.Price, Quantity: level.Quantity}
	}
	return rows
}

func orderBookLevelsFromRows(rows []orderBookLevelRow) []OrderBookLevel {
	levels := make([]OrderBookLevel, len(rows))
	for index, row := range rows {
		levels[index] = OrderBookLevel{Price: row.Price, Quantity: row.Quantity}
	}
	return levels
}
