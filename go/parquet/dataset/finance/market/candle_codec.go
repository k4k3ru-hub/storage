package market

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	parquetgo "github.com/parquet-go/parquet-go"
)

type CandleCodec struct{ maxRows int64 }

type candleRow struct {
	Timestamp         int64    `parquet:"timestamp,timestamp(microsecond)"`
	Open              float64  `parquet:"open"`
	High              float64  `parquet:"high"`
	Low               float64  `parquet:"low"`
	Close             float64  `parquet:"close"`
	Volume            float64  `parquet:"volume"`
	QuoteVolume       *float64 `parquet:"quote_volume,optional"`
	BuyBaseVolume     *float64 `parquet:"buy_base_volume,optional"`
	SellBaseVolume    *float64 `parquet:"sell_base_volume,optional"`
	UnknownBaseVolume *float64 `parquet:"unknown_base_volume,optional"`
	TradeCount        *uint64  `parquet:"trade_count,optional"`
	Quality           *uint64  `parquet:"quality,optional"`
	VenueSymbol       string   `parquet:"venue_symbol,optional"`
	Chain             string   `parquet:"chain,optional"`
	Network           string   `parquet:"network,optional"`
	PoolID            string   `parquet:"pool_id,optional"`
	RunID             string   `parquet:"run_id,optional"`
	Version           uint64   `parquet:"version,optional"`
}

// NewCandleCodec creates the backward-compatible OHLCV and execution aggregate codec.
//
// Version:
//   - 2026-09-23: Add optional execution totals, source identity and quality metadata.
func NewCandleCodec() *CandleCodec { return &CandleCodec{} }

// NewBatchReader opens a bounded Candle reader and normalizes additive legacy schemas.
//
// Version:
//   - 2026-09-23: Preserve legacy OHLCV compatibility and enforce optional row limits.
func (c *CandleCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[Candle], error) {
	reader, err := newBatchReader(ctx, source, size, candleFromRow)
	if err != nil {
		return nil, fmt.Errorf("failed to open candle reader: %w", err)
	}
	if c.maxRows > 0 && reader.FileInfo().NumRows > c.maxRows {
		return nil, errors.Join(fmt.Errorf("failed to open candle reader: rows=too_long max_length=%d", c.maxRows), reader.Close())
	}
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to inspect candle schema: %w", err), reader.Close())
	}
	if compatibleCandleSchema(file.Schema()) {
		r := reader.(*batchReader[Candle, candleRow])
		r.info.SchemaFingerprint = fingerprint(parquetgo.SchemaOf(new(candleRow)).String())
		zstd := true
		for _, group := range file.Metadata().RowGroups {
			for _, column := range group.Columns {
				if column.MetaData.Codec.String() != "ZSTD" {
					zstd = false
				}
			}
		}
		if zstd {
			r.info.CompressionFingerprint = fingerprint("candle:zstd")
		}
	}
	return reader, nil
}

// NewBatchWriter creates a streaming Candle writer.
//
// Version:
//   - 2026-09-23: Encode optional execution aggregate columns.
func (*CandleCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[Candle], error) {
	return newBatchWriter(ctx, destination, candleToRow)
}

// Encode writes Candle records, enforcing the configured per-file limit.
//
// Version:
//   - 2026-09-23: Add execution columns and preserve writer errors.
func (c *CandleCodec) Encode(ctx context.Context, destination io.Writer, records []Candle) error {
	if c.maxRows > 0 && int64(len(records)) > c.maxRows {
		return fmt.Errorf("failed to encode candles: rows=too_long max_length=%d", c.maxRows)
	}
	writer, err := c.NewBatchWriter(ctx, destination)
	if err != nil {
		return fmt.Errorf("failed to encode candles: %w", err)
	}
	if err := writer.Write(ctx, records); err != nil {
		return fmt.Errorf("failed to encode candles: %w", errors.Join(err, writer.Close()))
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to encode candles: %w", err)
	}
	return nil
}

// Decode reads Candle records, rejecting oversized files before allocating their rows.
//
// Version:
//   - 2026-09-23: Read additive legacy schemas and enforce optional row limits.
func (c *CandleCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) (records []Candle, err error) {
	reader, err := c.NewBatchReader(ctx, source, size)
	if err != nil {
		return nil, fmt.Errorf("failed to decode candles: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close candle reader: %w", closeErr))
		}
	}()
	count := reader.FileInfo().NumRows
	if count < 0 {
		return nil, fmt.Errorf("failed to decode candles: rows=out_of_range")
	}
	records = make([]Candle, int(count))
	read := 0
	for read < len(records) {
		n, readErr := reader.Read(ctx, records[read:])
		read += n
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, fmt.Errorf("failed to decode candles: %w", readErr)
		}
		if errors.Is(readErr, io.EOF) || n == 0 {
			break
		}
	}
	if read != len(records) {
		return nil, fmt.Errorf("failed to decode candles: rows=too_short")
	}
	return records, nil
}

func candleToRow(v Candle) candleRow {
	return candleRow{
		Timestamp: v.Timestamp.UnixMicro(), Open: v.Open, High: v.High, Low: v.Low, Close: v.Close, Volume: v.Volume,
		QuoteVolume: v.QuoteVolume, BuyBaseVolume: v.BuyBaseVolume, SellBaseVolume: v.SellBaseVolume, UnknownBaseVolume: v.UnknownBaseVolume,
		TradeCount: v.TradeCount, Quality: v.Quality, VenueSymbol: v.VenueSymbol, Chain: v.Chain, Network: v.Network, PoolID: v.PoolID, RunID: v.RunID, Version: v.Version,
	}
}

func candleFromRow(v candleRow) Candle {
	return Candle{
		Timestamp: timeFromUnixMicro(v.Timestamp), Open: v.Open, High: v.High, Low: v.Low, Close: v.Close, Volume: v.Volume,
		QuoteVolume: v.QuoteVolume, BuyBaseVolume: v.BuyBaseVolume, SellBaseVolume: v.SellBaseVolume, UnknownBaseVolume: v.UnknownBaseVolume,
		TradeCount: v.TradeCount, Quality: v.Quality, VenueSymbol: v.VenueSymbol, Chain: v.Chain, Network: v.Network, PoolID: v.PoolID, RunID: v.RunID, Version: v.Version,
	}
}

func compatibleCandleSchema(actual *parquetgo.Schema) bool {
	expected := parquetgo.SchemaOf(new(candleRow))
	fields := make(map[string]parquetgo.Node)
	for _, field := range expected.Fields() {
		fields[field.Name()] = field
	}
	seen := make(map[string]bool)
	for _, field := range actual.Fields() {
		want, ok := fields[field.Name()]
		if !ok || !parquetgo.EqualNodes(field, want) {
			return false
		}
		seen[field.Name()] = true
	}
	for name, node := range fields {
		if !seen[name] && !node.Optional() {
			return false
		}
	}
	return true
}
