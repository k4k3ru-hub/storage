package market

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type AMMPoolMetadataCodec struct{}

type ammPoolMetadataRow struct {
	MetadataID         string   `parquet:"metadata_id"`
	SupersedesID       *string  `parquet:"supersedes_id,optional"`
	ObservedTimestamp  int64    `parquet:"observed_timestamp,timestamp(microsecond)"`
	EffectiveTimestamp *int64   `parquet:"effective_timestamp,timestamp(microsecond),optional"`
	Chain              string   `parquet:"chain"`
	PoolID             string   `parquet:"pool_id"`
	BaseAssetID        string   `parquet:"base_asset_id"`
	QuoteAssetID       string   `parquet:"quote_asset_id"`
	BaseDecimals       uint8    `parquet:"base_decimals"`
	QuoteDecimals      uint8    `parquet:"quote_decimals"`
	FeeModel           string   `parquet:"fee_model"`
	FeeRate            *float64 `parquet:"fee_rate,optional"`
	PriceGridType      string   `parquet:"price_grid_type"`
	TickSpacing        *uint32  `parquet:"tick_spacing,optional"`
	BinStep            *uint32  `parquet:"bin_step,optional"`
	Hooks              *string  `parquet:"hooks,optional"`
	ConfigurationID    *string  `parquet:"configuration_id,optional"`
	Fingerprint        string   `parquet:"fingerprint"`
}

// NewAMMPoolMetadataCodec creates an AMM pool metadata Parquet codec.
//
// Version:
//   - 2026-08-22: Added.
func NewAMMPoolMetadataCodec() *AMMPoolMetadataCodec {
	return &AMMPoolMetadataCodec{}
}

// NewBatchReader opens a bounded AMM pool metadata Parquet reader.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMPoolMetadataCodec) NewBatchReader(ctx context.Context, source dataset.ReadSource, size int64) (dataset.BatchReader[AMMPoolMetadata], error) {
	return newBatchReader(ctx, source, size, ammPoolMetadataFromRow)
}

// NewBatchWriter creates a bounded AMM pool metadata Parquet writer.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMPoolMetadataCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[AMMPoolMetadata], error) {
	return newBatchWriter(ctx, destination, ammPoolMetadataToRow)
}

// Encode encodes AMM pool metadata records as Apache Parquet.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMPoolMetadataCodec) Encode(ctx context.Context, destination io.Writer, records []AMMPoolMetadata) error {
	rows := make([]ammPoolMetadataRow, len(records))
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows[index] = ammPoolMetadataToRow(record)
	}
	writer := parquetgo.NewGenericWriter[ammPoolMetadataRow](destination, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(rows); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write AMM pool metadata rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close AMM pool metadata Parquet writer: %w", err)
	}
	return nil
}

// Decode decodes Apache Parquet into AMM pool metadata records.
//
// Version:
//   - 2026-08-22: Added.
func (*AMMPoolMetadataCodec) Decode(ctx context.Context, source dataset.ReadSource, size int64) ([]AMMPoolMetadata, error) {
	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("open AMM pool metadata Parquet file: %w", err)
	}
	reader := parquetgo.NewGenericReader[ammPoolMetadataRow](file)
	defer reader.Close()
	rows := make([]ammPoolMetadataRow, reader.NumRows())
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
			return nil, fmt.Errorf("read AMM pool metadata rows: %w", err)
		}
	}
	if read != len(rows) {
		return nil, fmt.Errorf("read %d AMM pool metadata rows, want %d", read, len(rows))
	}
	records := make([]AMMPoolMetadata, len(rows))
	for index, row := range rows {
		records[index] = ammPoolMetadataFromRow(row)
	}
	return records, nil
}

func ammPoolMetadataToRow(record AMMPoolMetadata) ammPoolMetadataRow {
	var effectiveTimestamp *int64
	if record.EffectiveTimestamp != nil {
		value := record.EffectiveTimestamp.UnixMicro()
		effectiveTimestamp = &value
	}
	return ammPoolMetadataRow{
		MetadataID:         record.MetadataID,
		SupersedesID:       record.SupersedesID,
		ObservedTimestamp:  record.ObservedTimestamp.UnixMicro(),
		EffectiveTimestamp: effectiveTimestamp,
		Chain:              record.Chain,
		PoolID:             record.PoolID,
		BaseAssetID:        record.BaseAssetID,
		QuoteAssetID:       record.QuoteAssetID,
		BaseDecimals:       record.BaseDecimals,
		QuoteDecimals:      record.QuoteDecimals,
		FeeModel:           record.FeeModel,
		FeeRate:            record.FeeRate,
		PriceGridType:      record.PriceGridType,
		TickSpacing:        record.TickSpacing,
		BinStep:            record.BinStep,
		Hooks:              record.Hooks,
		ConfigurationID:    record.ConfigurationID,
		Fingerprint:        record.Fingerprint,
	}
}

func ammPoolMetadataFromRow(row ammPoolMetadataRow) AMMPoolMetadata {
	var effectiveTimestamp *time.Time
	if row.EffectiveTimestamp != nil {
		value := timeFromUnixMicro(*row.EffectiveTimestamp)
		effectiveTimestamp = &value
	}
	return AMMPoolMetadata{
		MetadataID:         row.MetadataID,
		SupersedesID:       row.SupersedesID,
		ObservedTimestamp:  timeFromUnixMicro(row.ObservedTimestamp),
		EffectiveTimestamp: effectiveTimestamp,
		Chain:              row.Chain,
		PoolID:             row.PoolID,
		BaseAssetID:        row.BaseAssetID,
		QuoteAssetID:       row.QuoteAssetID,
		BaseDecimals:       row.BaseDecimals,
		QuoteDecimals:      row.QuoteDecimals,
		FeeModel:           row.FeeModel,
		FeeRate:            row.FeeRate,
		PriceGridType:      row.PriceGridType,
		TickSpacing:        row.TickSpacing,
		BinStep:            row.BinStep,
		Hooks:              row.Hooks,
		ConfigurationID:    row.ConfigurationID,
		Fingerprint:        row.Fingerprint,
	}
}
