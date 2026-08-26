// open_interest_codec.go
package market

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	parquetgo "github.com/parquet-go/parquet-go"
)

type OpenInterestCodec struct{}
type openInterestRow struct {
	EventTimestamp           int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp        int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	RawQuantity              float64  `parquet:"raw_quantity"`
	RawUnit                  string   `parquet:"raw_unit"`
	Quantity                 float64  `parquet:"quantity"`
	NotionalValue            float64  `parquet:"notional_value"`
	NotionalCurrency         string   `parquet:"notional_currency"`
	ConversionPrice          *float64 `parquet:"conversion_price,optional"`
	ConversionPriceType      string   `parquet:"conversion_price_type"`
	ConversionPriceTimestamp *int64   `parquet:"conversion_price_timestamp,timestamp(microsecond),optional"`
	ContractSize             *float64 `parquet:"contract_size,optional"`
	ContractSizeUnit         *string  `parquet:"contract_size_unit,optional"`
	ContractSizeCurrency     *string  `parquet:"contract_size_currency,optional"`
}

// NewOpenInterestCodec creates an OpenInterest Parquet codec.
//
// Version:
//   - 2026-08-16: Added.
//   - 2026-08-19: Preserved raw units and normalization inputs.
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
//   - 2026-08-19: Added normalized OpenInterest fields.
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
//   - 2026-08-19: Added normalized OpenInterest fields.
//   - 2026-08-18: Added.
func (*OpenInterestCodec) NewBatchWriter(ctx context.Context, destination io.Writer) (dataset.BatchWriter[OpenInterest], error) {
	return newBatchWriter(ctx, destination, openInterestToRow)
}

// Encode encodes OpenInterest records as Apache Parquet.
//
// Version:
//   - 2026-08-19: Added normalized OpenInterest fields.
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
//   - 2026-08-19: Added normalized OpenInterest fields.
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
	var conversionPriceTimestamp *int64
	if record.ConversionPriceTimestamp != nil {
		value := record.ConversionPriceTimestamp.UnixMicro()
		conversionPriceTimestamp = &value
	}
	var contractSizeUnit *string
	if record.ContractSizeUnit != nil {
		value := string(*record.ContractSizeUnit)
		contractSizeUnit = &value
	}
	return openInterestRow{
		EventTimestamp:           record.EventTimestamp.UnixMicro(),
		ReceivedTimestamp:        record.ReceivedTimestamp.UnixMicro(),
		RawQuantity:              record.RawQuantity,
		RawUnit:                  string(record.RawUnit),
		Quantity:                 record.Quantity,
		NotionalValue:            record.NotionalValue,
		NotionalCurrency:         record.NotionalCurrency,
		ConversionPrice:          record.ConversionPrice,
		ConversionPriceType:      string(record.ConversionPriceType),
		ConversionPriceTimestamp: conversionPriceTimestamp,
		ContractSize:             record.ContractSize,
		ContractSizeUnit:         contractSizeUnit,
		ContractSizeCurrency:     record.ContractSizeCurrency,
	}
}

func openInterestFromRow(row openInterestRow) OpenInterest {
	var conversionPriceTimestamp *time.Time
	if row.ConversionPriceTimestamp != nil {
		value := timeFromUnixMicro(*row.ConversionPriceTimestamp)
		conversionPriceTimestamp = &value
	}
	var contractSizeUnit *ContractSizeUnit
	if row.ContractSizeUnit != nil {
		value := ContractSizeUnit(*row.ContractSizeUnit)
		contractSizeUnit = &value
	}
	return OpenInterest{
		EventTimestamp:           timeFromUnixMicro(row.EventTimestamp),
		ReceivedTimestamp:        timeFromUnixMicro(row.ReceivedTimestamp),
		RawQuantity:              row.RawQuantity,
		RawUnit:                  OpenInterestUnit(row.RawUnit),
		Quantity:                 row.Quantity,
		NotionalValue:            row.NotionalValue,
		NotionalCurrency:         row.NotionalCurrency,
		ConversionPrice:          row.ConversionPrice,
		ConversionPriceType:      OpenInterestPriceType(row.ConversionPriceType),
		ConversionPriceTimestamp: conversionPriceTimestamp,
		ContractSize:             row.ContractSize,
		ContractSizeUnit:         contractSizeUnit,
		ContractSizeCurrency:     row.ContractSizeCurrency,
	}
}
