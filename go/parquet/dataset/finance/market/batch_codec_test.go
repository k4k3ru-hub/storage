// batch_codec_test.go
package market

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	parquetgo "github.com/parquet-go/parquet-go"
	parquetformat "github.com/parquet-go/parquet-go/format"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

var (
	_ dataset.CompactionCodec[BBO]          = (*BBOCodec)(nil)
	_ dataset.CompactionCodec[Candle]       = (*CandleCodec)(nil)
	_ dataset.CompactionCodec[FundingRate]  = (*FundingRateCodec)(nil)
	_ dataset.CompactionCodec[Liquidation]  = (*LiquidationCodec)(nil)
	_ dataset.CompactionCodec[OpenInterest] = (*OpenInterestCodec)(nil)
	_ dataset.CompactionCodec[Trade]        = (*TradeCodec)(nil)
)

func TestCandleCodecBatchRoundTrip(t *testing.T) {
	records := []Candle{
		{
			Timestamp: time.Date(2026, 8, 18, 1, 2, 3, 456789000, time.UTC),
			Open:      100.25,
			High:      110.5,
			Low:       95.125,
			Close:     105.75,
			Volume:    1234.5,
		},
		{
			Timestamp: time.Date(2026, 8, 18, 1, 3, 3, 123000000, time.UTC),
			Open:      105.75,
			High:      112,
			Low:       104,
			Close:     111.5,
			Volume:    321,
		},
	}

	assertBatchRoundTrip(t, NewCandleCodec(), records)
}

func TestBBOCodecBatchRoundTrip(t *testing.T) {
	records := []BBO{
		{
			EventTimestamp:    time.Date(2026, 8, 18, 13, 2, 3, 456789000, time.UTC),
			ReceivedTimestamp: time.Date(2026, 8, 18, 13, 2, 3, 457789000, time.UTC),
			BidPrice:          118000.25,
			BidQuantity:       1.25,
			AskPrice:          118001.5,
			AskQuantity:       0.45,
		},
		{
			EventTimestamp:    time.Date(2026, 8, 18, 13, 2, 4, 123000000, time.UTC),
			ReceivedTimestamp: time.Date(2026, 8, 18, 13, 2, 4, 124000000, time.UTC),
			BidPrice:          118000.5,
			BidQuantity:       2.1,
			AskPrice:          118001.5,
			AskQuantity:       0.3,
		},
	}

	assertBatchRoundTrip(t, NewBBOCodec(), records)
}

func TestAdditionalCodecsBatchRoundTrip(t *testing.T) {
	event := time.Date(2026, 8, 18, 13, 2, 3, 456789000, time.UTC)
	received := event.Add(time.Millisecond)

	t.Run("trade", func(t *testing.T) {
		assertBatchRoundTrip(t, NewTradeCodec(), []Trade{
			{event, received, "trade-1", "buy", 118000.25, 1.5},
			{event.Add(time.Second), received.Add(time.Second), "trade-2", "sell", 118001.5, 0.25},
		})
	})
	t.Run("open_interest", func(t *testing.T) {
		assertBatchRoundTrip(t, NewOpenInterestCodec(), []OpenInterest{
			{event, received, 23456.75, 2760000000},
			{event.Add(time.Second), received.Add(time.Second), 23457.25, 2761000000},
		})
	})
	t.Run("funding_rate", func(t *testing.T) {
		assertBatchRoundTrip(t, NewFundingRateCodec(), []FundingRate{
			{event, received, event.Add(time.Hour), 0.0001, 0.00012, 118001, 117995},
			{event.Add(time.Second), received.Add(time.Second), event.Add(2 * time.Hour), 0.00011, 0.00013, 118002, 117996},
		})
	})
	t.Run("liquidation", func(t *testing.T) {
		assertBatchRoundTrip(t, NewLiquidationCodec(), []Liquidation{
			{event, received, "liq-1", "sell", 117500, 3.25},
			{event.Add(time.Second), received.Add(time.Second), "liq-2", "buy", 117600, 1.75},
		})
	})
}

func TestBatchCodecHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := NewCandleCodec().NewBatchWriter(ctx, &bytes.Buffer{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("create batch writer error = %v, want context.Canceled", err)
	}

	var encoded bytes.Buffer
	writer, err := NewCandleCodec().NewBatchWriter(context.Background(), &encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(ctx, []Candle{{Timestamp: time.Unix(0, 0).UTC()}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("write batch error = %v, want context.Canceled", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewCandleCodec().NewBatchReader(
		context.Background(),
		bytes.NewReader(encoded.Bytes()),
		int64(encoded.Len()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Read(ctx, make([]Candle, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("read batch error = %v, want context.Canceled", err)
	}
}

func assertBatchRoundTrip[T comparable](t *testing.T, codec dataset.CompactionCodec[T], records []T) {
	t.Helper()
	ctx := context.Background()
	var encoded bytes.Buffer
	writer, err := codec.NewBatchWriter(ctx, &encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := writer.Write(ctx, []T{record}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := parquetgo.OpenFile(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, rowGroup := range file.Metadata().RowGroups {
		for _, column := range rowGroup.Columns {
			if got, want := column.MetaData.Codec, parquetformat.Zstd; got != want {
				t.Fatalf("compression codec = %v, want %v", got, want)
			}
		}
	}

	reader, err := codec.NewBatchReader(ctx, bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	info := reader.FileInfo()
	if got, want := info.NumRows, int64(len(records)); got != want {
		t.Fatalf("file rows = %d, want %d", got, want)
	}
	if info.SchemaFingerprint == "" {
		t.Fatal("schema fingerprint is empty")
	}
	if info.CompressionFingerprint == "" {
		t.Fatal("compression fingerprint is empty")
	}

	decoded := make([]T, 0, len(records))
	buffer := make([]T, 1)
	for {
		n, readErr := reader.Read(ctx, buffer)
		decoded = append(decoded, buffer[:n]...)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
	}
	if len(decoded) != len(records) {
		t.Fatalf("decoded %d records, want %d", len(decoded), len(records))
	}
	for index := range records {
		if decoded[index] != records[index] {
			t.Fatalf("record %d = %+v, want %+v", index, decoded[index], records[index])
		}
	}
}
