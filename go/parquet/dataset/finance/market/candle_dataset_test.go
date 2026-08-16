//
// candle_dataset_test.go
//
package market

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store"
	"github.com/k4k3ru-hub/storage/go/parquet/store/local"
)

func TestCandleDatasetWriteRead(t *testing.T) {
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	parquetClient, err := client.New(client.Params{Store: localStore})
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewCandleDataset(parquetClient, CandleDatasetParams{
		Root:     "market-data/candles",
		FileName: "candles",
	})
	if err != nil {
		t.Fatal(err)
	}
	partition := dataset.Partition{
		"asset_class":     "crypto",
		"venue":           "binance",
		"instrument_type": "spot",
		"symbol":          "BTC-USDT",
		"timeframe":       "1m",
		"date":            "2026-08-14",
	}
	records := []Candle{
		{
			Timestamp: time.Date(2026, 8, 14, 1, 2, 3, 456000000, time.UTC),
			Open:      100.25,
			High:      110.5,
			Low:       95.125,
			Close:     105.75,
			Volume:    1234.5,
		},
		{
			Timestamp: time.Date(2026, 8, 14, 1, 3, 3, 0, time.UTC),
			Open:      105.75,
			High:      112,
			Low:       104,
			Close:     111.5,
			Volume:    321,
		},
	}

	result, err := value.Write(context.Background(), CandleWriteParams{
		Partition: partition,
		Records:   records,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := result.Key, "market-data/candles/asset_class=crypto/venue=binance/instrument_type=spot/symbol=BTC-USDT/timeframe=1m/date=2026-08-14/candles.parquet"; got != want {
		t.Fatalf("write key = %q, want %q", got, want)
	}
	if result.NumRows != int64(len(records)) || result.NumBytes == 0 {
		t.Fatalf("write result = %+v", result)
	}

	read, err := value.Read(context.Background(), CandleReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != len(records) {
		t.Fatalf("read %d records, want %d", len(read.Records), len(records))
	}
	for index := range records {
		if read.Records[index] != records[index] {
			t.Fatalf("record %d = %+v, want %+v", index, read.Records[index], records[index])
		}
	}

	_, err = value.Write(context.Background(), CandleWriteParams{
		Partition: partition,
		Records:   records,
	})
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("second write error = %v, want ErrAlreadyExists", err)
	}
}

func TestCandleDatasetReadsGeneratedParts(t *testing.T) {
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	parquetClient, err := client.New(client.Params{Store: localStore})
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewCandleDataset(parquetClient, CandleDatasetParams{Root: "candles"})
	if err != nil {
		t.Fatal(err)
	}
	partition := dataset.Partition{
		"asset_class":     "crypto",
		"venue":           "binance",
		"instrument_type": "spot",
		"symbol":          "BTC-USDT",
		"timeframe":       "1m",
		"date":            "2026-08-14",
	}
	for index := 0; index < 2; index++ {
		_, err := value.Write(context.Background(), CandleWriteParams{
			Partition: partition,
			Records: []Candle{{
				Timestamp: time.Unix(int64(index), 0).UTC(),
				Open:      float64(index),
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	read, err := value.Read(context.Background(), CandleReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(read.Records), 2; got != want {
		t.Fatalf("read %d records, want %d", got, want)
	}
	if got, want := len(read.Files), 2; got != want {
		t.Fatalf("read %d files, want %d", got, want)
	}
}
