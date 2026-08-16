// bbo_dataset_test.go
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

func TestBBODatasetWriteRead(t *testing.T) {
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	parquetClient, err := client.New(client.Params{Store: localStore})
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewBBODataset(parquetClient, BBODatasetParams{
		Root:     "market-data/bbo",
		FileName: "bbo",
	})
	if err != nil {
		t.Fatal(err)
	}
	partition := dataset.Partition{
		"asset_class":     "crypto",
		"venue":           "hyperliquid",
		"instrument_type": "perpetual",
		"symbol":          "BTC-USDC",
		"date":            "2026-08-15",
		"hour":            "13",
	}
	records := []BBO{
		{
			EventTimestamp:    time.Date(2026, 8, 15, 13, 2, 3, 456000000, time.UTC),
			ReceivedTimestamp: time.Date(2026, 8, 15, 13, 2, 3, 457000000, time.UTC),
			BidPrice:          118000.25,
			BidQuantity:       1.25,
			AskPrice:          118001.5,
			AskQuantity:       0.45,
		},
		{
			EventTimestamp:    time.Date(2026, 8, 15, 13, 2, 4, 0, time.UTC),
			ReceivedTimestamp: time.Date(2026, 8, 15, 13, 2, 4, 1000000, time.UTC),
			BidPrice:          118000.5,
			BidQuantity:       2.1,
			AskPrice:          118001.5,
			AskQuantity:       0.3,
		},
	}

	result, err := value.Write(context.Background(), BBOWriteParams{
		Partition: partition,
		Records:   records,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := result.Key, "market-data/bbo/asset_class=crypto/venue=hyperliquid/instrument_type=perpetual/symbol=BTC-USDC/date=2026-08-15/hour=13/bbo.parquet"; got != want {
		t.Fatalf("write key = %q, want %q", got, want)
	}
	if result.NumRows != int64(len(records)) || result.NumBytes == 0 {
		t.Fatalf("write result = %+v", result)
	}

	read, err := value.Read(context.Background(), BBOReadParams{Partition: partition})
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

	_, err = value.Write(context.Background(), BBOWriteParams{
		Partition: partition,
		Records:   records,
	})
	if !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("second write error = %v, want ErrAlreadyExists", err)
	}
}

func TestBBODatasetReadsGeneratedParts(t *testing.T) {
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	parquetClient, err := client.New(client.Params{Store: localStore})
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewBBODataset(parquetClient, BBODatasetParams{Root: "bbo"})
	if err != nil {
		t.Fatal(err)
	}
	partition := dataset.Partition{
		"asset_class":     "crypto",
		"venue":           "binance",
		"instrument_type": "spot",
		"symbol":          "BTC-USDT",
		"date":            "2026-08-15",
		"hour":            "13",
	}
	for index := 0; index < 2; index++ {
		timestamp := time.Unix(int64(index), 0).UTC()
		_, err := value.Write(context.Background(), BBOWriteParams{
			Partition: partition,
			Records: []BBO{
				{
					EventTimestamp:    timestamp,
					ReceivedTimestamp: timestamp.Add(time.Millisecond),
					BidPrice:          100 + float64(index),
					AskPrice:          101 + float64(index),
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	read, err := value.Read(context.Background(), BBOReadParams{Partition: partition})
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
