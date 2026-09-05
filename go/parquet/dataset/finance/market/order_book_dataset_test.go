package market

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store/local"
)

func TestOrderBookDatasetWriteRead(t *testing.T) {
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	parquetClient, err := client.New(client.Params{Store: localStore})
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewOrderBookDataset(parquetClient, OrderBookDatasetParams{Root: "market-data/order-books", FileName: "order-book"})
	if err != nil {
		t.Fatal(err)
	}
	partition := dataset.Partition{
		"asset_class": "crypto", "venue": "binance", "instrument_type": "spot", "symbol": "BTC-USDT", "date": "2026-09-05", "hour": "01",
	}
	timestamp := time.Date(2026, 9, 5, 1, 2, 3, 456000000, time.UTC)
	records := []OrderBook{{
		EventTimestamp: timestamp, ReceivedTimestamp: timestamp.Add(time.Millisecond), PublishedTimestamp: timestamp.Add(2 * time.Millisecond),
		VenueSymbol: "BTCUSDT", VenueSequence: "12345", Version: 7, Depth: 2,
		Bids: []OrderBookLevel{{Price: "79594.90", Quantity: "1.25"}, {Price: "79594.89", Quantity: "0.1"}},
		Asks: []OrderBookLevel{{Price: "79595.00", Quantity: "2.5"}, {Price: "79595.01", Quantity: "0.2"}},
	}}
	result, err := value.Write(context.Background(), OrderBookWriteParams{Partition: partition, Records: records})
	if err != nil {
		t.Fatal(err)
	}
	if result.NumRows != 1 || result.NumBytes == 0 {
		t.Fatalf("write result = %+v", result)
	}
	read, err := value.Read(context.Background(), OrderBookReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read.Records, records) {
		t.Fatalf("records = %#v, want %#v", read.Records, records)
	}
}

func TestOrderBookCompactionPolicyDeduplicatesPublishedVersion(t *testing.T) {
	policy := orderBookCompactionPolicy{}
	publishedAt := time.Date(2026, 9, 5, 3, 4, 5, 6000, time.UTC)
	left := OrderBook{VenueSymbol: "BTCUSDT", PublishedTimestamp: publishedAt, Version: 7}
	right := OrderBook{VenueSymbol: "BTCUSDT", PublishedTimestamp: publishedAt, Version: 7}
	leftKey, leftOK := policy.DeduplicationKey(left)
	rightKey, rightOK := policy.DeduplicationKey(right)
	if !leftOK || !rightOK || leftKey != rightKey {
		t.Fatalf("deduplication keys = (%q, %t), (%q, %t)", leftKey, leftOK, rightKey, rightOK)
	}
	restarted := OrderBook{VenueSymbol: "BTCUSDT", PublishedTimestamp: publishedAt.Add(time.Second), Version: 7}
	restartedKey, restartedOK := policy.DeduplicationKey(restarted)
	if !restartedOK || restartedKey == leftKey {
		t.Fatalf("restarted deduplication key = (%q, %t), want key distinct from %q", restartedKey, restartedOK, leftKey)
	}
}
