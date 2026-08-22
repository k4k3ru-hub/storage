package market

import (
	"context"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store/local"
)

func TestBBOCompactionSortsAndDeduplicatesExactRecords(t *testing.T) {
	parquetClient := newNormalizationTestClient(t)
	value, err := NewBBODataset(parquetClient, BBODatasetParams{Root: "bbo"})
	if err != nil {
		t.Fatal(err)
	}
	partition := normalizationIntradayPartition()
	earlier := BBO{EventTimestamp: normalizationTime(1), ReceivedTimestamp: normalizationTime(1).Add(time.Millisecond), BidPrice: 100, BidQuantity: 1, AskPrice: 101, AskQuantity: 2}
	later := BBO{EventTimestamp: normalizationTime(2), ReceivedTimestamp: normalizationTime(2).Add(time.Millisecond), BidPrice: 102, BidQuantity: 1, AskPrice: 103, AskQuantity: 2}
	writeNormalizationParts(t, value.Write, partition, []BBO{later, earlier}, []BBO{earlier})

	result, err := value.Compact(context.Background(), BBOCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	assertNormalizationCounts(t, result, 3, 2, 1)
	read, err := value.Read(context.Background(), BBOReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 2 || read.Records[0] != earlier || read.Records[1] != later {
		t.Fatalf("BBO records = %+v, want chronological unique records", read.Records)
	}
}

func TestAMMExecutableQuoteCompactionSortsAndDeduplicatesExactRecords(t *testing.T) {
	parquetClient := newNormalizationTestClient(t)
	value, err := NewAMMExecutableQuoteDataset(parquetClient, AMMExecutableQuoteDatasetParams{Root: "amm-executable-quotes"})
	if err != nil {
		t.Fatal(err)
	}
	partition := normalizationIntradayPartition()
	feeRate := 0.003
	earlier := AMMExecutableQuote{
		EventTimestamp: normalizationTime(1), ReceivedTimestamp: normalizationTime(1).Add(time.Millisecond),
		Chain: "ethereum", PoolID: "pool-1", BidPrice: 100, BidQuantity: 1, AskPrice: 101, AskQuantity: 2,
		EffectiveFeeRate: &feeRate, FeeIncluded: true,
	}
	later := earlier
	later.EventTimestamp = normalizationTime(2)
	later.ReceivedTimestamp = normalizationTime(2).Add(time.Millisecond)
	later.BidPrice = 102
	later.AskPrice = 103
	writeNormalizationParts(t, value.Write, partition, []AMMExecutableQuote{later, earlier}, []AMMExecutableQuote{earlier})

	result, err := value.Compact(context.Background(), AMMExecutableQuoteCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	assertNormalizationCounts(t, result, 3, 2, 1)
	read, err := value.Read(context.Background(), AMMExecutableQuoteReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 2 || !read.Records[0].EventTimestamp.Equal(earlier.EventTimestamp) || !read.Records[1].EventTimestamp.Equal(later.EventTimestamp) {
		t.Fatalf("AMM executable quote records = %+v, want chronological unique records", read.Records)
	}
}

func TestTradeCompactionSortsAndDeduplicatesByTradeID(t *testing.T) {
	parquetClient := newNormalizationTestClient(t)
	value, err := NewTradeDataset(parquetClient, TradeDatasetParams{Root: "trades"})
	if err != nil {
		t.Fatal(err)
	}
	partition := normalizationIntradayPartition()
	earlier := Trade{EventTimestamp: normalizationTime(1), ReceivedTimestamp: normalizationTime(1).Add(time.Millisecond), TradeID: "trade-1", Side: "buy", Price: 100, Quantity: 1}
	later := Trade{EventTimestamp: normalizationTime(2), ReceivedTimestamp: normalizationTime(2).Add(time.Millisecond), TradeID: "trade-2", Side: "sell", Price: 101, Quantity: 2}
	duplicate := earlier
	duplicate.ReceivedTimestamp = duplicate.ReceivedTimestamp.Add(time.Second)
	writeNormalizationParts(t, value.Write, partition, []Trade{later, earlier}, []Trade{duplicate})

	result, err := value.Compact(context.Background(), TradeCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	assertNormalizationCounts(t, result, 3, 2, 1)
	read, err := value.Read(context.Background(), TradeReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 2 || read.Records[0].TradeID != "trade-1" || read.Records[1].TradeID != "trade-2" {
		t.Fatalf("Trade records = %+v, want chronological unique trade IDs", read.Records)
	}
}

func TestCandleCompactionSortsAndDeduplicatesByTimestamp(t *testing.T) {
	parquetClient := newNormalizationTestClient(t)
	value, err := NewCandleDataset(parquetClient, CandleDatasetParams{Root: "candles"})
	if err != nil {
		t.Fatal(err)
	}
	partition := candleCompactionPartition()
	earlier := Candle{Timestamp: normalizationTime(1), Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 10}
	later := Candle{Timestamp: normalizationTime(2), Open: 101, High: 102, Low: 100, Close: 101.5, Volume: 11}
	duplicate := earlier
	duplicate.Close = 100.75
	writeNormalizationParts(t, value.Write, partition, []Candle{later, earlier}, []Candle{duplicate})

	result, err := value.Compact(context.Background(), CandleCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	assertNormalizationCounts(t, result, 3, 2, 1)
	read, err := value.Read(context.Background(), CandleReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 2 || !read.Records[0].Timestamp.Equal(earlier.Timestamp) || !read.Records[1].Timestamp.Equal(later.Timestamp) {
		t.Fatalf("Candle records = %+v, want chronological unique timestamps", read.Records)
	}
}

type normalizationWriter[T any] func(context.Context, dataset.WriteParams[T]) (dataset.WriteResult, error)

func writeNormalizationParts[T any](t *testing.T, write normalizationWriter[T], partition dataset.Partition, parts ...[]T) {
	t.Helper()
	for _, records := range parts {
		if _, err := write(context.Background(), dataset.WriteParams[T]{Partition: partition, Records: records}); err != nil {
			t.Fatal(err)
		}
	}
}

func newNormalizationTestClient(t *testing.T) *client.Client {
	t.Helper()
	objectStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	value, err := client.New(client.Params{Store: objectStore})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func normalizationIntradayPartition() dataset.Partition {
	return dataset.Partition{
		"asset_class": "crypto", "venue": "uniswap_v4", "instrument_type": "spot",
		"symbol": "ETH-USDC", "date": "2026-08-22", "hour": "00",
	}
}

func normalizationTime(second int) time.Time {
	return time.Date(2026, 8, 22, 0, 0, second, 0, time.UTC)
}

func assertNormalizationCounts(t *testing.T, result dataset.CompactResult, inputRows, outputRows, deduplicatedRows int64) {
	t.Helper()
	if result.InputRows != inputRows || result.OutputRows != outputRows || result.DeduplicatedRows != deduplicatedRows || result.NumRows != outputRows {
		t.Fatalf("compaction counts = %+v", result)
	}
}
