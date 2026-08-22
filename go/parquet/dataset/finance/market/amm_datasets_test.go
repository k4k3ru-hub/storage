package market

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store/local"
)

func TestAMMExecutableQuoteDatasetWriteRead(t *testing.T) {
	parquetClient := newAMMTestClient(t)
	value, err := NewAMMExecutableQuoteDataset(parquetClient, AMMExecutableQuoteDatasetParams{
		Root:     "market-data/amm-executable-quotes",
		FileName: "quotes",
	})
	if err != nil {
		t.Fatal(err)
	}
	partition := ammExecutableQuoteTestPartition()
	stateType := "block_number"
	stateValue := "24567890"
	feeRate := 0.0005
	records := []AMMExecutableQuote{{
		EventTimestamp:      time.Date(2026, 8, 22, 0, 1, 2, 345000000, time.UTC),
		ReceivedTimestamp:   time.Date(2026, 8, 22, 0, 1, 2, 346000000, time.UTC),
		Chain:               "ethereum",
		PoolID:              "0x1234",
		StateReferenceType:  &stateType,
		StateReferenceValue: &stateValue,
		BidPrice:            118000.25,
		BidQuantity:         1,
		AskPrice:            118001.5,
		AskQuantity:         1,
		EffectiveFeeRate:    &feeRate,
		FeeIncluded:         true,
	}}
	result, err := value.Write(context.Background(), AMMExecutableQuoteWriteParams{Partition: partition, Records: records})
	if err != nil {
		t.Fatal(err)
	}
	wantKey := "market-data/amm-executable-quotes/asset_class=crypto/venue=uniswap-v4/instrument_type=spot/symbol=BTC-USDC/date=2026-08-22/hour=00/quotes.parquet"
	if result.Key != wantKey {
		t.Fatalf("write key = %q, want %q", result.Key, wantKey)
	}
	read, err := value.Read(context.Background(), AMMExecutableQuoteReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read.Records, records) {
		t.Fatalf("records = %+v, want %+v", read.Records, records)
	}
}

func TestAMMPoolMetadataDatasetWriteRead(t *testing.T) {
	parquetClient := newAMMTestClient(t)
	value, err := NewAMMPoolMetadataDataset(parquetClient, AMMPoolMetadataDatasetParams{
		Root: "reference-data/amm-pools",
	})
	if err != nil {
		t.Fatal(err)
	}
	partition := ammPoolMetadataTestPartition()
	supersedesID := "metadata-0"
	effectiveTimestamp := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	feeRate := 0.0005
	tickSpacing := uint32(60)
	hooks := "0x5678"
	configurationID := "config-1"
	records := []AMMPoolMetadata{{
		MetadataID:         "metadata-1",
		SupersedesID:       &supersedesID,
		ObservedTimestamp:  time.Date(2026, 8, 22, 0, 0, 0, 123000000, time.UTC),
		EffectiveTimestamp: &effectiveTimestamp,
		Chain:              "ethereum",
		PoolID:             "0x1234",
		BaseAssetID:        "0xbase",
		QuoteAssetID:       "0xquote",
		BaseDecimals:       18,
		QuoteDecimals:      6,
		FeeModel:           "static",
		FeeRate:            &feeRate,
		PriceGridType:      "tick",
		TickSpacing:        &tickSpacing,
		Hooks:              &hooks,
		ConfigurationID:    &configurationID,
		Fingerprint:        "sha256:abcdef",
	}}
	result, err := value.Write(context.Background(), AMMPoolMetadataWriteParams{Partition: partition, Records: records})
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := "reference-data/amm-pools/asset_class=crypto/venue=uniswap-v4/part-"
	if !strings.HasPrefix(result.Key, wantPrefix) || !strings.HasSuffix(result.Key, ".parquet") {
		t.Fatalf("write key = %q, want %q prefix and .parquet suffix", result.Key, wantPrefix)
	}
	read, err := value.Read(context.Background(), AMMPoolMetadataReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read.Records, records) {
		t.Fatalf("records = %+v, want %+v", read.Records, records)
	}
}

func TestAMMDatasetsCompactPreservesRecords(t *testing.T) {
	t.Run("executable quotes", func(t *testing.T) {
		parquetClient := newAMMTestClient(t)
		value, err := NewAMMExecutableQuoteDataset(parquetClient, AMMExecutableQuoteDatasetParams{Root: "quotes"})
		if err != nil {
			t.Fatal(err)
		}
		partition := ammExecutableQuoteTestPartition()
		records := []AMMExecutableQuote{
			{EventTimestamp: time.Unix(1, 0).UTC(), ReceivedTimestamp: time.Unix(1, 1000).UTC(), Chain: "ethereum", PoolID: "pool-1", BidPrice: 100, BidQuantity: 1, AskPrice: 101, AskQuantity: 1, FeeIncluded: true},
			{EventTimestamp: time.Unix(2, 0).UTC(), ReceivedTimestamp: time.Unix(2, 1000).UTC(), Chain: "base", PoolID: "pool-2", BidPrice: 102, BidQuantity: 1, AskPrice: 103, AskQuantity: 1, FeeIncluded: true},
		}
		for _, record := range records {
			if _, err := value.Write(context.Background(), AMMExecutableQuoteWriteParams{Partition: partition, Records: []AMMExecutableQuote{record}}); err != nil {
				t.Fatal(err)
			}
		}
		result, err := value.Compact(context.Background(), AMMExecutableQuoteCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Compacted {
			t.Fatal("compaction result is not marked compacted")
		}
		read, err := value.Read(context.Background(), AMMExecutableQuoteReadParams{Partition: partition})
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(read.Records, func(i, j int) bool { return read.Records[i].PoolID < read.Records[j].PoolID })
		if !reflect.DeepEqual(read.Records, records) {
			t.Fatalf("records = %+v, want %+v", read.Records, records)
		}
	})

	t.Run("pool metadata", func(t *testing.T) {
		parquetClient := newAMMTestClient(t)
		value, err := NewAMMPoolMetadataDataset(parquetClient, AMMPoolMetadataDatasetParams{Root: "metadata"})
		if err != nil {
			t.Fatal(err)
		}
		partition := ammPoolMetadataTestPartition()
		records := []AMMPoolMetadata{
			{MetadataID: "metadata-1", ObservedTimestamp: time.Unix(1, 0).UTC(), Chain: "ethereum", PoolID: "pool-1", BaseAssetID: "base", QuoteAssetID: "quote", FeeModel: "static", PriceGridType: "tick", Fingerprint: "one"},
			{MetadataID: "metadata-2", ObservedTimestamp: time.Unix(2, 0).UTC(), Chain: "ethereum", PoolID: "pool-1", BaseAssetID: "base", QuoteAssetID: "quote", FeeModel: "static", PriceGridType: "tick", Fingerprint: "two"},
		}
		for _, record := range records {
			if _, err := value.Write(context.Background(), AMMPoolMetadataWriteParams{Partition: partition, Records: []AMMPoolMetadata{record}}); err != nil {
				t.Fatal(err)
			}
		}
		result, err := value.Compact(context.Background(), AMMPoolMetadataCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Compacted {
			t.Fatal("compaction result is not marked compacted")
		}
		read, err := value.Read(context.Background(), AMMPoolMetadataReadParams{Partition: partition})
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(read.Records, func(i, j int) bool { return read.Records[i].MetadataID < read.Records[j].MetadataID })
		if !reflect.DeepEqual(read.Records, records) {
			t.Fatalf("records = %+v, want %+v", read.Records, records)
		}
	})
}

func newAMMTestClient(t *testing.T) *client.Client {
	t.Helper()
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	parquetClient, err := client.New(client.Params{Store: localStore})
	if err != nil {
		t.Fatal(err)
	}
	return parquetClient
}

func ammExecutableQuoteTestPartition() dataset.Partition {
	return dataset.Partition{
		"asset_class":     "crypto",
		"venue":           "uniswap-v4",
		"instrument_type": "spot",
		"symbol":          "BTC-USDC",
		"date":            "2026-08-22",
		"hour":            "00",
	}
}

func ammPoolMetadataTestPartition() dataset.Partition {
	return dataset.Partition{"asset_class": "crypto", "venue": "uniswap-v4"}
}
