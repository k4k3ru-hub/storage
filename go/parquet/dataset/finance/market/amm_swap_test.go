package market

import (
	"context"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

func TestAMMSwapDatasetRoundTripsCrossChainProvenance(t *testing.T) {
	value, err := NewAMMSwapDataset(newAMMTestClient(t), AMMSwapDatasetParams{Root: "amm-swaps", FileName: "part.parquet", WriteMode: dataset.WriteModeCreate})
	if err != nil {
		t.Fatal(err)
	}
	partition := ammSwapTestPartition()
	stateType, stateValue, feeRate := "checkpoint", "123456", 0.0005
	record := AMMSwap{
		EventTimestamp: time.Date(2026, 8, 22, 12, 1, 2, 345000000, time.UTC), ReceivedTimestamp: time.Date(2026, 8, 22, 12, 1, 3, 456000000, time.UTC),
		SwapID: "sui:tx-digest:7", Chain: "sui", PoolID: "0xpool", TransactionID: "tx-digest", EventIndex: "7",
		StateReferenceType: &stateType, StateReferenceValue: &stateValue,
		Side: "buy", Price: 2500, BaseQuantity: 2, QuoteQuantity: 5000, EffectiveFeeRate: &feeRate,
	}
	result, err := value.Write(context.Background(), AMMSwapWriteParams{Partition: partition, Records: []AMMSwap{record}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := result.Key, "amm-swaps/asset_class=crypto/venue=cetus/instrument_type=spot/symbol=ETH-USDC/date=2026-08-22/hour=12/part.parquet"; got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
	read, err := value.Read(context.Background(), AMMSwapReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 1 {
		t.Fatalf("records = %d, want 1", len(read.Records))
	}
	got := read.Records[0]
	if got.SwapID != record.SwapID || got.Chain != record.Chain || got.PoolID != record.PoolID || got.TransactionID != record.TransactionID || got.EventIndex != record.EventIndex || got.Side != record.Side || got.Price != record.Price || got.BaseQuantity != record.BaseQuantity || got.QuoteQuantity != record.QuoteQuantity || got.StateReferenceType == nil || *got.StateReferenceType != stateType || got.StateReferenceValue == nil || *got.StateReferenceValue != stateValue || got.EffectiveFeeRate == nil || *got.EffectiveFeeRate != feeRate {
		t.Fatalf("record = %+v, want %+v", got, record)
	}
}

func TestAMMSwapCompactionSortsAndDeduplicatesBySwapID(t *testing.T) {
	value, err := NewAMMSwapDataset(newAMMTestClient(t), AMMSwapDatasetParams{Root: "amm-swaps"})
	if err != nil {
		t.Fatal(err)
	}
	partition := ammSwapTestPartition()
	base := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	earlier := AMMSwap{EventTimestamp: base.Add(time.Second), ReceivedTimestamp: base.Add(2 * time.Second), SwapID: "swap-1", Chain: "sui", PoolID: "pool", TransactionID: "tx-1", EventIndex: "0", Side: "buy", Price: 100, BaseQuantity: 1, QuoteQuantity: 100}
	later := AMMSwap{EventTimestamp: base.Add(3 * time.Second), ReceivedTimestamp: base.Add(4 * time.Second), SwapID: "swap-2", Chain: "sui", PoolID: "pool", TransactionID: "tx-2", EventIndex: "0", Side: "sell", Price: 101, BaseQuantity: 2, QuoteQuantity: 202}
	duplicate := earlier
	duplicate.ReceivedTimestamp = duplicate.ReceivedTimestamp.Add(time.Second)
	if _, err := value.Write(context.Background(), AMMSwapWriteParams{Partition: partition, Records: []AMMSwap{later, earlier}}); err != nil {
		t.Fatal(err)
	}
	if _, err := value.Write(context.Background(), AMMSwapWriteParams{Partition: partition, Records: []AMMSwap{duplicate}}); err != nil {
		t.Fatal(err)
	}
	result, err := value.Compact(context.Background(), AMMSwapCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Compacted || result.InputRows != 3 || result.OutputRows != 2 || result.DeduplicatedRows != 1 {
		t.Fatalf("compaction result = %+v", result)
	}
	read, err := value.Read(context.Background(), AMMSwapReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 2 || read.Records[0].SwapID != "swap-1" || read.Records[1].SwapID != "swap-2" {
		t.Fatalf("records = %+v", read.Records)
	}
}

func ammSwapTestPartition() dataset.Partition {
	return dataset.Partition{"asset_class": "crypto", "venue": "cetus", "instrument_type": "spot", "symbol": "ETH-USDC", "date": "2026-08-22", "hour": "12"}
}
