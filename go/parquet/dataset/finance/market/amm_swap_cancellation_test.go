package market

import (
	"bytes"
	"context"
	"fmt"
	"github.com/k4k3ru-hub/storage/go/parquet/store"
	parquetgo "github.com/parquet-go/parquet-go"
	"testing"
	"time"
)

// TestAMMSwapCompactionPreservesCancellationAndReplacement verifies cancellation persistence compatibility.
//
// Version:
//   - 2026-09-12: Added.
func TestAMMSwapCompactionPreservesCancellationAndReplacement(t *testing.T) {
	ctx := context.Background()
	d, err := NewAMMSwapDataset(newAMMTestClient(t), AMMSwapDatasetParams{Root: "amm-swaps"})
	if err != nil {
		t.Fatal(err)
	}
	network, oldHash, newHash := "mainnet", "old", "replacement"
	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	original := AMMSwap{EventTimestamp: at, ReceivedTimestamp: at, SwapID: "same-tx:44", Chain: "ethereum", Network: &network, BlockHash: &oldHash, PoolID: "pool", TransactionID: "same-tx", EventIndex: "44", Side: "buy", Price: 100, BaseQuantity: 1, QuoteQuantity: 100}
	removed := original
	removed.Removed = true
	removed.EventTimestamp = at.Add(time.Second)
	replacement := original
	replacement.BlockHash = &newHash
	replacement.EventTimestamp = at.Add(2 * time.Second)
	records := []AMMSwap{original, removed, replacement}
	for i := 0; i < 2; i++ {
		if _, err := d.Write(ctx, AMMSwapWriteParams{Partition: ammSwapTestPartition(), Records: records}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := d.Compact(ctx, AMMSwapCompactParams{Partition: ammSwapTestPartition(), TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputRows != 3 || result.DeduplicatedRows != 3 {
		t.Fatalf("result=%+v", result)
	}
	read, err := d.Read(ctx, AMMSwapReadParams{Partition: ammSwapTestPartition()})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Records) != 3 {
		t.Fatalf("records=%+v", read.Records)
	}
	for i, got := range read.Records {
		want := records[i]
		if got.Removed != want.Removed || got.BlockHash == nil || *got.BlockHash != *want.BlockHash || got.Network == nil || *got.Network != network {
			t.Fatalf("record[%d]=%+v", i, got)
		}
	}
	policy := ammSwapCompactionPolicy{}
	key, _ := policy.DeduplicationKey(original)
	for _, modify := range []func(*AMMSwap){func(r *AMMSwap) { s := "testnet"; r.Network = &s }, func(r *AMMSwap) { r.PoolID = "other" }, func(r *AMMSwap) { r.BlockHash = nil }} {
		other := original
		modify(&other)
		got, _ := policy.DeduplicationKey(other)
		if got == key {
			t.Fatal("distinct provenance collapsed")
		}
	}
}

// Legacy files predate network, block_hash, and removed columns.
type legacyAMMSwapRow struct {
	EventTimestamp      int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp   int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	SwapID              string   `parquet:"swap_id"`
	Chain               string   `parquet:"chain"`
	PoolID              string   `parquet:"pool_id"`
	TransactionID       string   `parquet:"transaction_id"`
	EventIndex          string   `parquet:"event_index"`
	StateReferenceType  *string  `parquet:"state_reference_type,optional"`
	StateReferenceValue *string  `parquet:"state_reference_value,optional"`
	Side                string   `parquet:"side"`
	Price               float64  `parquet:"price"`
	BaseQuantity        float64  `parquet:"base_quantity"`
	QuoteQuantity       float64  `parquet:"quote_quantity"`
	EffectiveFeeRate    *float64 `parquet:"effective_fee_rate,optional"`
}

// TestAMMSwapCodecReadsLegacyFile verifies cancellation persistence compatibility.
//
// Version:
//   - 2026-09-12: Added.
func TestAMMSwapCodecReadsLegacyFile(t *testing.T) {
	var buf bytes.Buffer
	writer := parquetgo.NewGenericWriter[legacyAMMSwapRow](&buf)
	if _, err := writer.Write([]legacyAMMSwapRow{{SwapID: "legacy", Chain: "sui", Price: 100}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	records, err := NewAMMSwapCodec().Decode(context.Background(), bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].SwapID != "legacy" || records[0].Removed || records[0].Network != nil || records[0].BlockHash != nil {
		t.Fatalf("legacy=%+v", records)
	}
}

// TestAMMSwapCompactsLegacyAndMixedSchemas verifies additive schema migration.
//
// Version:
//   - 2026-09-12: Added.
func TestAMMSwapCompactsLegacyAndMixedSchemas(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint(mixed), func(t *testing.T) {
			ctx := context.Background()
			client := newAMMTestClient(t)
			d, err := NewAMMSwapDataset(client, AMMSwapDatasetParams{Root: "amm-swaps"})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				key := fmt.Sprintf("amm-swaps/asset_class=crypto/venue=cetus/instrument_type=spot/symbol=ETH-USDC/date=2026-08-22/hour=12/old-%d.parquet", i)
				out, err := client.Store().Create(ctx, key, store.CreateParams{})
				if err != nil {
					t.Fatal(err)
				}
				writer := parquetgo.NewGenericWriter[legacyAMMSwapRow](out, parquetgo.Compression(&parquetgo.Zstd))
				if _, err := writer.Write([]legacyAMMSwapRow{{SwapID: "legacy", Chain: "sui", Price: 100}}); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if err := out.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if mixed {
				hash := "block"
				if _, err := d.Write(ctx, AMMSwapWriteParams{Partition: ammSwapTestPartition(), Records: []AMMSwap{{SwapID: "cancel", Removed: true, BlockHash: &hash}}}); err != nil {
					t.Fatal(err)
				}
			}
			result, err := d.Compact(ctx, AMMSwapCompactParams{Partition: ammSwapTestPartition(), TargetFileSizeBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			want := int64(1)
			if mixed {
				want++
			}
			if result.OutputRows != want || result.DeduplicatedRows != 1 {
				t.Fatalf("result=%+v", result)
			}
			read, err := d.Read(ctx, AMMSwapReadParams{Partition: ammSwapTestPartition()})
			if err != nil {
				t.Fatal(err)
			}
			var canceled int
			for _, r := range read.Records {
				if r.Removed {
					canceled++
				}
				if r.SwapID == "legacy" && (r.Network != nil || r.BlockHash != nil || r.Removed) {
					t.Fatalf("legacy=%+v", r)
				}
			}
			if mixed && canceled != 1 {
				t.Fatal("lost cancellation")
			}
		})
	}
}

// TestAMMSwapSchemaCompatibilityRejectsUnrelatedChanges preserves strict validation.
//
// Version:
//   - 2026-09-12: Added.
func TestAMMSwapSchemaCompatibilityRejectsUnrelatedChanges(t *testing.T) {
	for _, mode := range []string{"missing_price", "wrong_price", "extra"} {
		group := parquetgo.Group{}
		for _, f := range parquetgo.SchemaOf(new(ammSwapRow)).Fields() {
			group[f.Name()] = f
		}
		switch mode {
		case "missing_price":
			delete(group, "price")
		case "wrong_price":
			group["price"] = parquetgo.Leaf(parquetgo.Int64Type)
		case "extra":
			group["unexpected"] = parquetgo.Leaf(parquetgo.Int64Type)
		}
		if compatibleAMMSwapSchema(parquetgo.NewSchema("test", group)) {
			t.Fatalf("accepted %s", mode)
		}
	}
}
