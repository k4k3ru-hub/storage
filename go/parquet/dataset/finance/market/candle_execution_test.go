package market

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store"
	parquetgo "github.com/parquet-go/parquet-go"
)

type legacyCandleRow struct {
	Timestamp int64   `parquet:"timestamp,timestamp(microsecond)"`
	Open      float64 `parquet:"open"`
	High      float64 `parquet:"high"`
	Low       float64 `parquet:"low"`
	Close     float64 `parquet:"close"`
	Volume    float64 `parquet:"volume"`
}

// TestCandleExecutionColumnsAndLegacyCompatibility verifies round trips and old-reader compatibility.
//
// Version:
//   - 2026-09-23: Added.
func TestCandleExecutionColumnsAndLegacyCompatibility(t *testing.T) {
	ctx := context.Background()
	q, b, s, u := 1234.0, 3.0, 2.0, 1.0
	count, quality := uint64(6), uint64(0)
	want := Candle{Timestamp: time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC), Open: 100, High: 210, Low: 90, Close: 200, Volume: 6,
		QuoteVolume: &q, BuyBaseVolume: &b, SellBaseVolume: &s, UnknownBaseVolume: &u, TradeCount: &count, Quality: &quality,
		VenueSymbol: "BTC-USDT", Chain: "base", Network: "mainnet", PoolID: "pool", RunID: "run", Version: 5}
	var buf bytes.Buffer
	codec := NewCandleCodec()
	if err := codec.Encode(ctx, &buf, []Candle{want}); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(ctx, bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("round trip=%+v err=%v", got, err)
	}
	legacy := parquetgo.NewGenericReader[legacyCandleRow](bytes.NewReader(buf.Bytes()))
	old := make([]legacyCandleRow, 1)
	if n, err := legacy.Read(old); n != 1 || (err != nil && !errors.Is(err, io.EOF)) {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if old[0].Volume != 6 || old[0].Close != 200 {
		t.Fatalf("old reader=%+v", old)
	}

	var oldBuf bytes.Buffer
	writer := parquetgo.NewGenericWriter[legacyCandleRow](&oldBuf, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write(old); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = codec.Decode(ctx, bytes.NewReader(oldBuf.Bytes()), int64(oldBuf.Len()))
	if err != nil || len(got) != 1 || got[0].QuoteVolume != nil || got[0].Quality != nil || got[0].Volume != 6 {
		t.Fatalf("unknown legacy totals became complete: %+v err=%v", got, err)
	}
}

// TestCandleSnapshotBoundsAndCompactionIsolation verifies the shared writer and compactor contract.
//
// Version:
//   - 2026-09-23: Added.
func TestCandleSnapshotBoundsAndCompactionIsolation(t *testing.T) {
	ctx := context.Background()
	c := newNormalizationTestClient(t)
	snapshot, err := NewCandleDataset(c, CandleDatasetParams{Root: "candles", FileName: CandleSnapshotFilePrefix + "source-08.parquet", WriteMode: dataset.WriteModeOverwrite, MaxRowsPerFile: 60})
	if err != nil {
		t.Fatal(err)
	}
	partition := candleCompactionPartition()
	row := Candle{Timestamp: normalizationTime(1), Open: 100, High: 100, Low: 100, Close: 100, Volume: 1, PoolID: "pool", Chain: "base", Network: "mainnet"}
	result, err := snapshot.Write(ctx, CandleWriteParams{Partition: partition, Records: []Candle{row}})
	if err != nil {
		t.Fatal(err)
	}
	row.Close = 101
	if _, err := snapshot.Write(ctx, CandleWriteParams{Partition: partition, Records: []Candle{row}}); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Write(ctx, CandleWriteParams{Partition: partition, Records: make([]Candle, 61)}); err == nil {
		t.Fatal("oversized write accepted")
	}
	regular, err := NewCandleDataset(c, CandleDatasetParams{Root: "candles"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		records := []Candle{{Timestamp: normalizationTime(2), Open: 100, Volume: 2, PoolID: "pool-a"}, {Timestamp: normalizationTime(2), Open: 100, Volume: 2, PoolID: "pool-b"}}
		if _, err := regular.Write(ctx, CandleWriteParams{Partition: partition, Records: records}); err != nil {
			t.Fatal(err)
		}
	}
	compacted, err := regular.Compact(ctx, CandleCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil || compacted.InputRows != 4 || compacted.OutputRows != 2 {
		t.Fatalf("pool isolation=%+v err=%v", compacted, err)
	}
	for _, key := range compacted.InputFiles {
		if key == result.Key {
			t.Fatal("mutable snapshot compacted")
		}
	}
	read, err := snapshot.Read(ctx, CandleReadParams{Partition: partition})
	if err != nil || len(read.Records) != 1 || read.Records[0].Close != 101 {
		t.Fatalf("snapshot lost: %+v %v", read, err)
	}
	// A misconfigured/older producer cannot force unbounded allocations in readers.
	unbounded, err := NewCandleDataset(c, CandleDatasetParams{Root: "candles", FileName: CandleSnapshotFilePrefix + "source-08.parquet", WriteMode: dataset.WriteModeOverwrite})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unbounded.Write(ctx, CandleWriteParams{Partition: partition, Records: make([]Candle, 61)}); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.Read(ctx, CandleReadParams{Partition: partition}); err == nil {
		t.Fatal("oversized read accepted")
	}
}

// TestCandleCompactionAcceptsLegacySchema verifies additive columns do not block historical compaction.
//
// Version:
//   - 2026-09-23: Added.
func TestCandleCompactionAcceptsLegacySchema(t *testing.T) {
	ctx := context.Background()
	c := newNormalizationTestClient(t)
	d, err := NewCandleDataset(c, CandleDatasetParams{Root: "candles"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := d.Write(ctx, CandleWriteParams{Partition: candleCompactionPartition(), Records: []Candle{{Timestamp: normalizationTime(2), Volume: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	key := path.Join(path.Dir(result.Key), "part-legacy.parquet")
	object, err := c.Store().Create(ctx, key, store.CreateParams{})
	if err != nil {
		t.Fatal(err)
	}
	writer := parquetgo.NewGenericWriter[legacyCandleRow](object, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write([]legacyCandleRow{{Timestamp: normalizationTime(1).UnixMicro(), Volume: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := object.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	compacted, err := d.Compact(ctx, CandleCompactParams{Partition: candleCompactionPartition(), TargetFileSizeBytes: 1 << 20})
	if err != nil || compacted.OutputRows != 2 {
		t.Fatalf("legacy compaction=%+v err=%v", compacted, err)
	}
	read, err := d.Read(ctx, CandleReadParams{Partition: candleCompactionPartition()})
	if err != nil || len(read.Records) != 2 || read.Records[0].Quality != nil {
		t.Fatalf("legacy read=%+v err=%v", read, err)
	}
	if _, err := c.Store().Open(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("legacy source was not compacted: %v", err)
	}
}
