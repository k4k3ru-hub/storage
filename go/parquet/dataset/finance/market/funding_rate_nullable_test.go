package market

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/store"
	parquetgo "github.com/parquet-go/parquet-go"
)

// TestFundingRateOptionalTarget validates that only current estimates may omit the target.
//
// Version:
//   - 2026-10-22: Added.
func TestFundingRateOptionalTarget(t *testing.T) {
	for _, kind := range []FundingRateKind{FundingRateKindCurrentEstimate, FundingRateKindPredicted, FundingRateKindSettled} {
		t.Run(string(kind), func(t *testing.T) {
			at := time.Date(2026, 10, 22, 0, 0, 0, 123456000, time.UTC)
			r := FundingRate{EventTimestamp: at, ReceivedTimestamp: at, Kind: kind, IntervalMinutes: 60}
			if err := r.Validate(); (err == nil) != (kind == FundingRateKindCurrentEstimate) {
				t.Fatalf("kind=%s error=%v", kind, err)
			}
		})
	}
}

// TestFundingRateNullableRoundTrip checks null encoding, known targets, and batch compaction codecs.
//
// Version:
//   - 2026-10-22: Added.
func TestFundingRateNullableRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 22, 0, 0, 0, 123456000, time.UTC)
	want := []FundingRate{
		{EventTimestamp: at, ReceivedTimestamp: at, Rate: 0, Kind: FundingRateKindCurrentEstimate, IntervalMinutes: 60},
		{EventTimestamp: at.Add(time.Minute), ReceivedTimestamp: at.Add(time.Minute), Rate: -0.00002, Kind: FundingRateKindCurrentEstimate, IntervalMinutes: 60},
		{EventTimestamp: at.Add(time.Hour), ReceivedTimestamp: at.Add(time.Hour), FundingTimestamp: at.Add(time.Hour), Rate: 0.00001, Kind: FundingRateKindSettled, IntervalMinutes: 60},
	}
	codec := NewFundingRateCodec()
	var buf bytes.Buffer
	if err := codec.Encode(t.Context(), &buf, want); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v want=%+v", got, want)
	}
	file, err := parquetgo.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	reader := parquetgo.NewGenericReader[fundingRateRow](file)
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	rows := make([]fundingRateRow, 3)
	if n, err := reader.Read(rows); n != 3 || (err != nil && !errors.Is(err, io.EOF)) {
		t.Fatalf("rows=%d error=%v", n, err)
	}
	if rows[0].FundingTimestamp != nil || rows[1].FundingTimestamp != nil || rows[2].FundingTimestamp == nil || *rows[2].FundingTimestamp != want[2].FundingTimestamp.UnixMicro() {
		t.Fatalf("null targets or microsecond timestamp changed: %+v", rows)
	}
	assertBatchRoundTrip(t, codec, want)
}

// This mirrors the old required-column schema independently of the new codec.
type legacyFundingRateRow struct {
	EventTimestamp    int64    `parquet:"event_timestamp,timestamp(microsecond)"`
	ReceivedTimestamp int64    `parquet:"received_timestamp,timestamp(microsecond)"`
	FundingTimestamp  int64    `parquet:"funding_timestamp,timestamp(microsecond)"`
	Rate              float64  `parquet:"rate"`
	Kind              string   `parquet:"kind"`
	IntervalMinutes   int32    `parquet:"interval_minutes"`
	MarkPrice         *float64 `parquet:"mark_price,optional"`
	IndexPrice        *float64 `parquet:"index_price,optional"`
	PremiumRate       *float64 `parquet:"premium_rate,optional"`
}

// TestFundingRateReadsLegacyRequiredTimestamp checks both readers against a file from the old schema.
//
// Version:
//   - 2026-10-22: Added.
func TestFundingRateReadsLegacyRequiredTimestamp(t *testing.T) {
	at := time.Date(2026, 10, 22, 0, 0, 0, 123456000, time.UTC)
	want := FundingRate{EventTimestamp: at, ReceivedTimestamp: at, FundingTimestamp: at.Add(time.Hour), Rate: 0.0001, Kind: FundingRateKindCurrentEstimate, IntervalMinutes: 60}
	var buf bytes.Buffer
	w := parquetgo.NewGenericWriter[legacyFundingRateRow](&buf)
	if _, err := w.Write([]legacyFundingRateRow{{EventTimestamp: at.UnixMicro(), ReceivedTimestamp: at.UnixMicro(), FundingTimestamp: want.FundingTimestamp.UnixMicro(), Rate: want.Rate, Kind: string(want.Kind), IntervalMinutes: 60}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	codec := NewFundingRateCodec()
	got, err := codec.Decode(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil || len(got) != 1 {
		t.Fatalf("legacy decode: got=%+v error=%v", got, err)
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("legacy target changed: %+v", got[0])
	}
	r, err := codec.NewBatchReader(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	batch := make([]FundingRate, 1)
	if n, err := r.Read(context.Background(), batch); n != 1 || (err != nil && !errors.Is(err, io.EOF)) {
		t.Fatalf("legacy batch: n=%d error=%v", n, err)
	}
	if !reflect.DeepEqual(batch[0], want) {
		t.Fatalf("legacy batch target changed: %+v", batch[0])
	}
}

// TestFundingRateCompactionPreservesNullTarget checks ordering and deduplication with absent targets.
//
// Version:
//   - 2026-10-22: Added.
func TestFundingRateCompactionPreservesNullTarget(t *testing.T) {
	d, err := NewFundingRateDataset(newNormalizationTestClient(t), FundingRateDatasetParams{Root: "funding-rates"})
	if err != nil {
		t.Fatal(err)
	}
	at := normalizationTime(1)
	first := FundingRate{EventTimestamp: at, ReceivedTimestamp: at, Rate: 0, Kind: FundingRateKindCurrentEstimate, IntervalMinutes: 60}
	second := first
	second.EventTimestamp = at.Add(time.Minute)
	second.ReceivedTimestamp = second.EventTimestamp
	second.Rate = -0.0001
	partition := normalizationIntradayPartition()
	writeNormalizationParts(t, d.Write, partition, []FundingRate{second, first}, []FundingRate{first})
	result, err := d.Compact(t.Context(), FundingRateCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	assertNormalizationCounts(t, result, 3, 2, 1)
	read, err := d.Read(t.Context(), FundingRateReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read.Records, []FundingRate{first, second}) {
		t.Fatalf("compacted=%+v", read.Records)
	}
}

// TestFundingRateCompactionAcrossSchemas exercises old files and the nullable output in one partition.
//
// Version:
//   - 2026-10-22: Added.
func TestFundingRateCompactionAcrossSchemas(t *testing.T) {
	client := newNormalizationTestClient(t)
	d, err := NewFundingRateDataset(client, FundingRateDatasetParams{Root: "funding-rates"})
	if err != nil {
		t.Fatal(err)
	}
	at := normalizationTime(1)
	legacy := FundingRate{EventTimestamp: at, ReceivedTimestamp: at, FundingTimestamp: at.Add(time.Hour), Rate: 0.0001, Kind: FundingRateKindCurrentEstimate, IntervalMinutes: 60}
	partition := normalizationIntradayPartition()
	written, err := d.Write(t.Context(), FundingRateWriteParams{Partition: partition, Records: []FundingRate{legacy}})
	if err != nil {
		t.Fatal(err)
	}
	object, err := client.Store().Create(t.Context(), written.Key, store.CreateParams{Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	writer := parquetgo.NewGenericWriter[legacyFundingRateRow](object, parquetgo.Compression(&parquetgo.Zstd))
	if _, err := writer.Write([]legacyFundingRateRow{{EventTimestamp: at.UnixMicro(), ReceivedTimestamp: at.UnixMicro(), FundingTimestamp: legacy.FundingTimestamp.UnixMicro(), Rate: legacy.Rate, Kind: string(legacy.Kind), IntervalMinutes: 60}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := object.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	nullable := legacy
	nullable.EventTimestamp = at.Add(time.Minute)
	nullable.ReceivedTimestamp = nullable.EventTimestamp
	nullable.FundingTimestamp = time.Time{}
	if _, err := d.Write(t.Context(), FundingRateWriteParams{Partition: partition, Records: []FundingRate{nullable}}); err != nil {
		t.Fatal(err)
	}
	result, err := d.Compact(t.Context(), FundingRateCompactParams{Partition: partition, TargetFileSizeBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	assertNormalizationCounts(t, result, 2, 2, 0)
	got, err := d.Read(t.Context(), FundingRateReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Records, []FundingRate{legacy, nullable}) {
		t.Fatalf("mixed compaction=%+v", got.Records)
	}
}

// TestFundingSchemaCompatibilityIsNarrow keeps unrelated nullability mismatches distinct.
//
// Version:
//   - 2026-10-22: Added.
func TestFundingSchemaCompatibilityIsNarrow(t *testing.T) {
	current := parquetgo.SchemaOf(new(fundingRateRow))
	want := fundingRateSchemaFingerprint(current)
	for _, fieldName := range []string{"rate", "received_timestamp", "mark_price"} {
		fields := parquetgo.Group{}
		for _, field := range current.Fields() {
			var node parquetgo.Node = field
			if field.Name() == fieldName {
				if field.Optional() {
					node = parquetgo.Required(field)
				} else {
					node = parquetgo.Optional(field)
				}
			}
			fields[field.Name()] = node
		}
		if fundingRateSchemaFingerprint(parquetgo.NewSchema("funding_rate", fields)) == want {
			t.Fatalf("accepted unrelated nullability change: %s", fieldName)
		}
	}
}
