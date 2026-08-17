// compaction_test.go
package market

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store"
	"github.com/k4k3ru-hub/storage/go/parquet/store/local"
)

func TestCandleDatasetCompact(t *testing.T) {
	root := t.TempDir()
	localStore, value := newCandleDatasetForCompaction(t, root)
	partition := candleCompactionPartition()
	records := candleCompactionRecords(4)
	inputFiles := writeCandleParts(t, value, partition, records, 2)

	result, err := value.Compact(context.Background(), CandleCompactParams{
		Partition:           partition,
		TargetFileSizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Compacted {
		t.Fatal("compaction result is not marked compacted")
	}
	if got, want := len(result.InputFiles), 2; got != want {
		t.Fatalf("input files = %d, want %d", got, want)
	}
	if got, want := len(result.OutputFiles), 1; got != want {
		t.Fatalf("output files = %d, want %d", got, want)
	}
	if !strings.HasPrefix(filepath.Base(result.OutputFiles[0]), "compact-") {
		t.Fatalf("output file = %q, want compact- prefix", result.OutputFiles[0])
	}
	if result.NumRows != int64(len(records)) || result.InputBytes == 0 || result.OutputBytes == 0 {
		t.Fatalf("compaction result = %+v", result)
	}
	for _, key := range inputFiles {
		if _, err := localStore.Open(context.Background(), key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("open input %q error = %v, want ErrNotFound", key, err)
		}
	}
	assertCandleRecords(t, value, partition, records)
	assertNoCompactionTemporaryFiles(t, root)

	retry, err := value.Compact(context.Background(), CandleCompactParams{
		Partition:           partition,
		TargetFileSizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !isEmptyCompactResult(retry) {
		t.Fatalf("retry result = %+v, want no-op", retry)
	}
	assertCandleRecords(t, value, partition, records)
}

func TestCandleDatasetCompactSplitsOutputs(t *testing.T) {
	_, value := newCandleDatasetForCompaction(t, t.TempDir())
	partition := candleCompactionPartition()
	records := candleCompactionRecords(4)
	writeCandleParts(t, value, partition, records, 2)

	result, err := value.Compact(context.Background(), CandleCompactParams{
		Partition:           partition,
		TargetFileSizeBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(result.OutputFiles), len(records); got != want {
		t.Fatalf("output files = %d, want %d", got, want)
	}
	for index, key := range result.OutputFiles {
		wantSuffix := fmt.Sprintf("-%06d.parquet", index+1)
		if !strings.HasPrefix(filepath.Base(key), "compact-") || !strings.HasSuffix(key, wantSuffix) {
			t.Fatalf("output file = %q, want compact-*%s", key, wantSuffix)
		}
	}
	assertCandleRecords(t, value, partition, records)

	recompacted, err := value.Compact(context.Background(), CandleCompactParams{
		Partition:           partition,
		TargetFileSizeBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !recompacted.Compacted || len(recompacted.OutputFiles) != 1 {
		t.Fatalf("second compaction result = %+v", recompacted)
	}
	assertCandleRecords(t, value, partition, records)
}

func TestCandleDatasetCompactNoOp(t *testing.T) {
	_, value := newCandleDatasetForCompaction(t, t.TempDir())
	partition := candleCompactionPartition()
	params := CandleCompactParams{Partition: partition, TargetFileSizeBytes: 1024}

	result, err := value.Compact(context.Background(), params)
	if err != nil || !isEmptyCompactResult(result) {
		t.Fatalf("empty partition result = %+v error = %v", result, err)
	}
	writeCandleParts(t, value, partition, candleCompactionRecords(1), 1)
	result, err = value.Compact(context.Background(), params)
	if err != nil || !isEmptyCompactResult(result) {
		t.Fatalf("single part result = %+v error = %v", result, err)
	}
}

func TestCandleDatasetCompactRejectsInvalidTargetSize(t *testing.T) {
	_, value := newCandleDatasetForCompaction(t, t.TempDir())
	_, err := value.Compact(context.Background(), CandleCompactParams{
		Partition:           candleCompactionPartition(),
		TargetFileSizeBytes: 0,
	})
	if err == nil {
		t.Fatal("Compact accepted a non-positive target size")
	}
}

func TestCandleDatasetCompactHonorsCanceledContext(t *testing.T) {
	_, value := newCandleDatasetForCompaction(t, t.TempDir())
	partition := candleCompactionPartition()
	records := candleCompactionRecords(2)
	writeCandleParts(t, value, partition, records, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := value.Compact(ctx, CandleCompactParams{Partition: partition, TargetFileSizeBytes: 1024})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Compact error = %v, want context.Canceled", err)
	}
	assertCandleRecords(t, value, partition, records)
}

func TestCandleDatasetCompactPreservesInputsWhenDecodeFails(t *testing.T) {
	localStore, value := newCandleDatasetForCompaction(t, t.TempDir())
	partition := candleCompactionPartition()
	records := candleCompactionRecords(2)
	inputFiles := writeCandleParts(t, value, partition, records, 1)
	invalidKey := pathForPartition(partition, "invalid.parquet")
	writeRawObject(t, localStore, invalidKey, []byte("not parquet"))

	if _, err := value.Compact(context.Background(), CandleCompactParams{
		Partition: partition, TargetFileSizeBytes: 1024,
	}); err == nil {
		t.Fatal("Compact accepted an unreadable Parquet input")
	}
	for _, key := range append(inputFiles, invalidKey) {
		object, err := localStore.Open(context.Background(), key)
		if err != nil {
			t.Fatalf("input %q was not preserved: %v", key, err)
		}
		_ = object.Close()
	}
}

func TestCandleDatasetCompactPreservesInputsWhenPublishFails(t *testing.T) {
	root := t.TempDir()
	localStore, err := local.New(local.Params{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	failingStore := &compactionFaultStore{Store: localStore, failPublish: true}
	value := newCandleDatasetWithStore(t, failingStore)
	partition := candleCompactionPartition()
	records := candleCompactionRecords(2)
	inputFiles := writeCandleParts(t, value, partition, records, 1)

	if _, err := value.Compact(context.Background(), CandleCompactParams{
		Partition: partition, TargetFileSizeBytes: 1024,
	}); err == nil {
		t.Fatal("Compact succeeded when output publication failed")
	}
	for _, key := range inputFiles {
		object, err := localStore.Open(context.Background(), key)
		if err != nil {
			t.Fatalf("input %q was not preserved: %v", key, err)
		}
		_ = object.Close()
	}
	assertNoPublishedCompactionFiles(t, root)
	assertNoCompactionTemporaryFiles(t, root)
}

func TestCandleDatasetCompactReturnsInputDeleteFailure(t *testing.T) {
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	failingStore := &compactionFaultStore{Store: localStore, failInputDelete: true}
	value := newCandleDatasetWithStore(t, failingStore)
	partition := candleCompactionPartition()
	records := candleCompactionRecords(2)
	writeCandleParts(t, value, partition, records, 1)

	if _, err := value.Compact(context.Background(), CandleCompactParams{
		Partition: partition, TargetFileSizeBytes: 1024,
	}); err == nil {
		t.Fatal("Compact did not return an input deletion error")
	}
	result, err := value.Read(context.Background(), CandleReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Files) < 2 {
		t.Fatalf("files after delete failure = %d, want preserved inputs and published output", len(result.Files))
	}
}

func TestCandleDatasetCompactSnapshotsInputsAndRejectsConcurrentRun(t *testing.T) {
	localStore, err := local.New(local.Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	blockingStore := &compactionFaultStore{
		Store:          localStore,
		blockTemporary: make(chan struct{}),
		temporaryReady: make(chan struct{}),
	}
	value := newCandleDatasetWithStore(t, blockingStore)
	partition := candleCompactionPartition()
	records := candleCompactionRecords(3)
	writeCandleParts(t, value, partition, records[:2], 1)

	type compactResponse struct {
		result CandleCompactResult
		err    error
	}
	responseCh := make(chan compactResponse, 1)
	go func() {
		result, compactErr := value.Compact(context.Background(), CandleCompactParams{
			Partition: partition, TargetFileSizeBytes: 1024,
		})
		responseCh <- compactResponse{result: result, err: compactErr}
	}()
	<-blockingStore.temporaryReady

	if _, err := value.Compact(context.Background(), CandleCompactParams{
		Partition: partition, TargetFileSizeBytes: 1024,
	}); err == nil || !strings.Contains(err.Error(), "already in progress") {
		t.Fatalf("concurrent Compact error = %v, want already in progress", err)
	}
	lateResult, err := value.Write(context.Background(), CandleWriteParams{
		Partition: partition,
		Records:   records[2:],
	})
	if err != nil {
		t.Fatal(err)
	}
	close(blockingStore.blockTemporary)
	response := <-responseCh
	if response.err != nil {
		t.Fatal(response.err)
	}
	for _, key := range response.result.InputFiles {
		if key == lateResult.Key {
			t.Fatalf("late input %q was included in compaction snapshot", key)
		}
	}
	object, err := localStore.Open(context.Background(), lateResult.Key)
	if err != nil {
		t.Fatalf("late input was deleted: %v", err)
	}
	_ = object.Close()
	assertCandleRecords(t, value, partition, records)
}

func newCandleDatasetForCompaction(t *testing.T, root string) (*local.Store, *CandleDataset) {
	t.Helper()
	localStore, err := local.New(local.Params{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	return localStore, newCandleDatasetWithStore(t, localStore)
}

func newCandleDatasetWithStore(t *testing.T, objectStore store.Store) *CandleDataset {
	t.Helper()
	parquetClient, err := client.New(client.Params{Store: objectStore})
	if err != nil {
		t.Fatal(err)
	}
	value, err := NewCandleDataset(parquetClient, CandleDatasetParams{Root: "candles"})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func candleCompactionPartition() dataset.Partition {
	return dataset.Partition{
		"asset_class":     "crypto",
		"venue":           "binance",
		"instrument_type": "spot",
		"symbol":          "BTC-USDT",
		"timeframe":       "1m",
		"date":            "2026-08-18",
	}
}

func candleCompactionRecords(count int) []Candle {
	records := make([]Candle, count)
	for index := range records {
		records[index] = Candle{
			Timestamp: time.Date(2026, 8, 18, 1, index, 0, index*1000, time.UTC),
			Open:      100 + float64(index),
			High:      101 + float64(index),
			Low:       99 + float64(index),
			Close:     100.5 + float64(index),
			Volume:    10 + float64(index),
		}
	}
	return records
}

func writeCandleParts(
	t *testing.T,
	value *CandleDataset,
	partition dataset.Partition,
	records []Candle,
	batchSize int,
) []string {
	t.Helper()
	keys := make([]string, 0)
	for offset := 0; offset < len(records); offset += batchSize {
		end := offset + batchSize
		if end > len(records) {
			end = len(records)
		}
		result, err := value.Write(context.Background(), CandleWriteParams{
			Partition: partition,
			Records:   records[offset:end],
		})
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, result.Key)
	}
	return keys
}

func assertCandleRecords(t *testing.T, value *CandleDataset, partition dataset.Partition, want []Candle) {
	t.Helper()
	result, err := value.Read(context.Background(), CandleReadParams{Partition: partition})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != len(want) {
		t.Fatalf("records = %d, want %d", len(result.Records), len(want))
	}
	gotRecords := append([]Candle(nil), result.Records...)
	wantRecords := append([]Candle(nil), want...)
	sort.Slice(gotRecords, func(i, j int) bool { return gotRecords[i].Timestamp.Before(gotRecords[j].Timestamp) })
	sort.Slice(wantRecords, func(i, j int) bool { return wantRecords[i].Timestamp.Before(wantRecords[j].Timestamp) })
	for index := range wantRecords {
		if gotRecords[index] != wantRecords[index] {
			t.Fatalf("record %d = %+v, want %+v", index, gotRecords[index], wantRecords[index])
		}
	}
}

func assertNoCompactionTemporaryFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if strings.HasPrefix(entry.Name(), ".parquet-compact-") {
			t.Fatalf("temporary compaction file remains: %s", entry.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertNoPublishedCompactionFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if strings.HasPrefix(entry.Name(), "compact-") && strings.HasSuffix(entry.Name(), ".parquet") {
			t.Fatalf("published compaction file remains: %s", entry.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func pathForPartition(partition dataset.Partition, fileName string) string {
	return filepath.ToSlash(filepath.Join(
		"candles",
		"asset_class="+partition["asset_class"],
		"venue="+partition["venue"],
		"instrument_type="+partition["instrument_type"],
		"symbol="+partition["symbol"],
		"timeframe="+partition["timeframe"],
		"date="+partition["date"],
		fileName,
	))
}

func writeRawObject(t *testing.T, objectStore store.Store, key string, value []byte) {
	t.Helper()
	writer, err := objectStore.Create(context.Background(), key, store.CreateParams{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(value); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type compactionFaultStore struct {
	store.Store
	failPublish     bool
	failInputDelete bool
	blockTemporary  chan struct{}
	temporaryReady  chan struct{}
}

func (s *compactionFaultStore) Create(ctx context.Context, key string, params store.CreateParams) (store.ObjectWriter, error) {
	base := filepath.Base(key)
	if s.failPublish && strings.HasPrefix(base, "compact-") && strings.HasSuffix(base, ".parquet") {
		return nil, fs.ErrPermission
	}
	if s.blockTemporary != nil && strings.HasPrefix(base, ".parquet-compact-") {
		select {
		case <-s.temporaryReady:
		default:
			close(s.temporaryReady)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.blockTemporary:
		}
	}
	return s.Store.Create(ctx, key, params)
}

func (s *compactionFaultStore) Delete(ctx context.Context, key string) error {
	base := filepath.Base(key)
	if s.failInputDelete && strings.HasPrefix(base, "part-") {
		return fs.ErrPermission
	}
	deleter, ok := s.Store.(interface {
		Delete(context.Context, string) error
	})
	if !ok {
		return fs.ErrInvalid
	}
	return deleter.Delete(ctx, key)
}

func isEmptyCompactResult(result CandleCompactResult) bool {
	return !result.Compacted && len(result.InputFiles) == 0 && len(result.OutputFiles) == 0 &&
		result.NumRows == 0 && result.InputBytes == 0 && result.OutputBytes == 0
}
