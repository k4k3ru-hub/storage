package dataset

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type bufferedDatasetStub struct {
	mu     sync.Mutex
	writes []WriteParams[int]
	err    error
}

func (s *bufferedDatasetStub) Write(_ context.Context, params WriteParams[int]) (WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return WriteResult{}, s.err
	}
	params.Partition = clonePartition(params.Partition)
	params.Records = append([]int(nil), params.Records...)
	s.writes = append(s.writes, params)
	return WriteResult{NumRows: int64(len(params.Records))}, nil
}

func (s *bufferedDatasetStub) snapshot() []WriteParams[int] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]WriteParams[int](nil), s.writes...)
}

func newTestBufferedWriter(t *testing.T, dataset *bufferedDatasetStub, options BufferedWriterOptions) *BufferedWriter[int] {
	t.Helper()
	writer, err := NewBufferedWriter[int](dataset, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return writer
}

func TestBufferedWriterFlushesPartitionAtRowLimit(t *testing.T) {
	dataset := &bufferedDatasetStub{}
	writer := newTestBufferedWriter(t, dataset, BufferedWriterOptions{MaxRowsPerPartition: 3, MaxBufferedRows: 10, FlushInterval: time.Hour})
	partition := Partition{"venue": "binance", "symbol": "BTC-USDT"}
	if err := writer.Append(context.Background(), partition, 1, 2); err != nil {
		t.Fatal(err)
	}
	if got := len(dataset.snapshot()); got != 0 {
		t.Fatalf("writes before row limit = %d", got)
	}
	if err := writer.Append(context.Background(), partition, 3); err != nil {
		t.Fatal(err)
	}
	writes := dataset.snapshot()
	if len(writes) != 1 || len(writes[0].Records) != 3 {
		t.Fatalf("writes = %+v", writes)
	}
}

func TestBufferedWriterSeparatesPartitionsAndFlushesAll(t *testing.T) {
	dataset := &bufferedDatasetStub{}
	writer := newTestBufferedWriter(t, dataset, BufferedWriterOptions{MaxRowsPerPartition: 10, MaxBufferedRows: 20, FlushInterval: time.Hour})
	if err := writer.Append(context.Background(), Partition{"venue": "binance"}, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(context.Background(), Partition{"venue": "okx"}, 3); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	writes := dataset.snapshot()
	if len(writes) != 2 {
		t.Fatalf("writes = %+v", writes)
	}
}

func TestBufferedWriterFlushesExpiredPartition(t *testing.T) {
	dataset := &bufferedDatasetStub{}
	writer := newTestBufferedWriter(t, dataset, BufferedWriterOptions{MaxRowsPerPartition: 10, MaxBufferedRows: 20, FlushInterval: 10 * time.Millisecond})
	if err := writer.Append(context.Background(), Partition{"venue": "coinbase"}, 1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(dataset.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(dataset.snapshot()); got != 1 {
		t.Fatalf("writes after interval = %d", got)
	}
}

func TestBufferedWriterRestoresRecordsAfterWriteFailure(t *testing.T) {
	dataset := &bufferedDatasetStub{err: errors.New("write failed")}
	writer := newTestBufferedWriter(t, dataset, BufferedWriterOptions{MaxRowsPerPartition: 2, MaxBufferedRows: 10, FlushInterval: time.Hour})
	if err := writer.Append(context.Background(), Partition{"venue": "bybit"}, 1, 2); err == nil {
		t.Fatal("Append() error = nil")
	}
	dataset.mu.Lock()
	dataset.err = nil
	dataset.mu.Unlock()
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	writes := dataset.snapshot()
	if len(writes) != 1 || len(writes[0].Records) != 2 {
		t.Fatalf("restored writes = %+v", writes)
	}
}

func TestBufferedWriterReportsPeriodicFlushFailure(t *testing.T) {
	dataset := &bufferedDatasetStub{err: errors.New("write failed")}
	flushErrors := make(chan error, 1)
	writer := newTestBufferedWriter(t, dataset, BufferedWriterOptions{
		MaxRowsPerPartition: 10,
		MaxBufferedRows:     20,
		FlushInterval:       10 * time.Millisecond,
		OnFlushError: func(err error) {
			flushErrors <- err
		},
	})
	if err := writer.Append(context.Background(), Partition{"venue": "coinbase"}, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-flushErrors:
		if err == nil {
			t.Fatal("periodic flush error = nil")
		}
	case <-time.After(time.Second):
		t.Fatal("periodic flush error was not reported")
	}
	dataset.mu.Lock()
	dataset.err = nil
	dataset.mu.Unlock()
}

func TestBufferedWriterRejectsGlobalBufferOverflow(t *testing.T) {
	dataset := &bufferedDatasetStub{}
	writer := newTestBufferedWriter(t, dataset, BufferedWriterOptions{MaxRowsPerPartition: 3, MaxBufferedRows: 3, FlushInterval: time.Hour})
	if err := writer.Append(context.Background(), Partition{"venue": "binance"}, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(context.Background(), Partition{"venue": "okx"}, 3, 4); err == nil {
		t.Fatal("Append() error = nil")
	}
}

func clonePartition(value Partition) Partition {
	result := make(Partition, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}
