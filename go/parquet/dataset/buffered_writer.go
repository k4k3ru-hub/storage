package dataset

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// DatasetWriter writes records to immutable dataset parts.
type DatasetWriter[T any] interface {
	Write(context.Context, WriteParams[T]) (WriteResult, error)
}

// BufferedWriterOptions configures a partition-aware buffered writer.
type BufferedWriterOptions struct {
	MaxRowsPerPartition int
	MaxBufferedRows     int
	FlushInterval       time.Duration
	OnFlushError        func(error)
}

type bufferedPartition[T any] struct {
	partition  Partition
	records    []T
	bufferedAt time.Time
}

// BufferedWriter buffers records by partition before writing immutable parts.
type BufferedWriter[T any] struct {
	dataset DatasetWriter[T]
	options BufferedWriterOptions
	now     func() time.Time

	mu           sync.Mutex
	buffers      map[string]*bufferedPartition[T]
	bufferedRows int
	started      bool
	closed       bool
	cancel       context.CancelFunc
	done         chan struct{}
	flushMu      sync.Mutex
}

// NewBufferedWriter creates a partition-aware buffered dataset writer.
//
// Parameters:
//   - value: Dataset writer.
//   - options: Buffer and flush limits.
//
// Returns:
//   - Buffered writer.
//   - Creation error.
//
// Version:
//   - 2026-08-25: Added.
func NewBufferedWriter[T any](value DatasetWriter[T], options BufferedWriterOptions) (*BufferedWriter[T], error) {
	if value == nil {
		return nil, fmt.Errorf("failed to create buffered dataset writer: dataset=null")
	}
	if options.MaxRowsPerPartition <= 0 {
		return nil, fmt.Errorf("failed to create buffered dataset writer: max_rows_per_partition=out_of_range min_value=1")
	}
	if options.MaxBufferedRows < options.MaxRowsPerPartition {
		return nil, fmt.Errorf("failed to create buffered dataset writer: max_buffered_rows=out_of_range min_value=%d", options.MaxRowsPerPartition)
	}
	if options.FlushInterval <= 0 {
		return nil, fmt.Errorf("failed to create buffered dataset writer: flush_interval=out_of_range")
	}
	return &BufferedWriter[T]{
		dataset: value,
		options: options,
		now:     time.Now,
		buffers: make(map[string]*bufferedPartition[T]),
		done:    make(chan struct{}),
	}, nil
}

// Start starts periodic buffered-record flushing.
//
// Parameters:
//   - ctx: Writer lifecycle context.
//
// Returns:
//   - Startup error.
//
// Version:
//   - 2026-08-25: Added.
func (w *BufferedWriter[T]) Start(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("failed to start buffered dataset writer: writer=null")
	}
	if ctx == nil {
		return fmt.Errorf("failed to start buffered dataset writer: context=null")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failed to start buffered dataset writer: %w", err)
	}
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return fmt.Errorf("failed to start buffered dataset writer: writer=invalid")
	}
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("failed to start buffered dataset writer: writer=invalid")
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.started = true
	w.mu.Unlock()
	go w.run(runCtx)
	return nil
}

// Append buffers records for a dataset partition.
//
// Parameters:
//   - ctx: Append context.
//   - partition: Dataset partition.
//   - records: Records to buffer.
//
// Returns:
//   - Append or flush error.
//
// Version:
//   - 2026-08-25: Added.
func (w *BufferedWriter[T]) Append(ctx context.Context, partition Partition, records ...T) error {
	if w == nil {
		return fmt.Errorf("failed to append buffered dataset records: writer=null")
	}
	if ctx == nil {
		return fmt.Errorf("failed to append buffered dataset records: context=null")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failed to append buffered dataset records: %w", err)
	}
	if len(records) == 0 {
		return fmt.Errorf("failed to append buffered dataset records: records=empty")
	}
	key, copiedPartition, err := bufferedPartitionKey(partition)
	if err != nil {
		return fmt.Errorf("failed to append buffered dataset records: %w", err)
	}

	w.mu.Lock()
	if !w.started || w.closed {
		w.mu.Unlock()
		return fmt.Errorf("failed to append buffered dataset records: writer=invalid")
	}
	if len(records) > w.options.MaxBufferedRows-w.bufferedRows {
		w.mu.Unlock()
		return fmt.Errorf("failed to append buffered dataset records: buffer=full buffered_rows=%d max_buffered_rows=%d", w.bufferedRows, w.options.MaxBufferedRows)
	}
	buffer := w.buffers[key]
	if buffer == nil {
		buffer = &bufferedPartition[T]{partition: copiedPartition, bufferedAt: w.now().UTC()}
		w.buffers[key] = buffer
	}
	buffer.records = append(buffer.records, records...)
	w.bufferedRows += len(records)
	ready := len(buffer.records) >= w.options.MaxRowsPerPartition
	w.mu.Unlock()

	if ready {
		if err := w.flushKeys(ctx, []string{key}); err != nil {
			return fmt.Errorf("failed to append buffered dataset records: %w", err)
		}
	}
	return nil
}

// Flush writes every currently buffered partition.
//
// Parameters:
//   - ctx: Flush context.
//
// Returns:
//   - Flush error.
//
// Version:
//   - 2026-08-25: Added.
func (w *BufferedWriter[T]) Flush(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("failed to flush buffered dataset writer: writer=null")
	}
	if ctx == nil {
		return fmt.Errorf("failed to flush buffered dataset writer: context=null")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failed to flush buffered dataset writer: %w", err)
	}
	w.mu.Lock()
	keys := make([]string, 0, len(w.buffers))
	for key := range w.buffers {
		keys = append(keys, key)
	}
	w.mu.Unlock()
	if err := w.flushKeys(ctx, keys); err != nil {
		return fmt.Errorf("failed to flush buffered dataset writer: %w", err)
	}
	return nil
}

// Close stops periodic flushing and flushes remaining records.
//
// Parameters:
//   - ctx: Final flush context.
//
// Returns:
//   - Close or flush error.
//
// Version:
//   - 2026-08-25: Added.
func (w *BufferedWriter[T]) Close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	cancel := w.cancel
	started := w.started
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if started {
		<-w.done
	}
	if err := w.Flush(ctx); err != nil {
		return fmt.Errorf("failed to close buffered dataset writer: %w", err)
	}
	return nil
}

func (w *BufferedWriter[T]) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.options.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := w.flushExpired(context.WithoutCancel(ctx), now.UTC()); err != nil && w.options.OnFlushError != nil {
				w.options.OnFlushError(err)
			}
		}
	}
}

func (w *BufferedWriter[T]) flushExpired(ctx context.Context, now time.Time) error {
	w.mu.Lock()
	keys := make([]string, 0)
	for key, buffer := range w.buffers {
		if !buffer.bufferedAt.IsZero() && now.Sub(buffer.bufferedAt) >= w.options.FlushInterval {
			keys = append(keys, key)
		}
	}
	w.mu.Unlock()
	return w.flushKeys(ctx, keys)
}

func (w *BufferedWriter[T]) flushKeys(ctx context.Context, keys []string) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		w.mu.Lock()
		buffer := w.buffers[key]
		if buffer == nil || len(buffer.records) == 0 {
			w.mu.Unlock()
			continue
		}
		delete(w.buffers, key)
		w.bufferedRows -= len(buffer.records)
		w.mu.Unlock()

		if _, err := w.dataset.Write(ctx, WriteParams[T]{Partition: buffer.partition, Records: buffer.records}); err != nil {
			w.mu.Lock()
			current := w.buffers[key]
			if current == nil {
				w.buffers[key] = buffer
			} else {
				current.records = append(buffer.records, current.records...)
				if current.bufferedAt.After(buffer.bufferedAt) {
					current.bufferedAt = buffer.bufferedAt
				}
			}
			w.bufferedRows += len(buffer.records)
			w.mu.Unlock()
			return fmt.Errorf("failed to flush buffered dataset partition: %w", err)
		}
	}
	return nil
}

func bufferedPartitionKey(partition Partition) (string, Partition, error) {
	if len(partition) == 0 {
		return "", nil, fmt.Errorf("failed to create buffered dataset partition key: partition=empty")
	}
	keys := make([]string, 0, len(partition))
	for key, value := range partition {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return "", nil, fmt.Errorf("failed to create buffered dataset partition key: partition=invalid")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	copied := make(Partition, len(partition))
	var builder strings.Builder
	for _, key := range keys {
		value := partition[key]
		copied[key] = value
		fmt.Fprintf(&builder, "%d:%s=%d:%s;", len(key), key, len(value), value)
	}
	return builder.String(), copied, nil
}
