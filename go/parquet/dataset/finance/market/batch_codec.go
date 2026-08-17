// batch_codec.go
package market

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
)

type batchReader[T, R any] struct {
	reader  *parquetgo.GenericReader[R]
	convert func(R) T
	rows    []R
	info    dataset.CompactionFileInfo
}

func newBatchReader[T, R any](
	ctx context.Context,
	source dataset.ReadSource,
	size int64,
	convert func(R) T,
) (dataset.BatchReader[T], error) {
	if ctx == nil {
		return nil, fmt.Errorf("failed to create parquet batch reader: context=null")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("failed to create parquet batch reader: %w", err)
	}
	if source == nil {
		return nil, fmt.Errorf("failed to create parquet batch reader: source=null")
	}
	if size <= 0 {
		return nil, fmt.Errorf("failed to create parquet batch reader: size=out_of_range")
	}

	file, err := parquetgo.OpenFile(source, size)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet batch reader: %w", err)
	}
	return &batchReader[T, R]{
		reader:  parquetgo.NewGenericReader[R](file),
		convert: convert,
		info: dataset.CompactionFileInfo{
			NumRows:                file.NumRows(),
			SchemaFingerprint:      fingerprint(file.Schema().String()),
			CompressionFingerprint: compressionFingerprint(file),
		},
	}, nil
}

func (r *batchReader[T, R]) Read(ctx context.Context, records []T) (int, error) {
	if ctx == nil {
		return 0, fmt.Errorf("failed to read parquet batch: context=null")
	}
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("failed to read parquet batch: %w", err)
	}
	if len(records) == 0 {
		return 0, fmt.Errorf("failed to read parquet batch: records=empty")
	}
	if cap(r.rows) < len(records) {
		r.rows = make([]R, len(records))
	} else {
		r.rows = r.rows[:len(records)]
	}

	n, err := r.reader.Read(r.rows)
	for index := 0; index < n; index++ {
		records[index] = r.convert(r.rows[index])
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("failed to read parquet batch: %w", err)
	}
	return n, err
}

func (r *batchReader[T, R]) FileInfo() dataset.CompactionFileInfo {
	return r.info
}

func (r *batchReader[T, R]) Close() error {
	if err := r.reader.Close(); err != nil {
		return fmt.Errorf("failed to close parquet batch reader: %w", err)
	}
	return nil
}

type batchWriter[T, R any] struct {
	writer  *parquetgo.GenericWriter[R]
	convert func(T) R
	rows    []R
}

func newBatchWriter[T, R any](
	ctx context.Context,
	destination io.Writer,
	convert func(T) R,
) (dataset.BatchWriter[T], error) {
	if ctx == nil {
		return nil, fmt.Errorf("failed to create parquet batch writer: context=null")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("failed to create parquet batch writer: %w", err)
	}
	if destination == nil {
		return nil, fmt.Errorf("failed to create parquet batch writer: destination=null")
	}

	return &batchWriter[T, R]{
		writer: parquetgo.NewGenericWriter[R](
			destination,
			parquetgo.Compression(&parquetgo.Zstd),
		),
		convert: convert,
	}, nil
}

func (w *batchWriter[T, R]) Write(ctx context.Context, records []T) error {
	if ctx == nil {
		return fmt.Errorf("failed to write parquet batch: context=null")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failed to write parquet batch: %w", err)
	}
	if len(records) == 0 {
		return nil
	}
	if cap(w.rows) < len(records) {
		w.rows = make([]R, len(records))
	} else {
		w.rows = w.rows[:len(records)]
	}
	for index, record := range records {
		w.rows[index] = w.convert(record)
	}
	written, err := w.writer.Write(w.rows)
	if err != nil {
		return fmt.Errorf("failed to write parquet batch: %w", err)
	}
	if written != len(w.rows) {
		return fmt.Errorf("failed to write parquet batch: %w", io.ErrShortWrite)
	}
	return nil
}

func (w *batchWriter[T, R]) Close() error {
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet batch writer: %w", err)
	}
	return nil
}

func fingerprint(value string) string {
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", digest)
}

func compressionFingerprint(file *parquetgo.File) string {
	values := make(map[string]struct{})
	for _, rowGroup := range file.Metadata().RowGroups {
		for _, column := range rowGroup.Columns {
			value := strings.Join(column.MetaData.PathInSchema, ".") + "=" + column.MetaData.Codec.String()
			values[value] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(values))
	for value := range values {
		ordered = append(ordered, value)
	}
	sort.Strings(ordered)
	return fingerprint(strings.Join(ordered, "\n"))
}
