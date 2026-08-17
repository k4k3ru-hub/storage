// compaction_codec.go
package dataset

import (
	"context"
	"io"
)

type CompactionFileInfo struct {
	NumRows                int64
	SchemaFingerprint      string
	CompressionFingerprint string
}

type BatchReader[T any] interface {
	// Read reads the next batch of records into records.
	//
	// Parameters:
	//   - ctx: Context for the operation.
	//   - records: Destination buffer.
	//
	// Returns:
	//   - Number of records read.
	//   - Read error, including io.EOF when no more records remain.
	//
	// Version:
	//   - 2026-08-17: Added.
	Read(ctx context.Context, records []T) (int, error)

	// FileInfo returns immutable metadata for the opened Parquet file.
	//
	// Returns:
	//   - Parquet file metadata used for compaction validation.
	//
	// Version:
	//   - 2026-08-17: Added.
	FileInfo() CompactionFileInfo

	// Close releases reader resources.
	//
	// Version:
	//   - 2026-08-17: Added.
	Close() error
}

type BatchWriter[T any] interface {
	// Write writes one batch of records.
	//
	// Parameters:
	//   - ctx: Context for the operation.
	//   - records: Records to write.
	//
	// Version:
	//   - 2026-08-17: Added.
	Write(ctx context.Context, records []T) error

	// Close completes the Parquet stream and releases writer resources.
	//
	// Version:
	//   - 2026-08-17: Added.
	Close() error
}

type CompactionCodec[T any] interface {
	Codec[T]

	// NewBatchReader opens a bounded Parquet record reader.
	//
	// Parameters:
	//   - ctx: Context for the operation.
	//   - source: Parquet source.
	//   - size: Source size in bytes.
	//
	// Returns:
	//   - Batch reader.
	//
	// Version:
	//   - 2026-08-17: Added.
	NewBatchReader(ctx context.Context, source ReadSource, size int64) (BatchReader[T], error)

	// NewBatchWriter creates a bounded Parquet record writer.
	//
	// Parameters:
	//   - ctx: Context for the operation.
	//   - destination: Parquet destination.
	//
	// Returns:
	//   - Batch writer.
	//
	// Version:
	//   - 2026-08-17: Added.
	NewBatchWriter(ctx context.Context, destination io.Writer) (BatchWriter[T], error)
}
