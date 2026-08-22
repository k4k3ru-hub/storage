// dataset.go
package dataset

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/store"
)

type ReadSource interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type Codec[T any] interface {
	//
	// Encode encodes records into a Parquet byte stream.
	//
	Encode(ctx context.Context, destination io.Writer, records []T) error

	//
	// Decode decodes records from a Parquet byte stream.
	//
	Decode(ctx context.Context, source ReadSource, size int64) ([]T, error)
}

type Partition map[string]string

type WriteMode uint8

const (
	WriteModeCreate WriteMode = iota
	WriteModeOverwrite
)

type Params struct {
	Root             string
	PartitionColumns []string
	FileName         string
	WriteMode        WriteMode
}

// CompactionPolicy defines record ordering and equality for compaction.
//
// Version:
//   - 2026-08-22: Added.
type CompactionPolicy[T any] interface {
	Compare(left, right T) int
	DeduplicationKey(record T) (string, bool)
}

type WriteParams[T any] struct {
	Partition Partition
	Records   []T
}

type WriteResult struct {
	Key      string
	NumRows  int64
	NumBytes int64
}

type ReadParams struct {
	Partition Partition
}

type ReadResult[T any] struct {
	Records []T
	Files   []string
}

type Dataset[T any] struct {
	store            store.Store
	codec            Codec[T]
	root             string
	partitionColumns []string
	fileName         string
	writeMode        WriteMode
	compactionPolicy CompactionPolicy[T]
	compactionMu     sync.Mutex
	compacting       map[string]struct{}
}

// NewWithCompactionPolicy creates a typed Parquet dataset with ordered,
// deduplicating compaction.
//
// Parameters:
//   - c: Parquet client.
//   - codec: Dataset codec.
//   - params: Dataset parameters.
//   - policy: Compaction ordering and equality policy.
//
// Returns:
//   - Typed dataset.
//   - Creation error.
//
// Version:
//   - 2026-08-22: Added.
func NewWithCompactionPolicy[T any](c *client.Client, codec Codec[T], params Params, policy CompactionPolicy[T]) (*Dataset[T], error) {
	value, err := New(c, codec, params)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, fmt.Errorf("dataset compaction policy is nil")
	}
	value.compactionPolicy = policy
	return value, nil
}

// New creates a typed Parquet dataset.
//
// Version:
//   - 2026-08-14: Added.
func New[T any](c *client.Client, codec Codec[T], params Params) (*Dataset[T], error) {
	if c == nil {
		return nil, fmt.Errorf("dataset client is nil")
	}
	if codec == nil {
		return nil, fmt.Errorf("dataset codec is nil")
	}
	root, err := cleanRoot(params.Root)
	if err != nil {
		return nil, err
	}
	columns, err := validatePartitionColumns(params.PartitionColumns)
	if err != nil {
		return nil, err
	}
	fileName, err := cleanFileName(params.FileName)
	if err != nil {
		return nil, err
	}
	if params.WriteMode != WriteModeCreate && params.WriteMode != WriteModeOverwrite {
		return nil, fmt.Errorf("invalid dataset write mode: %d", params.WriteMode)
	}

	return &Dataset[T]{
		store:            c.Store(),
		codec:            codec,
		root:             root,
		partitionColumns: columns,
		fileName:         fileName,
		writeMode:        params.WriteMode,
		compacting:       make(map[string]struct{}),
	}, nil
}

// Write writes one immutable Parquet part to the dataset.
//
// Version:
//   - 2026-08-14: Added.
func (d *Dataset[T]) Write(ctx context.Context, params WriteParams[T]) (result WriteResult, err error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(params.Records) == 0 {
		return result, fmt.Errorf("dataset records are empty")
	}
	prefix, err := d.partitionPrefix(params.Partition)
	if err != nil {
		return result, err
	}
	fileName := d.fileName
	if fileName == "" {
		fileName, err = randomFileName()
		if err != nil {
			return result, err
		}
	}
	key := path.Join(prefix, fileName)

	destination, err := d.store.Create(ctx, key, store.CreateParams{
		Overwrite: d.writeMode == WriteModeOverwrite,
	})
	if err != nil {
		return result, fmt.Errorf("create dataset object %q: %w", key, err)
	}
	committed := false
	defer func() {
		if !committed {
			err = errors.Join(err, destination.Abort(context.WithoutCancel(ctx)))
		}
	}()

	if err := d.codec.Encode(ctx, destination, params.Records); err != nil {
		return result, fmt.Errorf("encode dataset object %q: %w", key, err)
	}
	if err := destination.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit dataset object %q: %w", key, err)
	}
	committed = true

	return WriteResult{
		Key:      key,
		NumRows:  int64(len(params.Records)),
		NumBytes: destination.BytesWritten(),
	}, nil
}

// Read reads all Parquet parts matching a dataset partition.
//
// Version:
//   - 2026-08-14: Added.
func (d *Dataset[T]) Read(ctx context.Context, params ReadParams) (ReadResult[T], error) {
	prefix, err := d.partitionPrefix(params.Partition)
	if err != nil {
		return ReadResult[T]{}, err
	}
	keys, err := d.objectKeys(ctx, prefix)
	if err != nil {
		return ReadResult[T]{}, err
	}
	if len(keys) == 0 {
		return ReadResult[T]{}, fmt.Errorf("%w: %s", store.ErrNotFound, prefix)
	}

	result := ReadResult[T]{Files: keys}
	for _, key := range keys {
		object, err := d.store.Open(ctx, key)
		if err != nil {
			return ReadResult[T]{}, fmt.Errorf("open dataset object %q: %w", key, err)
		}
		records, decodeErr := d.codec.Decode(ctx, object, object.Size())
		closeErr := object.Close()
		if decodeErr != nil {
			return ReadResult[T]{}, errors.Join(fmt.Errorf("decode dataset object %q: %w", key, decodeErr), closeErr)
		}
		if closeErr != nil {
			return ReadResult[T]{}, fmt.Errorf("close dataset object %q: %w", key, closeErr)
		}
		result.Records = append(result.Records, records...)
	}
	return result, nil
}

func (d *Dataset[T]) objectKeys(ctx context.Context, prefix string) ([]string, error) {
	if d.fileName != "" {
		return []string{path.Join(prefix, d.fileName)}, nil
	}
	iterator, err := d.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list dataset prefix %q: %w", prefix, err)
	}
	defer iterator.Close()

	keys := make([]string, 0)
	for iterator.Next(ctx) {
		object := iterator.Object()
		if strings.HasSuffix(strings.ToLower(object.Key), ".parquet") {
			keys = append(keys, object.Key)
		}
	}
	if err := errors.Join(iterator.Err(), ctx.Err()); err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func (d *Dataset[T]) partitionPrefix(partition Partition) (string, error) {
	if len(partition) != len(d.partitionColumns) {
		return "", fmt.Errorf("dataset partition has %d values, want %d", len(partition), len(d.partitionColumns))
	}
	parts := make([]string, 0, len(d.partitionColumns)+1)
	parts = append(parts, d.root)
	for _, column := range d.partitionColumns {
		value, ok := partition[column]
		if !ok || value == "" {
			return "", fmt.Errorf("dataset partition %q is missing", column)
		}
		if strings.ContainsAny(value, "/\\") || value == "." || value == ".." {
			return "", fmt.Errorf("dataset partition %q has invalid value %q", column, value)
		}
		parts = append(parts, column+"="+value)
	}
	return path.Join(parts...), nil
}

func cleanRoot(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("invalid dataset root %q", value)
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("invalid dataset root %q", value)
	}
	return cleaned, nil
}

func cleanFileName(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if value == "." || value == ".." || strings.ContainsAny(value, "/\\") {
		return "", fmt.Errorf("invalid dataset file name %q", value)
	}
	if !strings.HasSuffix(strings.ToLower(value), ".parquet") {
		value += ".parquet"
	}
	return value, nil
}

func validatePartitionColumns(values []string) ([]string, error) {
	columns := append([]string(nil), values...)
	seen := make(map[string]struct{}, len(columns))
	for index, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" || strings.ContainsAny(column, "=/\\") {
			return nil, fmt.Errorf("invalid dataset partition column %q", column)
		}
		if _, ok := seen[column]; ok {
			return nil, fmt.Errorf("duplicate dataset partition column %q", column)
		}
		seen[column] = struct{}{}
		columns[index] = column
	}
	return columns, nil
}

func randomFileName() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate dataset file name: %w", err)
	}
	return "part-" + hex.EncodeToString(value) + ".parquet", nil
}
