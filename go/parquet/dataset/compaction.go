// compaction.go
package dataset

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strings"

	"github.com/k4k3ru-hub/storage/go/parquet/store"
)

const compactionBatchSize = 1024

type CompactParams struct {
	Partition           Partition
	TargetFileSizeBytes int64
}

type CompactResult struct {
	Compacted        bool
	InputFiles       []string
	OutputFiles      []string
	InputRows        int64
	OutputRows       int64
	DeduplicatedRows int64
	NumRows          int64
	InputBytes       int64
	OutputBytes      int64
}

type objectDeleter interface {
	Delete(ctx context.Context, key string) error
}

type compactionInput struct {
	key  string
	size int64
	info CompactionFileInfo
}

type compactionOutput struct {
	temporaryKey string
	finalKey     string
	numRows      int64
	numBytes     int64
}

// Compact compacts immutable Parquet parts in one dataset partition.
//
// Parameters:
//   - ctx: Context for the operation.
//   - params: Partition and target output size.
//
// Returns:
//   - Compaction result. Compacted is false when fewer than two parts exist.
//
// The configured store must also implement Delete(ctx, key). Publishing multiple
// output objects is not atomic; published outputs are rolled back on ordinary
// publication or validation errors. If input deletion has started and then
// fails, outputs are retained to avoid data loss.
//
// Version:
//   - 2026-08-18: Added.
func (d *Dataset[T]) Compact(ctx context.Context, params CompactParams) (result CompactResult, err error) {
	operationErr := "failed to compact dataset"
	if d == nil {
		return result, fmt.Errorf("%s: dataset=null", operationErr)
	}
	if ctx == nil {
		return result, fmt.Errorf("%s: context=null", operationErr)
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("%s: %w", operationErr, err)
	}
	if d.store == nil {
		return result, fmt.Errorf("%s: store=null", operationErr)
	}
	if params.TargetFileSizeBytes <= 0 {
		return result, fmt.Errorf("%s: target_file_size_bytes=out_of_range", operationErr)
	}
	prefix, err := d.partitionPrefix(params.Partition)
	if err != nil {
		return result, fmt.Errorf("%s: %w", operationErr, err)
	}
	if !d.beginCompaction(prefix) {
		return result, fmt.Errorf("%s: partition compaction already in progress: partition=%q", operationErr, prefix)
	}
	defer d.endCompaction(prefix)

	objects, err := d.compactionObjects(ctx, prefix)
	if err != nil {
		return result, fmt.Errorf("%s: %w", operationErr, err)
	}
	if len(objects) < 2 {
		return CompactResult{}, nil
	}

	codec, ok := d.codec.(CompactionCodec[T])
	if !ok {
		return result, fmt.Errorf("%s: codec does not support compaction", operationErr)
	}
	deleter, ok := d.store.(objectDeleter)
	if !ok {
		return result, fmt.Errorf("%s: store does not support object deletion", operationErr)
	}

	inputs, baseline, inputRows, inputBytes, err := d.inspectCompactionInputs(ctx, codec, objects)
	if err != nil {
		return result, fmt.Errorf("%s: %w", operationErr, err)
	}
	compactionID, err := newCompactionID()
	if err != nil {
		return result, fmt.Errorf("%s: %w", operationErr, err)
	}
	targetRows := estimatedTargetRows(params.TargetFileSizeBytes, inputRows, inputBytes)
	outputs, expectedOutputRows, err := d.writeCompactionOutputs(ctx, codec, inputs, prefix, compactionID, targetRows)
	if err != nil {
		cleanupErr := deleteAllCompactionObjects(context.WithoutCancel(ctx), deleter, temporaryKeys(outputs))
		return result, errors.Join(fmt.Errorf("%s: %w", operationErr, err), cleanupErr)
	}
	temporaryCleaned := false
	defer func() {
		if temporaryCleaned {
			return
		}
		cleanupErr := deleteAllCompactionObjects(context.WithoutCancel(ctx), deleter, temporaryKeys(outputs))
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to clean up compaction temporary outputs: %w", cleanupErr))
		}
	}()

	outputRows, outputBytes, err := d.validateCompactionObjects(ctx, codec, outputs, true, baseline)
	if err != nil {
		return result, fmt.Errorf("%s: %w", operationErr, err)
	}
	deduplicatedRows := inputRows - outputRows
	if outputRows != expectedOutputRows || deduplicatedRows < 0 || outputRows+deduplicatedRows != inputRows {
		return result, fmt.Errorf(
			"%s: output row count mismatch: input_rows=%d expected_output_rows=%d output_rows=%d deduplicated_rows=%d",
			operationErr,
			inputRows,
			expectedOutputRows,
			outputRows,
			deduplicatedRows,
		)
	}

	published, err := d.publishCompactionOutputs(ctx, outputs)
	if err != nil {
		rollbackErr := deleteAllCompactionObjects(context.WithoutCancel(ctx), deleter, published)
		return result, errors.Join(fmt.Errorf("%s: %w", operationErr, err), rollbackErr)
	}
	publishedRows, publishedBytes, err := d.validateCompactionObjects(ctx, codec, outputs, false, baseline)
	if err != nil || publishedRows != outputRows {
		rollbackErr := deleteAllCompactionObjects(context.WithoutCancel(ctx), deleter, published)
		if err != nil {
			return result, errors.Join(fmt.Errorf("%s: %w", operationErr, err), rollbackErr)
		}
		return result, errors.Join(
			fmt.Errorf("%s: published row count mismatch: expected_output_rows=%d output_rows=%d", operationErr, outputRows, publishedRows),
			rollbackErr,
		)
	}
	if publishedBytes != outputBytes {
		rollbackErr := deleteAllCompactionObjects(context.WithoutCancel(ctx), deleter, published)
		return result, errors.Join(
			fmt.Errorf("%s: published byte count mismatch: temporary_bytes=%d published_bytes=%d", operationErr, outputBytes, publishedBytes),
			rollbackErr,
		)
	}
	if err := deleteAllCompactionObjects(context.WithoutCancel(ctx), deleter, temporaryKeys(outputs)); err != nil {
		rollbackErr := deleteAllCompactionObjects(context.WithoutCancel(ctx), deleter, published)
		return result, errors.Join(fmt.Errorf("%s: failed to clean up temporary outputs: %w", operationErr, err), rollbackErr)
	}
	temporaryCleaned = true

	if err := deleteCompactionObjects(ctx, deleter, inputKeys(inputs)); err != nil {
		return result, fmt.Errorf("%s: failed to delete input objects: %w", operationErr, err)
	}

	result = CompactResult{
		Compacted:        true,
		InputFiles:       inputKeys(inputs),
		OutputFiles:      finalKeys(outputs),
		InputRows:        inputRows,
		OutputRows:       outputRows,
		DeduplicatedRows: deduplicatedRows,
		NumRows:          outputRows,
		InputBytes:       inputBytes,
		OutputBytes:      publishedBytes,
	}
	return result, nil
}

func (d *Dataset[T]) beginCompaction(prefix string) bool {
	d.compactionMu.Lock()
	defer d.compactionMu.Unlock()
	if d.compacting == nil {
		d.compacting = make(map[string]struct{})
	}
	if _, ok := d.compacting[prefix]; ok {
		return false
	}
	d.compacting[prefix] = struct{}{}
	return true
}

func (d *Dataset[T]) endCompaction(prefix string) {
	d.compactionMu.Lock()
	defer d.compactionMu.Unlock()
	delete(d.compacting, prefix)
}

func (d *Dataset[T]) compactionObjects(ctx context.Context, prefix string) ([]store.ObjectInfo, error) {
	iterator, err := d.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list compaction inputs: %w: partition=%q", err, prefix)
	}
	defer iterator.Close()

	objects := make([]store.ObjectInfo, 0)
	for iterator.Next(ctx) {
		object := iterator.Object()
		if path.Dir(object.Key) != prefix || !strings.HasSuffix(strings.ToLower(object.Key), ".parquet") {
			continue
		}
		objects = append(objects, object)
	}
	if err := errors.Join(iterator.Err(), ctx.Err()); err != nil {
		return nil, fmt.Errorf("failed to iterate compaction inputs: %w: partition=%q", err, prefix)
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

func (d *Dataset[T]) inspectCompactionInputs(
	ctx context.Context,
	codec CompactionCodec[T],
	objects []store.ObjectInfo,
) ([]compactionInput, CompactionFileInfo, int64, int64, error) {
	inputs := make([]compactionInput, 0, len(objects))
	var baseline CompactionFileInfo
	var totalRows int64
	var totalBytes int64
	for index, objectInfo := range objects {
		object, err := d.store.Open(ctx, objectInfo.Key)
		if err != nil {
			return nil, baseline, 0, 0, fmt.Errorf("failed to open compaction input: %w: key=%q", err, objectInfo.Key)
		}
		reader, readerErr := codec.NewBatchReader(ctx, object, object.Size())
		if readerErr != nil {
			_ = object.Close()
			return nil, baseline, 0, 0, fmt.Errorf("failed to inspect compaction input: %w: key=%q", readerErr, objectInfo.Key)
		}
		info := reader.FileInfo()
		closeErr := errors.Join(reader.Close(), object.Close())
		if closeErr != nil {
			return nil, baseline, 0, 0, fmt.Errorf("failed to close compaction input: %w: key=%q", closeErr, objectInfo.Key)
		}
		if index == 0 {
			baseline = info
		} else if info.SchemaFingerprint != baseline.SchemaFingerprint || info.CompressionFingerprint != baseline.CompressionFingerprint {
			return nil, baseline, 0, 0, fmt.Errorf("failed to validate compaction input: incompatible parquet metadata: key=%q", objectInfo.Key)
		}
		inputs = append(inputs, compactionInput{key: objectInfo.Key, size: objectInfo.Size, info: info})
		totalRows += info.NumRows
		totalBytes += objectInfo.Size
	}
	return inputs, baseline, totalRows, totalBytes, nil
}

func (d *Dataset[T]) writeCompactionOutputs(
	ctx context.Context,
	codec CompactionCodec[T],
	inputs []compactionInput,
	prefix string,
	compactionID string,
	targetRows int64,
) (outputs []compactionOutput, outputRows int64, err error) {
	var normalizedRecords []T
	if d.compactionPolicy != nil {
		normalizedRecords, err = d.readAndNormalizeCompactionRecords(ctx, codec, inputs)
		if err != nil {
			return nil, 0, err
		}
	}
	var destination store.ObjectWriter
	var writer BatchWriter[T]
	var current compactionOutput
	sequence := 0

	abortCurrent := func() error {
		if destination == nil {
			return nil
		}
		return destination.Abort(context.WithoutCancel(ctx))
	}
	closeCurrent := func() error {
		if writer == nil {
			return nil
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close compaction output: %w: key=%q", err, current.temporaryKey)
		}
		if err := destination.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit compaction temporary output: %w: key=%q", err, current.temporaryKey)
		}
		current.numBytes = destination.BytesWritten()
		outputs = append(outputs, current)
		destination = nil
		writer = nil
		current = compactionOutput{}
		return nil
	}
	openCurrent := func() error {
		sequence++
		base := fmt.Sprintf("compact-%s-%06d", compactionID, sequence)
		current = compactionOutput{
			temporaryKey: path.Join(prefix, ".parquet-"+base+".tmp"),
			finalKey:     path.Join(prefix, base+".parquet"),
		}
		value, createErr := d.store.Create(ctx, current.temporaryKey, store.CreateParams{})
		if createErr != nil {
			return fmt.Errorf("failed to create compaction temporary output: %w: key=%q", createErr, current.temporaryKey)
		}
		destination = value
		valueWriter, writerErr := codec.NewBatchWriter(ctx, destination)
		if writerErr != nil {
			return fmt.Errorf("failed to create compaction batch writer: %w: key=%q", writerErr, current.temporaryKey)
		}
		writer = valueWriter
		return nil
	}

	defer func() {
		if abortErr := abortCurrent(); abortErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to abort compaction output: %w", abortErr))
		}
	}()
	writeRecords := func(records []T) error {
		position := 0
		for position < len(records) {
			if writer == nil {
				if err := openCurrent(); err != nil {
					return err
				}
			}
			remaining := targetRows - current.numRows
			count := int64(len(records) - position)
			if count > remaining {
				count = remaining
			}
			if err := writer.Write(ctx, records[position:position+int(count)]); err != nil {
				return fmt.Errorf("failed to encode compaction output: %w: key=%q", err, current.temporaryKey)
			}
			position += int(count)
			current.numRows += count
			outputRows += count
			if current.numRows >= targetRows {
				if err := closeCurrent(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if d.compactionPolicy != nil {
		if err := writeRecords(normalizedRecords); err != nil {
			return outputs, outputRows, err
		}
		if err := closeCurrent(); err != nil {
			return outputs, outputRows, err
		}
		return outputs, outputRows, nil
	}

	buffer := make([]T, compactionBatchSize)
	for _, input := range inputs {
		object, openErr := d.store.Open(ctx, input.key)
		if openErr != nil {
			return outputs, outputRows, fmt.Errorf("failed to open compaction input: %w: key=%q", openErr, input.key)
		}
		reader, readerErr := codec.NewBatchReader(ctx, object, object.Size())
		if readerErr != nil {
			_ = object.Close()
			return outputs, outputRows, fmt.Errorf("failed to create compaction batch reader: %w: key=%q", readerErr, input.key)
		}
		for {
			n, readErr := reader.Read(ctx, buffer)
			if err := writeRecords(buffer[:n]); err != nil {
				_ = reader.Close()
				_ = object.Close()
				return outputs, outputRows, err
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = reader.Close()
				_ = object.Close()
				return outputs, outputRows, fmt.Errorf("failed to decode compaction input: %w: key=%q", readErr, input.key)
			}
		}
		if closeErr := errors.Join(reader.Close(), object.Close()); closeErr != nil {
			return outputs, outputRows, fmt.Errorf("failed to close compaction input: %w: key=%q", closeErr, input.key)
		}
	}
	if err := closeCurrent(); err != nil {
		return outputs, outputRows, err
	}
	return outputs, outputRows, nil
}

func (d *Dataset[T]) readAndNormalizeCompactionRecords(
	ctx context.Context,
	codec CompactionCodec[T],
	inputs []compactionInput,
) ([]T, error) {
	records := make([]T, 0)
	buffer := make([]T, compactionBatchSize)
	for _, input := range inputs {
		object, err := d.store.Open(ctx, input.key)
		if err != nil {
			return nil, fmt.Errorf("failed to open compaction input: %w: key=%q", err, input.key)
		}
		reader, err := codec.NewBatchReader(ctx, object, object.Size())
		if err != nil {
			_ = object.Close()
			return nil, fmt.Errorf("failed to create compaction batch reader: %w: key=%q", err, input.key)
		}
		for {
			n, readErr := reader.Read(ctx, buffer)
			records = append(records, buffer[:n]...)
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = reader.Close()
				_ = object.Close()
				return nil, fmt.Errorf("failed to decode compaction input: %w: key=%q", readErr, input.key)
			}
		}
		if closeErr := errors.Join(reader.Close(), object.Close()); closeErr != nil {
			return nil, fmt.Errorf("failed to close compaction input: %w: key=%q", closeErr, input.key)
		}
	}
	normalized := make([]T, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		key, deduplicate := d.compactionPolicy.DeduplicationKey(record)
		if deduplicate {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}
		normalized = append(normalized, record)
	}
	sort.SliceStable(normalized, func(left, right int) bool {
		return d.compactionPolicy.Compare(normalized[left], normalized[right]) < 0
	})
	return normalized, nil
}

func (d *Dataset[T]) validateCompactionObjects(
	ctx context.Context,
	codec CompactionCodec[T],
	outputs []compactionOutput,
	temporary bool,
	baseline CompactionFileInfo,
) (int64, int64, error) {
	buffer := make([]T, compactionBatchSize)
	var totalRows int64
	var totalBytes int64
	for _, output := range outputs {
		key := output.finalKey
		if temporary {
			key = output.temporaryKey
		}
		object, err := d.store.Open(ctx, key)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to open compaction output: %w: key=%q", err, key)
		}
		reader, readerErr := codec.NewBatchReader(ctx, object, object.Size())
		if readerErr != nil {
			_ = object.Close()
			return 0, 0, fmt.Errorf("failed to validate compaction output: %w: key=%q", readerErr, key)
		}
		info := reader.FileInfo()
		if info.SchemaFingerprint != baseline.SchemaFingerprint || info.CompressionFingerprint != baseline.CompressionFingerprint {
			_ = reader.Close()
			_ = object.Close()
			return 0, 0, fmt.Errorf("failed to validate compaction output: incompatible parquet metadata: key=%q", key)
		}
		var readRows int64
		for {
			n, readErr := reader.Read(ctx, buffer)
			readRows += int64(n)
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = reader.Close()
				_ = object.Close()
				return 0, 0, fmt.Errorf("failed to read compaction output: %w: key=%q", readErr, key)
			}
		}
		closeErr := errors.Join(reader.Close(), object.Close())
		if closeErr != nil {
			return 0, 0, fmt.Errorf("failed to close compaction output: %w: key=%q", closeErr, key)
		}
		if readRows != info.NumRows || readRows != output.numRows {
			return 0, 0, fmt.Errorf(
				"failed to validate compaction output: row count mismatch: key=%q metadata_rows=%d read_rows=%d expected_rows=%d",
				key,
				info.NumRows,
				readRows,
				output.numRows,
			)
		}
		totalRows += readRows
		totalBytes += object.Size()
	}
	return totalRows, totalBytes, nil
}

func (d *Dataset[T]) publishCompactionOutputs(ctx context.Context, outputs []compactionOutput) ([]string, error) {
	published := make([]string, 0, len(outputs))
	buffer := make([]byte, 64*1024)
	for _, output := range outputs {
		source, err := d.store.Open(ctx, output.temporaryKey)
		if err != nil {
			return published, fmt.Errorf("failed to open compaction temporary output: %w: key=%q", err, output.temporaryKey)
		}
		destination, createErr := d.store.Create(ctx, output.finalKey, store.CreateParams{})
		if createErr != nil {
			_ = source.Close()
			return published, fmt.Errorf("failed to create compaction output: %w: key=%q", createErr, output.finalKey)
		}
		copyErr := copyObject(ctx, destination, source, buffer)
		closeErr := source.Close()
		if copyErr != nil {
			_ = destination.Abort(context.WithoutCancel(ctx))
			return published, errors.Join(fmt.Errorf("failed to copy compaction output: %w: key=%q", copyErr, output.finalKey), closeErr)
		}
		if closeErr != nil {
			_ = destination.Abort(context.WithoutCancel(ctx))
			return published, fmt.Errorf("failed to close compaction temporary output: %w: key=%q", closeErr, output.temporaryKey)
		}
		if err := destination.Commit(ctx); err != nil {
			_ = destination.Abort(context.WithoutCancel(ctx))
			return published, fmt.Errorf("failed to publish compaction output: %w: key=%q", err, output.finalKey)
		}
		published = append(published, output.finalKey)
	}
	return published, nil
}

func copyObject(ctx context.Context, destination io.Writer, source io.Reader, buffer []byte) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := source.Read(buffer)
		if n > 0 {
			written, err := destination.Write(buffer[:n])
			if err != nil {
				return err
			}
			if written != n {
				return io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func deleteCompactionObjects(ctx context.Context, deleter objectDeleter, keys []string) error {
	for _, key := range keys {
		if err := deleter.Delete(ctx, key); err != nil {
			return fmt.Errorf("failed to delete object: %w: key=%q", err, key)
		}
	}
	return nil
}

func deleteAllCompactionObjects(ctx context.Context, deleter objectDeleter, keys []string) error {
	var result error
	for _, key := range keys {
		if err := deleter.Delete(ctx, key); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to delete object: %w: key=%q", err, key))
		}
	}
	return result
}

func estimatedTargetRows(targetBytes, totalRows, totalBytes int64) int64 {
	if totalRows <= 0 || totalBytes <= 0 {
		return 1
	}
	estimate := float64(targetBytes) * float64(totalRows) / float64(totalBytes)
	if estimate >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	targetRows := int64(estimate)
	if targetRows < 1 {
		return 1
	}
	return targetRows
}

func newCompactionID() (string, error) {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("failed to generate compaction id: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func temporaryKeys(outputs []compactionOutput) []string {
	keys := make([]string, len(outputs))
	for index, output := range outputs {
		keys[index] = output.temporaryKey
	}
	return keys
}

func finalKeys(outputs []compactionOutput) []string {
	keys := make([]string, len(outputs))
	for index, output := range outputs {
		keys[index] = output.finalKey
	}
	return keys
}

func inputKeys(inputs []compactionInput) []string {
	keys := make([]string, len(inputs))
	for index, input := range inputs {
		keys[index] = input.key
	}
	return keys
}
