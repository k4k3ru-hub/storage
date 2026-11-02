package market

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	pq "github.com/parquet-go/parquet-go"
)

type validatedObservationWriter[T any] struct {
	inner    dataset.BatchWriter[T]
	validate func(T) error
}

// Write validates each observation before forwarding the batch.
//
// Version:
//   - 2026-11-02: Added.
func (w *validatedObservationWriter[T]) Write(ctx context.Context, values []T) error {
	for _, value := range values {
		if err := w.validate(value); err != nil {
			return fmt.Errorf("failed to write observation batch: %w", err)
		}
	}
	return w.inner.Write(ctx, values)
}

// Close completes the validated observation stream.
//
// Version:
//   - 2026-11-02: Added.
func (w *validatedObservationWriter[T]) Close() error { return w.inner.Close() }

func readObservationRecords[T any](ctx context.Context, r dataset.BatchReader[T]) (out []T, err error) {
	defer func() { err = errors.Join(err, r.Close()) }()
	buffer := make([]T, 1024)
	for {
		n, e := r.Read(ctx, buffer)
		out = append(out, buffer[:n]...)
		if errors.Is(e, io.EOF) {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
	}
}

func validateObservationSchema(ctx context.Context, source dataset.ReadSource, size int64, current, legacy *pq.Schema, funding bool) error {
	if ctx == nil || source == nil {
		return fmt.Errorf("failed to read observation: dependency=null")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failed to read observation: %w", err)
	}
	file, err := pq.OpenFile(source, size)
	if err != nil {
		return fmt.Errorf("failed to read observation: %w", err)
	}
	normalize := func(s *pq.Schema) string {
		group := pq.Group{}
		for _, f := range s.Fields() {
			var n pq.Node = f
			if funding && f.Name() == "funding_timestamp" && f.Required() {
				n = pq.Optional(f)
			}
			group[f.Name()] = n
		}
		return pq.NewSchema("observation", group).String()
	}
	actual := normalize(file.Schema())
	if actual != normalize(current) && actual != normalize(legacy) {
		return fmt.Errorf("failed to read observation: schema=invalid")
	}
	return nil
}

func observationContextKey(c ObservationContext) string {
	return fmt.Sprintf("%s/%s/%s", optionalStringKey(c.Network), optionalStringKey(c.SourceID), optionalStringKey(c.CollectionRunID))
}
func compareObservationContext(a, b ObservationContext) int {
	return strings.Compare(observationContextKey(a), observationContextKey(b))
}
