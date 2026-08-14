//
// local_test.go
//
package local

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/k4k3ru-hub/storage/go/parquet/store"
)

func TestStoreCreateOpenAndList(t *testing.T) {
	value, err := New(Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	writer, err := value.Create(ctx, "candles/date=2026-08-14/data.parquet", store.CreateParams{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("PAR1dataPAR1")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	object, err := value.Open(ctx, "candles/date=2026-08-14/data.parquet")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(object)
	if err != nil {
		t.Fatal(err)
	}
	if err := object.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "PAR1dataPAR1"; got != want {
		t.Fatalf("object data = %q, want %q", got, want)
	}

	iterator, err := value.List(ctx, "candles")
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	if !iterator.Next(ctx) {
		t.Fatalf("iterator did not return object: %v", iterator.Err())
	}
	if got, want := iterator.Object().Key, "candles/date=2026-08-14/data.parquet"; got != want {
		t.Fatalf("object key = %q, want %q", got, want)
	}
	if iterator.Next(ctx) {
		t.Fatal("iterator returned an unexpected second object")
	}
}

func TestStoreCreateDoesNotOverwriteByDefault(t *testing.T) {
	value, err := New(Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for index := 0; index < 2; index++ {
		writer, err := value.Create(ctx, "data.parquet", store.CreateParams{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}
		err = writer.Commit(ctx)
		if index == 0 && err != nil {
			t.Fatal(err)
		}
		if index == 1 && !errors.Is(err, store.ErrAlreadyExists) {
			t.Fatalf("commit error = %v, want ErrAlreadyExists", err)
		}
		_ = writer.Abort(ctx)
	}
}

func TestStoreRejectsParentTraversal(t *testing.T) {
	value, err := New(Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := value.Open(context.Background(), "../outside"); !errors.Is(err, store.ErrInvalidKey) {
		t.Fatalf("open error = %v, want ErrInvalidKey", err)
	}
}
