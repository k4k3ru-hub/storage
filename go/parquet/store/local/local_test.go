// local_test.go
package local

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
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

func TestStoreCreateAppliesPublishedFileMode(t *testing.T) {
	root := t.TempDir()
	value, err := New(Params{Root: root, FileMode: 0o640})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	writer, err := value.Create(ctx, "data.parquet", store.CreateParams{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "data.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o640); got != want {
		t.Fatalf("object file mode = %#o, want %#o", got, want)
	}
}

func TestStoreCreateAppliesPublishedFileModeWhenOverwriting(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "data.parquet")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := New(Params{Root: root, FileMode: 0o640})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	writer, err := value.Create(ctx, "data.parquet", store.CreateParams{Overwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o640); got != want {
		t.Fatalf("object file mode = %#o, want %#o", got, want)
	}
}

func TestNewRejectsInvalidPublishedFileMode(t *testing.T) {
	for _, mode := range []os.FileMode{0o641, 0o620, os.ModeSetuid | 0o640} {
		if _, err := New(Params{Root: t.TempDir(), FileMode: mode}); err == nil {
			t.Fatalf("New() accepted invalid file mode %#o", mode)
		}
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

func TestStoreDelete(t *testing.T) {
	value, err := New(Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	writer, err := value.Create(ctx, "candles/date=2026-08-17/data.parquet", store.CreateParams{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	if err := value.Delete(ctx, "candles/date=2026-08-17/data.parquet"); err != nil {
		t.Fatal(err)
	}
	if _, err := value.Open(ctx, "candles/date=2026-08-17/data.parquet"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("open deleted object error = %v, want ErrNotFound", err)
	}
	if err := value.Delete(ctx, "candles/date=2026-08-17/data.parquet"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("delete missing object error = %v, want ErrNotFound", err)
	}
}

func TestStoreDeleteRejectsInvalidKey(t *testing.T) {
	value, err := New(Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := value.Delete(context.Background(), "../outside"); !errors.Is(err, store.ErrInvalidKey) {
		t.Fatalf("delete error = %v, want ErrInvalidKey", err)
	}
}

func TestStoreDeleteHonorsCanceledContext(t *testing.T) {
	value, err := New(Params{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := value.Delete(ctx, "data.parquet"); !errors.Is(err, context.Canceled) {
		t.Fatalf("delete error = %v, want context.Canceled", err)
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
