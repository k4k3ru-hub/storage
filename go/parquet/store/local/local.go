//
// local.go
//
package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/k4k3ru-hub/storage/go/parquet/store"
)

type Params struct {
	Root string
}

type Store struct {
	root string
}

// New creates a local object store.
//
// Version:
//   - 2026-08-14: Added.
func New(params Params) (*Store, error) {
	if strings.TrimSpace(params.Root) == "" {
		return nil, fmt.Errorf("local store root is empty")
	}

	root, err := filepath.Abs(params.Root)
	if err != nil {
		return nil, fmt.Errorf("resolve local store root: %w", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create local store root: %w", err)
	}

	return &Store{root: root}, nil
}

// Open opens an object for random-access reading.
//
// Version:
//   - 2026-08-14: Added.
func (s *Store) Open(ctx context.Context, key string) (store.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	filename, err := s.resolve(key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(filename)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", store.ErrNotFound, key)
	}
	if err != nil {
		return nil, fmt.Errorf("open local object %q: %w", key, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat local object %q: %w", key, err)
	}

	return &object{File: file, size: info.Size()}, nil
}

// Create creates an atomic local object writer.
//
// Version:
//   - 2026-08-14: Added.
func (s *Store) Create(ctx context.Context, key string, params store.CreateParams) (store.ObjectWriter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	filename, err := s.resolve(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return nil, fmt.Errorf("create local object directory: %w", err)
	}

	temporary, err := os.CreateTemp(filepath.Dir(filename), ".parquet-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary local object: %w", err)
	}

	return &objectWriter{
		file:      temporary,
		finalPath: filename,
		overwrite: params.Overwrite,
	}, nil
}

// List lists objects whose keys have the given prefix.
//
// Version:
//   - 2026-08-14: Added.
func (s *Store) List(ctx context.Context, prefix string) (store.Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	prefixPath, err := s.resolvePrefix(prefix)
	if err != nil {
		return nil, err
	}

	objects := make([]store.ObjectInfo, 0)
	err = filepath.WalkDir(prefixPath, func(path string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return fs.SkipDir
		}
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".parquet-") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		key, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		objects = append(objects, store.ObjectInfo{
			Key:  filepath.ToSlash(key),
			Size: info.Size(),
		})
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("list local objects: %w", err)
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	return &iterator{objects: objects, index: -1}, nil
}

func (s *Store) resolve(key string) (string, error) {
	if key == "" || filepath.IsAbs(key) {
		return "", fmt.Errorf("%w: %q", store.ErrInvalidKey, key)
	}
	cleaned := filepath.Clean(filepath.FromSlash(key))
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q", store.ErrInvalidKey, key)
	}
	resolved := filepath.Join(s.root, cleaned)
	if resolved == s.root || !strings.HasPrefix(resolved, s.root+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q", store.ErrInvalidKey, key)
	}
	return resolved, nil
}

func (s *Store) resolvePrefix(prefix string) (string, error) {
	if prefix == "" {
		return s.root, nil
	}
	return s.resolve(prefix)
}

type object struct {
	*os.File
	size int64
}

func (o *object) Size() int64 {
	return o.size
}

type objectWriter struct {
	mu        sync.Mutex
	file      *os.File
	finalPath string
	overwrite bool
	written   int64
	finished  bool
}

func (w *objectWriter) Write(value []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return 0, os.ErrClosed
	}
	n, err := w.file.Write(value)
	w.written += int64(n)
	return n, err
}

func (w *objectWriter) BytesWritten() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

func (w *objectWriter) Commit(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync temporary local object: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close temporary local object: %w", err)
	}

	temporaryPath := w.file.Name()
	if w.overwrite {
		if err := os.Rename(temporaryPath, w.finalPath); err != nil {
			return fmt.Errorf("replace local object: %w", err)
		}
	} else {
		if err := os.Link(temporaryPath, w.finalPath); errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: %s", store.ErrAlreadyExists, w.finalPath)
		} else if err != nil {
			return fmt.Errorf("commit local object: %w", err)
		}
		if err := os.Remove(temporaryPath); err != nil {
			_ = os.Remove(w.finalPath)
			return fmt.Errorf("remove temporary local object: %w", err)
		}
	}
	w.finished = true
	return nil
}

func (w *objectWriter) Abort(_ context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return nil
	}
	w.finished = true
	closeErr := w.file.Close()
	if errors.Is(closeErr, os.ErrClosed) {
		closeErr = nil
	}
	removeErr := os.Remove(w.file.Name())
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}

type iterator struct {
	objects []store.ObjectInfo
	index   int
}

func (i *iterator) Next(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	i.index++
	return i.index < len(i.objects)
}

func (i *iterator) Object() store.ObjectInfo {
	if i.index < 0 || i.index >= len(i.objects) {
		return store.ObjectInfo{}
	}
	return i.objects[i.index]
}

func (*iterator) Err() error {
	return nil
}

func (*iterator) Close() error {
	return nil
}
