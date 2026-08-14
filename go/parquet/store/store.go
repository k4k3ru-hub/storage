//
// store.go
//
package store

import (
	"context"
	"errors"
	"io"
)

var (
	ErrAlreadyExists = errors.New("store: object already exists")
	ErrInvalidKey    = errors.New("store: invalid key")
	ErrNotFound      = errors.New("store: object not found")
)

type CreateParams struct {
	Overwrite bool
}

type Object interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer

	//
	// Size returns the object size in bytes.
	//
	Size() int64
}

type ObjectWriter interface {
	io.Writer

	//
	// Abort discards an uncommitted object.
	//
	Abort(ctx context.Context) error

	//
	// BytesWritten returns the number of bytes accepted by Write.
	//
	BytesWritten() int64

	//
	// Commit publishes the completed object.
	//
	Commit(ctx context.Context) error
}

type ObjectInfo struct {
	Key  string
	Size int64
}

type Iterator interface {
	//
	// Next advances the iterator.
	//
	Next(ctx context.Context) bool

	//
	// Object returns the current object information.
	//
	Object() ObjectInfo

	//
	// Err returns the iteration error.
	//
	Err() error

	//
	// Close releases iterator resources.
	//
	Close() error
}

type Store interface {
	//
	// Open opens an object for random-access reading.
	//
	Open(ctx context.Context, key string) (Object, error)

	//
	// Create creates an object writer.
	//
	Create(ctx context.Context, key string, params CreateParams) (ObjectWriter, error)

	//
	// List lists objects whose keys have the given prefix.
	//
	List(ctx context.Context, prefix string) (Iterator, error)
}
