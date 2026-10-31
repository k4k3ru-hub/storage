package news

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

type errorConnector struct{ err error }

// Connect returns the injected connection error.
//
// Version:
//   - 2026-10-31: Added.
func (c errorConnector) Connect(context.Context) (driver.Conn, error) { return nil, c.err }

// Driver returns the matching failure driver.
//
// Version:
//   - 2026-10-31: Added.
func (c errorConnector) Driver() driver.Driver { return errorDriver{c.err} }

type errorDriver struct{ err error }

// Open returns the injected connection error.
//
// Version:
//   - 2026-10-31: Added.
func (d errorDriver) Open(string) (driver.Conn, error) { return nil, d.err }
func newErrorDB(t *testing.T, err error) *sql.DB {
	t.Helper()
	db := sql.OpenDB(errorConnector{err})
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

// TestReadExecutorErrorsAndNilDependencies verifies error chains and nil dependencies.
//
// Version:
//   - 2026-10-31: Added.
func TestReadExecutorErrorsAndNilDependencies(t *testing.T) {
	s, err := NewStore("")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	underlying := errors.New("injected transport failure")
	db := newErrorDB(t, underlying)
	_, err = s.GetFetch(ctx, db, testID(1))
	if !errors.Is(err, underlying) {
		t.Fatal(err)
	}
	_, err = s.GetRevision(ctx, db, testHash(1), testHash(2))
	if !errors.Is(err, underlying) {
		t.Fatal(err)
	}
	_, err = s.GetAnnotation(ctx, db, testID(1))
	if !errors.Is(err, underlying) {
		t.Fatal(err)
	}
	ready, err := s.LatestReady(ctx, db, []string{"source"}, "profile")
	if !errors.Is(err, underlying) || ready != nil {
		t.Fatal(err)
	}
	page, err := s.ListPinnedObservations(ctx, db, ListRequest{Inputs: []PinnedInput{{testID(1), testID(2), testHash(1), testHash(2)}}, ProfileKey: "profile", Limit: 1})
	if !errors.Is(err, underlying) || len(page.Observations) != 0 || page.Scope != (Hash{}) {
		t.Fatal(err)
	}
	var nilDB *sql.DB
	if _, err := s.GetFetch(ctx, nilDB, testID(1)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.GetAnnotation(nil, db, testID(1)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	var tx *sql.Tx
	for _, call := range []func() error{func() error { return s.RegisterPublisher(ctx, tx, Publisher{}) }, func() error { return s.RegisterSource(ctx, tx, Source{}, 0) }, func() error { return s.RegisterCategory(ctx, tx, Category{}) }, func() error { return s.BeginFetch(ctx, tx, FetchStart{}) }, func() error { return s.CompleteFetch(ctx, tx, FetchCompletion{}) }, func() error { return s.FailFetch(ctx, tx, Failure{}) }, func() error { return s.BeginAnnotation(ctx, tx, AnnotationStart{}) }, func() error { return s.CompleteAnnotation(ctx, tx, AnnotationCompletion{}) }, func() error { return s.FailAnnotation(ctx, tx, Failure{}) }} {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}
