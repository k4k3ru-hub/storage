package news_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/k4k3ru-hub/storage/go/mysql/news"
)

// ExampleNewStore demonstrates construction without database I/O.
//
// Version:
//   - 2026-10-31: Added.
func ExampleNewStore() {
	store, err := news.NewStore("market_hub_")
	if err != nil {
		fmt.Println(err)
		return
	}
	ddl, err := store.Schema()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(strings.Count(ddl, "CREATE TABLE `market_hub_news_"))
	// Output: 10
}

// persistFetchedNews demonstrates two short caller-owned transactions for fixture input.
// Actual acquisition belongs between the committed BeginFetch and CompleteFetch calls.
func persistFetchedNews(ctx context.Context, db *sql.DB, store *news.Store,
	start news.FetchStart, completion news.FetchCompletion) error {
	apply := func(write func(*sql.Tx) error) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin news transaction: %w", err)
		}
		if err := write(tx); err != nil {
			return fmt.Errorf("failed to persist news transaction: %w", errors.Join(err, tx.Rollback()))
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit news transaction: %w", err)
		}
		return nil
	}
	if err := apply(func(tx *sql.Tx) error { return store.BeginFetch(ctx, tx, start) }); err != nil {
		return err
	}
	return apply(func(tx *sql.Tx) error { return store.CompleteFetch(ctx, tx, completion) })
}
