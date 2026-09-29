//go:build mysqlintegration

package ammpool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestMySQLPruneBatchLimits keeps each deletion bounded and separates history from parent ownership.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLPruneBatchLimits(t *testing.T) {
	s, _ := senderMySQL(t)
	f := senderFixture()
	cutoff := f.Cursor.UpdatedAt.Add(time.Minute)
	b := Batch{Cursor: f.Cursor}
	for i := 0; i < 3; i++ {
		p, e := f.Snapshots[0], f.Events[0]
		p.Identity.PoolID = fmt.Sprintf("pool-%d", i)
		if i == 2 {
			p.CreatedAt = cutoff
			e.ObservedAt = cutoff
		}
		e.Pool = p.Identity
		b.Snapshots = append(b.Snapshots, p)
		b.Events = append(b.Events, e)
	}
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if count, err := s.PruneEventBatch(t.Context(), cutoff, 1); err != nil || count != 1 {
		t.Fatal("history batch exceeded its limit", count, err)
	}
	for _, p := range b.Snapshots {
		if _, err := s.Get(t.Context(), p.Identity); err != nil {
			t.Fatal("history pruning deleted a parent", err)
		}
	}
	first, err := s.PruneSnapshotBatch(t.Context(), cutoff, 1)
	if err != nil || len(first) != 1 {
		t.Fatal("parent batch exceeded its limit", err)
	}
	if _, err := s.Get(t.Context(), first[0]); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("returned identity was not committed", err)
	}
	second, err := s.PruneSnapshotBatch(t.Context(), cutoff, 1)
	if err != nil || len(second) != 1 || second[0] == first[0] {
		t.Fatal("remaining parent did not survive the first batch", err)
	}
	if ids, err := s.PruneSnapshotBatch(t.Context(), cutoff, 1); err != nil || len(ids) != 0 {
		t.Fatal("cutoff boundary was deleted", ids, err)
	}
	if count, err := s.PruneEventBatch(t.Context(), cutoff, 1); err != nil || count != 0 {
		t.Fatal("cutoff boundary history was deleted", count, err)
	}
}

// TestMySQLPruneDeletedSnapshots reports committed batches even if a later batch fails.
//
// Version:
//   - 2026-09-29: Added.
func TestMySQLPruneDeletedSnapshots(t *testing.T) {
	s, db := senderMySQL(t)
	f := senderFixture()
	cutoff := f.Cursor.UpdatedAt.Add(time.Minute)
	b := Batch{Cursor: f.Cursor}
	for i := 0; i < 1002; i++ {
		p := f.Snapshots[0]
		p.Identity.PoolID = fmt.Sprintf("pool-%d", i)
		if i == 1001 {
			p.CreatedAt = cutoff // the exact boundary must survive
		}
		b.Snapshots = append(b.Snapshots, p)
	}
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	seen := map[Identity]bool{}
	report := func(ids []Identity) {
		if len(ids) > 1000 {
			t.Fatal("unbounded deletion report")
		}
		for _, id := range ids {
			if seen[id] || id == b.Snapshots[1001].Identity {
				t.Fatal("duplicate or retained parent reported as deleted")
			}
			seen[id] = true
		}
	}
	err := s.PruneWithDeletedSnapshots(ctx, cutoff, cutoff, func(ids []Identity) {
		report(ids)
		var count int
		if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM market_hub_onchain_amm_pool_new_pair_snapshots").Scan(&count); err != nil || count != 2 {
			t.Fatal("callback ran before deletion commit", count, err)
		}
		cancel()
	})
	if !errors.Is(err, context.Canceled) || len(seen) != 1000 {
		t.Fatal("partial commit reporting lost", len(seen), err)
	}
	if err := s.PruneWithDeletedSnapshots(t.Context(), cutoff, cutoff, report); err != nil || len(seen) != 1001 {
		t.Fatal("retry did not report only remaining expired parent", err)
	}
	if _, err := s.Get(t.Context(), b.Snapshots[1001].Identity); err != nil {
		t.Fatal("retention boundary deleted", err)
	}
	if err := s.Prune(t.Context(), cutoff, cutoff.Add(time.Second)); err != nil {
		t.Fatal("legacy prune failed", err)
	}
}
