//go:build mysqlintegration

package ammpool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestMySQLPruneCanonicalRanges deletes the globally oldest parents across both canonical states.
//
// Version:
//   - 2026-10-03: Added.
func TestMySQLPruneCanonicalRanges(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		mixed bool
	}{{"mixed-1", 1, true}, {"mixed-64", 64, true}, {"mixed-1000", 1000, true}, {"noncanonical-only", 64, false}} {
		t.Run(tc.name, func(t *testing.T) {
			limit := tc.limit
			s, db := senderMySQL(t)
			f := senderFixture()
			cutoff := f.Cursor.UpdatedAt.Add(time.Minute)
			b := Batch{Cursor: f.Cursor}
			for i := 0; i < 2*limit+3; i++ {
				p := f.Snapshots[0]
				p.Identity.PoolID = fmt.Sprintf("expired-%04d", i)
				p.Canonical = tc.mixed && i%2 == 0
				// Repeated creation times exercise the binary-ID tie break across states.
				p.CreatedAt = cutoff.Add(-time.Duration(i%7+1) * time.Second)
				b.Snapshots = append(b.Snapshots, p)
			}
			for _, canonical := range []bool{false, true} {
				p := f.Snapshots[0]
				p.Identity.PoolID = fmt.Sprintf("boundary-%t", canonical)
				p.Canonical, p.CreatedAt = canonical, cutoff
				b.Snapshots = append(b.Snapshots, p)
			}
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			// This unbounded query is an independent ordering oracle in the isolated fixture.
			want := expiredPruneIdentities(t, s, db, cutoff)
			for len(want) > 0 {
				got, err := s.PruneSnapshotBatch(t.Context(), cutoff, limit)
				n := min(limit, len(want))
				if err != nil || !slices.Equal(got, want[:n]) {
					t.Fatalf("oldest batch mismatch: got=%v want=%v err=%v", got, want[:n], err)
				}
				want = want[n:]
				if remaining := expiredPruneIdentities(t, s, db, cutoff); !slices.Equal(remaining, want) {
					t.Fatal("deleted an unreported parent or retained a reported parent")
				}
			}
			if got, err := s.PruneSnapshotBatch(t.Context(), cutoff, limit); err != nil || len(got) != 0 {
				t.Fatal("empty expired ranges did not terminate", got, err)
			}
			for _, p := range b.Snapshots[len(b.Snapshots)-2:] {
				// Get intentionally hides noncanonical parents, so check physical retention.
				id := p.Identity.ID()
				var count int
				if err := db.QueryRowContext(t.Context(), s.query(`SELECT COUNT(*) FROM
 onchain_amm_pool_new_pair_snapshots WHERE id=?`), id[:]).Scan(&count); err != nil || count != 1 {
					t.Fatal("exact cutoff boundary was deleted", count, err)
				}
			}
		})
	}
}

func expiredPruneIdentities(t *testing.T, s *Store, db *sql.DB, cutoff time.Time) []Identity {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), s.query(`SELECT chain_family,chain,network,venue,pool_id
 FROM onchain_amm_pool_new_pair_snapshots WHERE pool_created_at<? ORDER BY pool_created_at,id`), cutoff)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	var out []Identity
	for rows.Next() {
		var p Identity
		if err := rows.Scan(&p.ChainFamily, &p.Chain, &p.Network, &p.Venue, &p.PoolID); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestMySQLPruneSecondRangeCancellation rolls back the first range when the second range times out.
//
// Version:
//   - 2026-10-03: Added.
func TestMySQLPruneSecondRangeCancellation(t *testing.T) {
	s, db := senderMySQL(t)
	f := senderFixture()
	cutoff := f.Cursor.UpdatedAt.Add(time.Minute)
	b := Batch{Cursor: f.Cursor}
	for _, canonical := range []bool{false, true} {
		for _, expired := range []bool{true, false} {
			p := f.Snapshots[0]
			p.Identity.PoolID = fmt.Sprintf("pool-%t-%t", canonical, expired)
			p.Canonical = canonical
			if !expired {
				p.CreatedAt = cutoff
			}
			b.Snapshots = append(b.Snapshots, p)
		}
	}
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	want := expiredPruneIdentities(t, s, db, cutoff)
	lock := lockPruneParent(t, s, db, b.Snapshots[2].Identity)
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	ids, err := s.PruneSnapshotBatch(ctx, cutoff, 1)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "is_canonical=true") || len(ids) != 0 {
		t.Fatal("second range failure did not return an empty, inspectable deadline error", ids, err)
	}
	if got := expiredPruneIdentities(t, s, db, cutoff); !slices.Equal(got, want) {
		t.Fatal("first range was partially deleted")
	}
	// A canceled connection can remain in a server-side lock wait. Release the blocker
	// before checking rollback; the context deadline does not guarantee immediate lock release.
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	first := b.Snapshots[0].Identity.ID()
	writeCtx, cancelWrite := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWrite()
	if _, err := db.ExecContext(writeCtx, s.query(`UPDATE onchain_amm_pool_new_pair_snapshots
 SET updated_at=updated_at WHERE id=?`), first[:]); err != nil {
		t.Fatal("first range lock survived rollback", err)
	}
	if ids, err := s.PruneSnapshotBatch(t.Context(), cutoff, 64); err != nil || !slices.Equal(ids, want) {
		t.Fatal("retry did not delete both retained parents", ids, err)
	}
}

func lockPruneParent(t *testing.T, s *Store, db *sql.DB, pool Identity) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	id := pool.ID()
	if _, err := tx.ExecContext(t.Context(), s.query(`UPDATE onchain_amm_pool_new_pair_snapshots
 SET updated_at=updated_at WHERE id=?`), id[:]); err != nil {
		t.Fatal(err)
	}
	return tx
}

// TestMySQLPruneIndexedIsolation verifies range plans and pruning beside a locked unexpired parent.
//
// Version:
//   - 2026-10-03: Added.
func TestMySQLPruneIndexedIsolation(t *testing.T) {
	s, db := senderMySQL(t)
	f := senderFixture()
	cutoff := f.Cursor.UpdatedAt.Add(time.Minute)
	b := Batch{Cursor: f.Cursor}
	for i := 0; i < 10128; i++ {
		p := f.Snapshots[0]
		p.Identity.PoolID = fmt.Sprintf("pool-%05d", i)
		p.Canonical = i%2 == 0
		if i >= 128 {
			p.CreatedAt = cutoff.Add(time.Duration(i-128) * time.Second)
		}
		b.Snapshots = append(b.Snapshots, p)
	}
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	for _, canonical := range []bool{false, true} {
		assertPruneRangePlan(t, s, db, canonical, cutoff)
	}
	// Keep a recent parent write open while expired parents are removed.
	lockPruneParent(t, s, db, b.Snapshots[len(b.Snapshots)-1].Identity)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	started := time.Now()
	ids, err := s.PruneSnapshotBatch(ctx, cutoff, 64)
	if err != nil || len(ids) != 64 {
		t.Fatal("unexpired parent lock blocked bounded pruning", len(ids), err)
	}
	t.Logf("10,128 parents, 128 expired, 64 deleted: %s", time.Since(started))
	if remaining := expiredPruneIdentities(t, s, db, cutoff); len(remaining) != 64 {
		t.Fatal("wrong remaining expired parent count", len(remaining))
	}
}

func assertPruneRangePlan(t *testing.T, s *Store, db *sql.DB, canonical bool, cutoff time.Time) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "EXPLAIN "+s.query(pruneSnapshotCandidatesSQL), canonical, cutoff, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Error(err)
		}
	}()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		targets := make([]any, len(columns))
		for i := range targets {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		plan := map[string]string{}
		for i, name := range columns {
			plan[name] = values[i].String
		}
		if plan["type"] != "range" || plan["key"] != "idx_amm_pool_created" || strings.Contains(plan["Extra"], "filesort") {
			t.Fatal("parent candidates did not use the ordered retention range", plan)
		}
		t.Logf("canonical=%t: type=%s key=%s rows=%s extra=%s", canonical, plan["type"], plan["key"], plan["rows"], plan["Extra"])
		count++
	}
	if err := rows.Err(); err != nil || count != 1 {
		t.Fatal("missing candidate query plan", count, err)
	}
}

// TestMySQLPruneParentChildren keeps a retained Pool and its shared sender transaction intact.
//
// Version:
//   - 2026-10-03: Added.
func TestMySQLPruneParentChildren(t *testing.T) {
	s, db := senderMySQL(t)
	b := senderFixture()
	cutoff := b.Cursor.UpdatedAt
	b.Snapshots[0].CreatedAt = cutoff.Add(-24 * time.Hour)
	f := senderFixture()
	pool := f.Snapshots[0].Identity
	pool.PoolID = "retained"
	f.Snapshots[0].Identity = pool
	f.Events[0].Pool = pool
	f.ActivityMinutes[0].Pool = pool
	f.SenderSnapshots[0].Pool = pool
	f.SenderEvents[0].Pool = pool
	b.Snapshots = append(b.Snapshots, f.Snapshots...)
	b.Events = append(b.Events, f.Events...)
	b.ActivityMinutes = append(b.ActivityMinutes, f.ActivityMinutes...)
	b.SenderSnapshots = append(b.SenderSnapshots, f.SenderSnapshots...)
	b.SenderEvents = append(b.SenderEvents, f.SenderEvents...)
	b.AdmitSenderEvents = append(b.AdmitSenderEvents, f.Events[0].ID())
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	ids, err := s.PruneSnapshotBatch(t.Context(), cutoff, 64)
	if err != nil || !slices.Equal(ids, []Identity{b.Snapshots[0].Identity}) {
		t.Fatal("wrong parent deleted", ids, err)
	}
	for _, suffix := range []string{"events", "activity_minutes", "sender_snapshots", "sender_events"} {
		for i, p := range b.Snapshots {
			var count int
			id := p.Identity.ID()
			query := s.query("SELECT COUNT(*) FROM onchain_amm_pool_new_pair_" + suffix + " WHERE pool_id=?")
			if err := db.QueryRowContext(t.Context(), query, id[:]).Scan(&count); err != nil || count != i {
				t.Fatal("parent cascade affected the wrong children", suffix, p.Identity.PoolID, count, err)
			}
		}
	}
	pools, transactions, events := readSenderState(t, s)
	if len(pools) != 1 || len(events) != 1 || len(transactions) != 1 || pools[0].Pool != pool || events[0].Transaction != transactions[0].Key {
		t.Fatal("retained Pool lost its shared sender transaction")
	}
}
