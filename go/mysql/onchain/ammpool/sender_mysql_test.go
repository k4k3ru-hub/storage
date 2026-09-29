//go:build mysqlintegration

package ammpool

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func senderMySQL(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("AMMPOOL_TEST_DSN")
	if dsn == "" {
		t.Skip("AMMPOOL_TEST_DSN is not configured")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	cfg.DBName = ""
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal("invalid test connector")
	}
	admin := sql.OpenDB(connector)
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Error(err)
		}
	})
	name := fmt.Sprintf("sender_test_%x", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"`"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP DATABASE `"+name+"`"); err != nil {
			t.Error(err)
		}
	})
	cfg.DBName = name
	connector, err = mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal("invalid test connector")
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s, err := NewStoreWithTablePrefix(db, "market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTables(ctx); err != nil {
		t.Fatal("non-idempotent schema", err)
	}
	return s, db
}

func readSenderState(t *testing.T, s *Store) ([]SenderSnapshot, []SenderTransaction, []SenderEvent) {
	t.Helper()
	var pools []SenderSnapshot
	var transactions []SenderTransaction
	var events []SenderEvent
	err := s.WalkSenderState(t.Context(), SenderRestoreLimits{MaxSenderPools, MaxSenderTransactions, MaxSenderEvents, MaxSenderPoolEvents}, SenderStateConsumer{
		Pool:        func(_ Snapshot, p SenderSnapshot) error { pools = append(pools, p); return nil },
		Transaction: func(v SenderTransaction) error { transactions = append(transactions, v); return nil },
		Event:       func(v SenderEvent) error { events = append(events, v); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return pools, transactions, events
}

// TestMySQLSenderAtomicity verifies admission, rollback, cancellation and generation replacement.
//
// Version:
//   - 2026-09-28: Added.
//   - 2026-09-29: Use explicit admission and update-only cancellation batches.
func TestMySQLSenderAtomicity(t *testing.T) {
	s, db := senderMySQL(t)
	ctx := t.Context()
	b := senderFixture()
	now := b.Cursor.UpdatedAt
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, b); !errors.Is(err, ErrCursorConflict) {
		t.Fatal("cursor replay accepted", err)
	}
	pools, transactions, events := readSenderState(t, s)
	if len(pools) != 1 || len(transactions) != 1 || len(events) != 1 || events[0].ID() != b.Events[0].ID() {
		t.Fatal("restore mismatch")
	}
	// Wrong generation reaches the FK after snapshot, minute and transaction writes.
	fail := senderFixture()
	fail.Cursor.Revision = 1
	fail.Snapshots[0].State = json.RawMessage(`{"v":2}`)
	fail.ActivityMinutes[0].Totals = json.RawMessage(`{"swapCount":"2"}`)
	fail.SenderEvents[0].Generation = 2
	fail.Events[0].Index = "1"
	fail.SenderEvents[0].Index = "1"
	fail.SenderTransactions[0].Key.TransactionID = "new-tx"
	fail.Events[0].TransactionID = "new-tx"
	fail.SenderEvents[0].Transaction.TransactionID = "new-tx"
	fail.AdmitSenderEvents = [][32]byte{fail.SenderEvents[0].ID()}
	if err := s.Commit(ctx, fail); err == nil {
		t.Fatal("wrong generation accepted")
	}
	parent, err := s.Get(ctx, b.Snapshots[0].Identity)
	if err != nil || parent.Revision != 1 {
		t.Fatal("parent not rolled back", err)
	}
	minutes, err := s.ActivityMinutes(ctx, parent.Identity, now, now.Add(time.Minute))
	if err != nil || len(minutes) != 1 {
		t.Fatal(err)
	}
	var totals map[string]string
	if err := json.Unmarshal(minutes[0].Totals, &totals); err != nil || totals["swapCount"] != "1" {
		t.Fatal("minute not rolled back", err)
	}
	cursor, err := s.Cursor(ctx, b.Cursor.Source)
	if err != nil || cursor.Revision != 1 {
		t.Fatal("cursor advanced after rollback", err)
	}
	_, transactions, _ = readSenderState(t, s)
	if len(transactions) != 1 {
		t.Fatal("transaction not rolled back")
	}
	// Independent prefixes must also have noncolliding anonymous CHECK/FK names.
	other, err := NewStoreWithTablePrefix(db, "second_")
	if err != nil {
		t.Fatal(err)
	}
	if err := other.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}
	// Identity-changing corrections cannot silently mutate existing evidence.
	bad := senderFixture()
	bad.Cursor.Revision = 1
	bad.SenderEvents[0].Direction = SenderToken1ToToken0
	if err := s.Commit(ctx, bad); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("immutable direction changed", err)
	}
	// Cancel both the Activity occurrence and sender contribution in one commit.
	b.Cursor.Revision = 1
	b.Events[0].Canonical = false
	b.SenderEvents[0].Canonical = false
	b.AdmitSenderEvents = nil
	b.SenderTransactions = nil
	b.ActivityMinutes[0].Totals = json.RawMessage(`{"swapCount":"0"}`)
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	_, _, events = readSenderState(t, s)
	if events[0].Canonical {
		t.Fatal("cancellation lost")
	}
	replay := senderFixture()
	replay.Cursor.Revision = 2
	if err := s.Commit(ctx, replay); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("canceled occurrence resurrected", err)
	}
	// Reset old children before changing the composite FK's parent generation.
	b.Cursor.Revision = 2
	b.Events = nil
	b.SenderEvents = nil
	b.SenderTransactions = nil
	b.ActivityMinutes = nil
	b.ResetActivity = []Identity{parent.Identity}
	b.ResetSenders = []Identity{parent.Identity}
	b.SenderSnapshots[0].Generation = 2
	b.SenderSnapshots[0].CreationEventID = digest("replacement")
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	pools, transactions, events = readSenderState(t, s)
	if pools[0].Generation != 2 || len(events) != 0 || len(transactions) != 1 {
		t.Fatal("reset corrupted shared state")
	}
	if err := s.Prune(ctx, now.Add(time.Minute), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	pools, transactions, events = readSenderState(t, s)
	if len(pools) != 0 || len(events) != 0 || len(transactions) != 1 {
		t.Fatal("cascade deleted shared transaction")
	}
}

// TestMySQLSenderAttempts verifies concurrent reservations, restart leases and terminal protection.
//
// Version:
//   - 2026-09-28: Added.
func TestMySQLSenderAttempts(t *testing.T) {
	s, db := senderMySQL(t)
	b := senderFixture()
	ctx := t.Context()
	now, key := b.Cursor.UpdatedAt, b.SenderTransactions[0].Key
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan SenderTransaction, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.ReserveSenderAttempt(ctx, key, 0, now)
			if err == nil {
				results <- r
			} else {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	if len(results) != 1 {
		t.Fatalf("reservation winners=%d", len(results))
	}
	for err := range errs {
		if !errors.Is(err, ErrSenderConflict) {
			t.Fatal(err)
		}
	}
	first := <-results
	retryAt := now.Add(2 * time.Second)
	if err := s.CompleteSenderAttempt(ctx, first, SenderAttemptResult{Status: SenderPending, NextAttemptAt: &retryAt, UpdatedAt: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveSenderAttempt(ctx, key, 1, now.Add(time.Second)); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("early retry allowed", err)
	}
	second, err := s.ReserveSenderAttempt(ctx, key, 1, retryAt)
	if err != nil {
		t.Fatal(err)
	}
	address := "MixedCaseSender"
	if err := s.CompleteSenderAttempt(ctx, first, SenderAttemptResult{Status: SenderResolved, SenderID: &address, UpdatedAt: now.Add(3 * time.Second)}); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("late result overwrote retry", err)
	}
	// Recompose a store as after restart: no in-memory lease or attempt reset.
	restarted, err := NewStoreWithTablePrefix(db, "market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.SenderTransaction(ctx, key)
	if err != nil || recovered.Attempts != 2 {
		t.Fatal("attempts lost on restore", err)
	}
	if _, err := restarted.ReserveSenderAttempt(ctx, key, 2, now.Add(3*time.Second)); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("active lease lost", err)
	}
	third, err := restarted.ReserveSenderAttempt(ctx, key, 2, *second.NextAttemptAt)
	if err != nil {
		t.Fatal(err)
	}
	fourth, err := restarted.ReserveSenderAttempt(ctx, key, 3, *third.NextAttemptAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ReserveSenderAttempt(ctx, key, 4, *fourth.NextAttemptAt); err == nil {
		t.Fatal("fifth attempt allowed")
	}
	if n, err := s.ExpireSenderTransactions(ctx, fourth.UpdatedAt, 1000); err != nil || n != 0 {
		t.Fatal("in-flight final attempt expired", n, err)
	}
	if n, err := s.ExpireSenderTransactions(ctx, *fourth.NextAttemptAt, 1000); err != nil || n != 1 {
		t.Fatal("exhausted attempt retained", n, err)
	}
	if err := s.CompleteSenderAttempt(ctx, fourth, SenderAttemptResult{Status: SenderResolved, SenderID: &address, UpdatedAt: *fourth.NextAttemptAt}); !errors.Is(err, ErrSenderConflict) {
		t.Fatal("terminal result reopened", err)
	}
	parent, err := s.Get(ctx, b.Snapshots[0].Identity)
	if err != nil || parent.Revision != 1 {
		t.Fatal("async operation rewrote parent", err)
	}
	b.Cursor.Revision = 1
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	got, err := s.SenderTransaction(ctx, key)
	if err != nil || got.Status != SenderAbandoned || got.Attempts != 4 || !got.CreatedAt.Equal(now) {
		t.Fatal("admission reset exhausted transaction", got, err)
	}
}

// TestMySQLSenderResolutionAndRestore verifies shared resolution and a coherent bounded restore.
//
// Version:
//   - 2026-09-28: Added.
//   - 2026-09-29: Mark the second Pool occurrence as a new admission.
func TestMySQLSenderResolutionAndRestore(t *testing.T) {
	s, _ := senderMySQL(t)
	ctx := t.Context()
	b := senderFixture()
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	// Two occurrences in different pools share one transaction result.
	second := senderFixture()
	second.Cursor.Revision = 1
	second.Snapshots[0].Identity.PoolID = "pool-2"
	pool := second.Snapshots[0].Identity
	second.Events[0].Pool = pool
	second.SenderEvents[0].Pool = pool
	second.SenderSnapshots[0].Pool = pool
	second.ActivityMinutes[0].Pool = pool
	second.AdmitSenderEvents = [][32]byte{second.SenderEvents[0].ID()}
	if err := s.Commit(ctx, second); err != nil {
		t.Fatal(err)
	}
	var reservation SenderTransaction
	reserved := false
	consumed := 0
	consumer := SenderStateConsumer{
		Pool: func(_ Snapshot, _ SenderSnapshot) error {
			if !reserved {
				var err error
				reservation, err = s.ReserveSenderAttempt(ctx, b.SenderTransactions[0].Key, 0, b.Cursor.UpdatedAt)
				reserved = true
				return err
			}
			return nil
		},
		Transaction: func(tx SenderTransaction) error {
			if tx.Attempts != 0 {
				return fmt.Errorf("failed to verify coherent restore: attempts=invalid")
			}
			return nil
		},
		Event: func(SenderEvent) error { consumed++; return nil },
	}
	if err := s.WalkSenderState(ctx, SenderRestoreLimits{2, 1, 2, 1}, consumer); err != nil {
		t.Fatal(err)
	}
	if consumed != 2 {
		t.Fatal("shared events omitted")
	}
	address := "sender"
	if err := s.CompleteSenderAttempt(ctx, reservation, SenderAttemptResult{Status: SenderResolved, SenderID: &address, UpdatedAt: b.Cursor.UpdatedAt.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	pools, transactions, events := readSenderState(t, s)
	if len(pools) != 2 || len(transactions) != 1 || len(events) != 2 || *transactions[0].SenderID != address {
		t.Fatal("shared result lost")
	}
	noop := SenderStateConsumer{Pool: func(Snapshot, SenderSnapshot) error { return nil }, Transaction: func(SenderTransaction) error { return nil }, Event: func(SenderEvent) error { return nil }}
	for _, limits := range []SenderRestoreLimits{{1, 1, 2, 1}, {2, 1, 1, 1}} {
		if err := s.WalkSenderState(ctx, limits, noop); !errors.Is(err, ErrSenderCapacity) {
			t.Fatal("restore overflow hidden", err)
		}
	}
	sentinel := errors.New("consumer stopped")
	noop.Transaction = func(SenderTransaction) error { return sentinel }
	if err := s.WalkSenderState(ctx, SenderRestoreLimits{2, 1, 2, 1}, noop); !errors.Is(err, sentinel) {
		t.Fatal("callback error chain lost", err)
	}
}

// TestMySQLSenderRetention verifies minute expiry, referenced transaction retention and bounded deletion.
//
// Version:
//   - 2026-09-28: Added.
//   - 2026-09-29: Mark the replacement block occurrence as a new admission.
func TestMySQLSenderRetention(t *testing.T) {
	s, _ := senderMySQL(t)
	ctx := t.Context()
	b := senderFixture()
	now := b.Cursor.UpdatedAt
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	// The same transaction is re-included at a later position, past its original cache expiry.
	later := senderFixture()
	later.Cursor.Revision = 1
	later.Events[0].PositionID = "later-block"
	later.Events[0].ObservedAt = now.Add(10 * time.Minute)
	later.SenderEvents[0].PositionID = "later-block"
	later.SenderEvents[0].ObservedAt = later.Events[0].ObservedAt
	later.SenderEvents[0].UpdatedAt = later.Events[0].ObservedAt
	later.SenderEvents[0].MinuteStartedAt = &later.Events[0].ObservedAt
	later.SenderEvents[0].ExpiresAt = now.Add(26 * time.Minute)
	later.AdmitSenderEvents = [][32]byte{later.SenderEvents[0].ID()}
	if err := s.Commit(ctx, later); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ReleaseSenderSnapshot(ctx, b.SenderSnapshots[0], now.Add(time.Minute)); err != nil || ok {
		t.Fatal("live children cascaded during release", err)
	}
	r, err := s.PruneSenders(ctx, now.Add(20*time.Minute), 1)
	if err != nil || r.Events != 1 || r.Transactions != 0 {
		t.Fatal("reference did not protect expired result", r, err)
	}
	_, transactions, events := readSenderState(t, s)
	if len(transactions) != 1 || len(events) != 1 || !transactions[0].ExpiresAt.Equal(now.Add(20*time.Minute)) {
		t.Fatal("expiry extended or reference lost")
	}
	r, err = s.PruneSenders(ctx, now.Add(26*time.Minute), 1)
	if err != nil || r.Events != 1 || r.Transactions != 1 {
		t.Fatal("unreferenced data retained", r, err)
	}
	if ok, err := s.ReleaseSenderSnapshot(ctx, b.SenderSnapshots[0], now.Add(26*time.Minute)); err != nil || !ok {
		t.Fatal("empty snapshot retained", err)
	}
	pools, transactions, events := readSenderState(t, s)
	if len(pools)+len(transactions)+len(events) != 0 {
		t.Fatal("orphan records retained")
	}
}

// TestMySQLSenderUnknownMinute verifies timestamp adoption cannot revive expired evidence.
//
// Version:
//   - 2026-09-28: Added.
//   - 2026-09-29: Skip late minute corrections without failing the Activity commit.
func TestMySQLSenderUnknownMinute(t *testing.T) {
	for _, delay := range []time.Duration{time.Minute, 2*time.Minute - time.Nanosecond, 2 * time.Minute, 3 * time.Minute} {
		t.Run(delay.String(), func(t *testing.T) {
			s, _ := senderMySQL(t)
			b := senderFixture()
			now := b.Cursor.UpdatedAt
			b.SenderEvents[0].MinuteStartedAt = nil
			b.SenderEvents[0].ExpiresAt = now.Add(2 * time.Minute)
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			b = senderCorrection(b)
			b.Cursor.Revision = 1
			b.Events = nil
			b.SenderEvents[0].MinuteStartedAt = &now
			b.SenderEvents[0].ExpiresAt = now.Add(16 * time.Minute)
			b.SenderEvents[0].UpdatedAt = now.Add(delay)
			if err := s.Commit(t.Context(), b); err != nil {
				t.Fatal("minute correction blocked Activity", err)
			}
			_, _, events := readSenderState(t, s)
			if delay < 2*time.Minute {
				if events[0].MinuteStartedAt == nil || !events[0].ExpiresAt.Equal(now.Add(16*time.Minute)) {
					t.Fatal("timely minute adoption lost")
				}
			} else if events[0].MinuteStartedAt != nil || !events[0].ExpiresAt.Equal(now.Add(2*time.Minute)) {
				t.Fatal("expired unknown minute revived")
			}
			if n, err := s.ExpireSenderTransactions(t.Context(), now.Add(3*time.Minute), 1000); err != nil || n != 1 {
				t.Fatal("unattempted deadline did not expire", n, err)
			}
		})
	}
}

// TestMySQLSenderInvalidRangeAndCapacity verifies conservative invalidation and physical row limits.
//
// Version:
//   - 2026-09-29: Added.
//   - 2026-09-29: Keep admission intents consistent with each changed occurrence.
func TestMySQLSenderInvalidRangeAndCapacity(t *testing.T) {
	s, db := senderMySQL(t)
	ctx := t.Context()
	b := senderFixture()
	now := b.Cursor.UpdatedAt
	end := now.Add(time.Minute)
	b.SenderSnapshots[0].InvalidFrom = &now
	b.SenderSnapshots[0].InvalidTo = &end
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	b.Cursor.Revision = 1
	b.SenderSnapshots[0].InvalidFrom = nil
	b.SenderSnapshots[0].InvalidTo = nil
	if err := s.Commit(ctx, b); err == nil {
		t.Fatal("live invalid interval cleared")
	}
	b.SenderSnapshots[0].UpdatedAt = now.Add(16 * time.Minute)
	b.SenderEvents = nil
	b.Events = nil
	b.SenderTransactions = nil
	b.AdmitSenderEvents = nil
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal("expired invalid range retained", err)
	}
	// Retained expired rows still count toward restore limits.
	next := senderFixture()
	next.Cursor.Revision = 2
	next.SenderSnapshots = nil
	next.SenderEvents[0].Index = "1"
	next.SenderEvents[0].Transaction.TransactionID = "second-tx"
	next.SenderTransactions[0].Key.TransactionID = "second-tx"
	next.Events[0].Index = "1"
	next.Events[0].TransactionID = "second-tx"
	next.AdmitSenderEvents = [][32]byte{next.SenderEvents[0].ID()}
	if err := s.Commit(ctx, next); err != nil {
		t.Fatal(err)
	}
	noop := SenderStateConsumer{Pool: func(Snapshot, SenderSnapshot) error { return nil }, Transaction: func(SenderTransaction) error { return nil }, Event: func(SenderEvent) error { return nil }}
	for _, limits := range []SenderRestoreLimits{{1, 1, 2, 2}, {1, 2, 2, 1}} {
		if err := s.WalkSenderState(ctx, limits, noop); !errors.Is(err, ErrSenderCapacity) {
			t.Fatal("physical capacity hidden", err)
		}
	}
	id := next.SenderTransactions[0].Key.ID()
	if _, err := db.ExecContext(ctx, s.query("UPDATE onchain_amm_pool_new_pair_sender_transactions SET attempts=5 WHERE id=?"), id[:]); err == nil {
		t.Fatal("SQL attempt CHECK missing")
	}
	// SQL shape alone cannot validate opaque identifiers: Go restore must reject corruption.
	if _, err := db.ExecContext(ctx, s.query("UPDATE onchain_amm_pool_new_pair_sender_transactions SET status='resolved',sender_id='',next_attempt_at=NULL WHERE id=?"), id[:]); err != nil {
		t.Fatal(err)
	}
	if err := s.WalkSenderState(ctx, SenderRestoreLimits{1, 2, 2, 2}, noop); err == nil {
		t.Fatal("invalid saved sender treated as usable")
	}
}
