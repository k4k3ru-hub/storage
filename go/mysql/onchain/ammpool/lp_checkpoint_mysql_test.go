//go:build mysqlintegration

package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func seedLPCheckpointPools(t *testing.T, s *Store, count int) (Batch, []LPCheckpointSaveParams) {
	t.Helper()
	base := lpCheckpointExample()
	b := Batch{Cursor: Cursor{Source: base.Source, Position: json.RawMessage(`{"through":"100"}`), UpdatedAt: base.Checkpoint.UpdatedAt}}
	params := make([]LPCheckpointSaveParams, count)
	for i := range count {
		p := base
		p.Checkpoint.Pool.PoolID = fmt.Sprintf("lp-pool-%04d", i)
		v := Snapshot{Identity: p.Checkpoint.Pool, Token0ID: "token0", Token1ID: "token1", CreatedAt: b.Cursor.UpdatedAt,
			State: json.RawMessage(`{"untouched":"parent"}`), Canonical: true, UpdatedAt: b.Cursor.UpdatedAt}
		e := Event{Pool: v.Identity, Source: b.Cursor.Source, PositionNumber: 100, PositionID: "creation-hash", TransactionID: "creation-tx", Index: fmt.Sprint(i), Type: "created",
			OccurredAt: b.Cursor.UpdatedAt, ObservedAt: b.Cursor.UpdatedAt, Payload: json.RawMessage(`{}`), Canonical: true}
		p.Checkpoint.CreationEventID = e.ID()
		b.Snapshots = append(b.Snapshots, v)
		b.Events = append(b.Events, e)
		params[i] = p
	}
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	return b, params
}

func currentLPCursor(t *testing.T, s *Store, source Source) Cursor {
	t.Helper()
	c, err := s.Cursor(t.Context(), source)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func saveLPCheckpoint(t *testing.T, s *Store, p LPCheckpointSaveParams) LPCheckpointSaveResult {
	t.Helper()
	p.ExpectedCursorRevision = currentLPCursor(t, s, p.Source).Revision
	out, err := s.SaveLPCheckpoint(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func loadLPCheckpoint(t *testing.T, s *Store, source Source, refs ...LPCheckpointRef) []LPCheckpointEntry {
	t.Helper()
	out, err := s.LoadLPCheckpoints(t.Context(), source, refs)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestMySQLLPCheckpointLifecycle verifies isolated writes, stable old evidence and exact revisions.
//
// Version:
//   - 2026-10-01: Added.
func TestMySQLLPCheckpointLifecycle(t *testing.T) {
	s, _ := senderMySQL(t)
	b, ps := seedLPCheckpointPools(t, s, 2)
	p := ps[0]
	ctx := t.Context()
	before, err := s.Get(ctx, p.Checkpoint.Pool)
	if err != nil {
		t.Fatal(err)
	}
	c := currentLPCursor(t, s, p.Source)
	saved := saveLPCheckpoint(t, s, p)
	after, err := s.Get(ctx, p.Checkpoint.Pool)
	if err != nil {
		t.Fatal(err)
	}
	if after.Revision != before.Revision || !bytes.Equal(after.State, before.State) || !after.UpdatedAt.Equal(before.UpdatedAt) || !bytes.Equal(c.Position, saved.Cursor.Position) || saved.Cursor.Revision != c.Revision+1 || saved.Metadata.Ref.Revision != saved.Cursor.Revision {
		t.Fatal("checkpoint rewrote projection/history or failed to advance its cursor")
	}
	entries := loadLPCheckpoint(t, s, p.Source, saved.Metadata.Ref)
	if len(entries) != 1 || entries[0].Metadata.PayloadBytes != uint32(len(entries[0].Checkpoint.Payload)) || !bytes.Contains(entries[0].Checkpoint.Payload, []byte("340282366920938463463374607431768211455")) {
		t.Fatal("exact decimal evidence lost")
	}
	if out, err := s.SaveLPCheckpoint(ctx, p); !errors.Is(err, ErrCursorConflict) || out.Cursor.Revision != 0 {
		t.Fatal("old source revision accepted", err)
	}
	// A routine parent update must not invalidate otherwise reusable LP evidence.
	b.Cursor = saved.Cursor
	b.Snapshots = b.Snapshots[:1]
	b.Snapshots[0].State = json.RawMessage(`{"untouched":"new-price"}`)
	b.Events = nil
	if err := s.Commit(ctx, b); err != nil {
		t.Fatal(err)
	}
	if len(loadLPCheckpoint(t, s, p.Source, saved.Metadata.Ref)) != 1 {
		t.Fatal("ordinary parent revision invalidated checkpoint")
	}
	p.ExpectedCursorRevision = saved.Cursor.Revision + 1
	if _, err := s.SaveLPCheckpoint(ctx, p); !errors.Is(err, ErrLPCheckpointConflict) {
		t.Fatal("stale parent revision accepted", err)
	}
	p.ExpectedParentRevision = 2
	index := "0"
	p.Checkpoint.Position.Index = &index
	p.Checkpoint.Position.Number = 101
	updated := saveLPCheckpoint(t, s, p)
	if len(loadLPCheckpoint(t, s, p.Source, saved.Metadata.Ref)) != 0 {
		t.Fatal("superseded read reference returned newer payload")
	}
	entries = loadLPCheckpoint(t, s, p.Source, updated.Metadata.Ref)
	if len(entries) != 1 || entries[0].Checkpoint.Position.Index == nil || *entries[0].Checkpoint.Position.Index != "0" {
		t.Fatal("partial-block cursor lost")
	}
	saveLPCheckpoint(t, s, ps[1])
	page, err := s.ListLPCheckpointMetadata(ctx, LPCheckpointListParams{Source: p.Source, Limit: 1})
	if err != nil || len(page.Items) != 1 || page.NextAfterID == nil {
		t.Fatal("first metadata page", err)
	}
	last, err := s.ListLPCheckpointMetadata(ctx, LPCheckpointListParams{Source: p.Source, Limit: 1, AfterPoolID: page.NextAfterID})
	if err != nil || len(last.Items) != 1 || last.NextAfterID != nil || last.Items[0].Ref.Pool == page.Items[0].Ref.Pool {
		t.Fatal("metadata pagination", err)
	}
	wrongSource := p.Source
	wrongSource.Key = "another-manager"
	if len(loadLPCheckpoint(t, s, wrongSource, updated.Metadata.Ref)) != 0 {
		t.Fatal("cross-source read")
	}
	other, err := NewStoreWithTablePrefix(s.db, "other_")
	if err != nil {
		t.Fatal(err)
	}
	if err := other.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}
	page, err = other.ListLPCheckpointMetadata(ctx, LPCheckpointListParams{Source: p.Source, Limit: 1})
	if err != nil || len(page.Items) != 0 {
		t.Fatal("prefix isolation", err)
	}
}

// TestMySQLLPCheckpointCorrections verifies atomic cancellation, replacement, reset and cascade.
//
// Version:
//   - 2026-10-01: Added.
func TestMySQLLPCheckpointCorrections(t *testing.T) {
	s, db := senderMySQL(t)
	b, ps := seedLPCheckpointPools(t, s, 1)
	p := ps[0]
	ctx := t.Context()
	saved := saveLPCheckpoint(t, s, p)
	reset := Batch{Cursor: b.Cursor, ResetLPCheckpoints: []Identity{p.Checkpoint.Pool}}
	if err := s.Commit(ctx, reset); !errors.Is(err, ErrCursorConflict) {
		t.Fatal("stale reset accepted", err)
	}
	if len(loadLPCheckpoint(t, s, p.Source, saved.Metadata.Ref)) != 1 {
		t.Fatal("failed reset deleted checkpoint")
	}
	foreign := p.Source
	foreign.Key = "foreign"
	reset.Cursor = Cursor{Source: foreign, Position: json.RawMessage(`{}`), UpdatedAt: b.Cursor.UpdatedAt}
	if err := s.Commit(ctx, reset); !errors.Is(err, ErrLPCheckpointConflict) {
		t.Fatal("foreign reset accepted", err)
	}
	if currentLPCursor(t, s, foreign).Revision != 0 {
		t.Fatal("failed reset advanced source")
	}
	old, replacement := b.Events[0], b.Events[0]
	replacement.PositionID = "replacement-hash"
	old.Canonical = false
	change := Batch{Cursor: saved.Cursor, Events: []Event{old, replacement}}
	if err := s.Commit(ctx, change); err != nil {
		t.Fatal(err)
	}
	if len(loadLPCheckpoint(t, s, p.Source, saved.Metadata.Ref)) != 0 {
		t.Fatal("old creation retained")
	}
	p.Checkpoint.CreationEventID = replacement.ID()
	newSaved := saveLPCheckpoint(t, s, p)
	change.Cursor = newSaved.Cursor
	change.Events = []Event{old}
	if err := s.Commit(ctx, change); err != nil {
		t.Fatal(err)
	}
	if len(loadLPCheckpoint(t, s, p.Source, newSaved.Metadata.Ref)) != 1 {
		t.Fatal("old cancellation removed new generation")
	}
	// A rejected batch must roll back parent updates and the automatic deletion.
	bad := Batch{Cursor: currentLPCursor(t, s, p.Source), Snapshots: b.Snapshots,
		ActivityMinutes: []ActivityMinute{{Pool: p.Checkpoint.Pool, Start: b.Cursor.UpdatedAt.Truncate(time.Minute), Totals: json.RawMessage(`{}`), UpdatedAt: b.Cursor.UpdatedAt}}}
	bad.Snapshots[0].Canonical = false
	// The activity table is deliberately unavailable after validation to fail late in Commit.
	if _, err := db.ExecContext(ctx, s.query("RENAME TABLE onchain_amm_pool_new_pair_activity_minutes TO unavailable_activity")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(ctx, bad); err == nil {
		t.Fatal("late database error accepted")
	}
	if _, err := db.ExecContext(ctx, s.query("RENAME TABLE unavailable_activity TO onchain_amm_pool_new_pair_activity_minutes")); err != nil {
		t.Fatal(err)
	}
	if len(loadLPCheckpoint(t, s, p.Source, newSaved.Metadata.Ref)) != 1 {
		t.Fatal("rolled-back parent cancellation lost checkpoint")
	}
	bad.ActivityMinutes = nil
	if err := s.Commit(ctx, bad); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListLPCheckpointMetadata(ctx, LPCheckpointListParams{Source: p.Source, Limit: 1})
	if err != nil || len(page.Items) != 0 {
		t.Fatal("noncanonical parent retained checkpoint", err)
	}
	bad.Cursor = currentLPCursor(t, s, p.Source)
	bad.Snapshots[0].Canonical = true
	if err := s.Commit(ctx, bad); err != nil {
		t.Fatal(err)
	}
	parent, err := s.Get(ctx, p.Checkpoint.Pool)
	if err != nil {
		t.Fatal(err)
	}
	p.ExpectedParentRevision = parent.Revision
	restored := saveLPCheckpoint(t, s, p)
	reset.Cursor, reset.ResetLPCheckpoints = restored.Cursor, []Identity{p.Checkpoint.Pool}
	if err := s.Commit(ctx, reset); err != nil {
		t.Fatal(err)
	}
	recreated := saveLPCheckpoint(t, s, p)
	if recreated.Metadata.Ref.Revision <= restored.Metadata.Ref.Revision || len(loadLPCheckpoint(t, s, p.Source, restored.Metadata.Ref)) != 0 {
		t.Fatal("delete/recreate reused old reference")
	}
	if _, err := s.PruneSnapshotBatch(ctx, b.Cursor.UpdatedAt.Add(time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, s.query("SELECT COUNT(*) FROM onchain_amm_pool_new_pair_lp_checkpoints")).Scan(&count); err != nil || count != 0 {
		t.Fatal("parent cascade failed", count, err)
	}
}

func lpPayloadSize(size int) []byte {
	// MySQL's normalized object adds one space after the colon.
	return []byte(`{"v":"` + strings.Repeat("x", size-len(`{"v": ""}`)) + `"}`)
}

func seedLPCheckpointCapacity(t *testing.T, s *Store, db *sql.DB, b Batch, payload []byte) {
	t.Helper()
	sid := b.Cursor.Source.ID()
	n := len(b.Snapshots)
	_, err := db.ExecContext(t.Context(), s.query(`INSERT INTO onchain_amm_pool_new_pair_lp_checkpoints
 (pool_id,source_id,creation_event_id,format_version,position_kind,position_number,position_id,payload,revision,updated_at)
 SELECT p.id,e.source_id,e.id,1,'block',100,'hash',?,1,? FROM onchain_amm_pool_new_pair_snapshots p
 JOIN onchain_amm_pool_new_pair_events e ON e.pool_id=p.id
 WHERE e.source_id=? AND p.pool_id NOT IN (?,?)`), payload, dbTime(b.Cursor.UpdatedAt), sid[:], b.Snapshots[n-2].Identity.PoolID, b.Snapshots[n-1].Identity.PoolID)
	if err != nil {
		t.Fatal(err)
	}
}

// TestMySQLLPCheckpointCapacity verifies normalization, byte quotas, row quotas and racing writers.
//
// Version:
//   - 2026-10-01: Added.
func TestMySQLLPCheckpointCapacity(t *testing.T) {
	for _, test := range []struct {
		name  string
		pools int
		size  int
	}{{"rows", MaxLPCheckpointPools + 1, 2}, {"bytes", MaxLPCheckpointSourceBytes/MaxLPCheckpointBytes + 1, MaxLPCheckpointBytes}} {
		t.Run(test.name, func(t *testing.T) {
			s, db := senderMySQL(t)
			b, ps := seedLPCheckpointPools(t, s, test.pools)
			payload := []byte(`{}`)
			if test.size > 2 {
				payload = lpPayloadSize(test.size)
			}
			seedLPCheckpointCapacity(t, s, db, b, payload)
			candidates := ps[len(ps)-2:]
			var wg sync.WaitGroup
			errs := make([]error, 2)
			for i := range candidates {
				candidates[i].Checkpoint.Payload = payload
				wg.Go(func() { _, errs[i] = s.SaveLPCheckpoint(t.Context(), candidates[i]) })
			}
			wg.Wait()
			loser := -1
			for i, err := range errs {
				if errors.Is(err, ErrCursorConflict) {
					loser = i
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if loser == -1 || errs[1-loser] != nil {
				t.Fatal("expected exactly one writer", errs)
			}
			c := currentLPCursor(t, s, b.Cursor.Source)
			candidates[loser].ExpectedCursorRevision = c.Revision
			if out, err := s.SaveLPCheckpoint(t.Context(), candidates[loser]); !errors.Is(err, ErrLPCheckpointCapacity) || out.Cursor.Revision != 0 {
				t.Fatal("capacity violation accepted", err)
			}
			if currentLPCursor(t, s, b.Cursor.Source).Revision != c.Revision {
				t.Fatal("failed capacity check advanced cursor")
			}
			if test.name == "bytes" {
				refs := make([]LPCheckpointRef, 9)
				for i := range refs {
					refs[i] = LPCheckpointRef{Pool: ps[i].Checkpoint.Pool, Revision: 1}
				}
				if len(loadLPCheckpoint(t, s, b.Cursor.Source, refs[:8]...)) != 8 {
					t.Fatal("4 MiB boundary rejected")
				}
				if out, err := s.LoadLPCheckpoints(t.Context(), b.Cursor.Source, refs); !errors.Is(err, ErrLPCheckpointCapacity) || len(out) != 0 {
					t.Fatal("unbounded or partial read", err)
				}
				// Expansion occurs in MySQL even though the input is below 512 KiB.
				p := ps[0]
				p.ExpectedCursorRevision = c.Revision
				p.Checkpoint.Payload = []byte(`{"a":"` + strings.Repeat("x", MaxLPCheckpointBytes-len(`{"a":"","b":""}`)-1) + `","b":""}`)
				if out, err := s.SaveLPCheckpoint(t.Context(), p); !errors.Is(err, ErrLPCheckpointCapacity) || out.Metadata.Ref.Revision != 0 {
					t.Fatal("normalized size violation accepted", err)
				}
			}
		})
	}
}

// TestMySQLLPCheckpointReadSnapshot verifies that selection and payload use the same database view.
//
// Version:
//   - 2026-10-01: Added.
func TestMySQLLPCheckpointReadSnapshot(t *testing.T) {
	s, _ := senderMySQL(t)
	b, ps := seedLPCheckpointPools(t, s, 1)
	p := ps[0]
	saved := saveLPCheckpoint(t, s, p)
	tx, err := s.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			t.Error(e)
		}
	}()
	meta, err := s.readLPCheckpointSelection(t.Context(), tx, p.Source, []LPCheckpointRef{saved.Metadata.Ref})
	if err != nil || len(meta) != 1 {
		t.Fatal(err)
	}
	p.Checkpoint.Payload = lpPayloadSize(MaxLPCheckpointBytes)
	newSaved := saveLPCheckpoint(t, s, p)
	where, args := lpCheckpointSelection(p.Source, []LPCheckpointRef{saved.Metadata.Ref})
	rows, err := tx.QueryContext(t.Context(), s.query("SELECT c.pool_id,c.revision,"+lpCheckpointPayloadText+" FROM onchain_amm_pool_new_pair_lp_checkpoints c WHERE "+where), args...)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := readLPCheckpointPayloads(rows, meta)
	if err != nil || len(entries) != 1 || len(entries[0].Checkpoint.Payload) >= MaxLPCheckpointBytes {
		t.Fatal("payload crossed snapshot boundary", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(loadLPCheckpoint(t, s, p.Source, saved.Metadata.Ref)) != 0 {
		t.Fatal("new read returned superseded payload")
	}
	if _, err := s.PruneEventBatch(context.Background(), b.Cursor.UpdatedAt.Add(time.Hour), 1); err != nil {
		t.Fatal(err)
	}
	if len(loadLPCheckpoint(t, s, p.Source, newSaved.Metadata.Ref)) != 0 {
		t.Fatal("checkpoint survived missing creation proof")
	}
	p.ExpectedCursorRevision = newSaved.Cursor.Revision
	if _, err := s.SaveLPCheckpoint(t.Context(), p); !errors.Is(err, ErrLPCheckpointConflict) {
		t.Fatal("missing creation proof saved", err)
	}
}

// TestMySQLLPCheckpointConstraints verifies that direct SQL cannot bypass the row-level DDL checks.
//
// Version:
//   - 2026-10-01: Added.
func TestMySQLLPCheckpointConstraints(t *testing.T) {
	s, db := senderMySQL(t)
	_, ps := seedLPCheckpointPools(t, s, 1)
	p := ps[0]
	saved := saveLPCheckpoint(t, s, p)
	pid, sid := p.Checkpoint.Pool.ID(), p.Source.ID()
	for _, invalid := range []struct {
		assignment string
		args       []any
	}{
		{"format_version=0", nil},
		{"revision=0", nil},
		{"position_id=''", nil},
		{"event_index=''", nil},
		{"payload=?", []any{`[]`}},
		{"payload=?", []any{lpPayloadSize(MaxLPCheckpointBytes + 1)}},
	} {
		args := append(invalid.args, pid[:])
		if _, err := db.ExecContext(t.Context(), s.query("UPDATE onchain_amm_pool_new_pair_lp_checkpoints SET "+invalid.assignment+" WHERE pool_id=?"), args...); err == nil {
			t.Fatal("constraint accepted invalid direct update", invalid.assignment)
		}
	}
	if _, err := db.ExecContext(t.Context(), s.query("DELETE FROM onchain_amm_pool_new_pair_sync_cursors WHERE id=?"), sid[:]); err == nil {
		t.Fatal("source with checkpoint references was deleted")
	}
	if len(loadLPCheckpoint(t, s, p.Source, saved.Metadata.Ref)) != 1 {
		t.Fatal("failed DDL checks modified the valid checkpoint")
	}
}
