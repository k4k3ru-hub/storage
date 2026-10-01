//go:build mysqlintegration

package ammpool

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type lpCheckpointBarrierConnector struct {
	driver.Connector
	arrived atomic.Int32
	release chan struct{}
}

// Connect decorates a real connection to coordinate checkpoint ownership reads.
//
// Version:
//   - 2026-10-01: Added.
func (c *lpCheckpointBarrierConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &lpCheckpointBarrierConnection{Conn: conn, owner: c}, nil
}

type lpCheckpointBarrierConnection struct {
	driver.Conn
	owner *lpCheckpointBarrierConnector
}

// BeginTx retains the real driver's isolation and context handling.
//
// Version:
//   - 2026-10-01: Added.
func (c *lpCheckpointBarrierConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

// QueryContext waits until both writers have checked the initially empty checkpoint table.
//
// Version:
//   - 2026-10-01: Added.
func (c *lpCheckpointBarrierConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil || !strings.HasPrefix(query, "SELECT source_id,creation_event_id") {
		return rows, err
	}
	if c.owner.arrived.Add(1) == 2 {
		close(c.owner.release)
	}
	select {
	case <-c.owner.release:
		return rows, nil
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), rows.Close())
	}
}

// TestMySQLLPCheckpointIndependentSources verifies concurrent first saves without cross-source gap locks.
//
// Version:
//   - 2026-10-01: Added.
func TestMySQLLPCheckpointIndependentSources(t *testing.T) {
	s, db := senderMySQL(t)
	b, params := seedLPCheckpointPools(t, s, 1)
	other := params[0]
	other.Source.Key = "another-manager"
	other.Checkpoint.Pool.PoolID = "another-pool"
	b.Cursor.Source = other.Source
	b.Snapshots[0].Identity = other.Checkpoint.Pool
	b.Events[0].Source, b.Events[0].Pool = other.Source, other.Checkpoint.Pool
	other.Checkpoint.CreationEventID = b.Events[0].ID()
	if err := s.Commit(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	params = append(params, other)
	before := make([]Cursor, len(params))
	for i, p := range params {
		before[i] = currentLPCursor(t, s, p.Source)
	}

	cfg, err := mysql.ParseDSN(os.Getenv("AMMPOOL_TEST_DSN"))
	if err != nil {
		t.Fatal("invalid test connection")
	}
	if err := db.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&cfg.DBName); err != nil {
		t.Fatal(err)
	}
	cfg.ParseTime = true
	// Keep parameterized queries on QueryContext so the barrier cannot be bypassed
	// by database/sql's fallback to prepared statements.
	cfg.InterpolateParams = true
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal("invalid isolated test connector")
	}
	barrier := &lpCheckpointBarrierConnector{Connector: connector, release: make(chan struct{})}
	concurrentDB := sql.OpenDB(barrier)
	concurrentDB.SetMaxOpenConns(2)
	t.Cleanup(func() {
		if err := concurrentDB.Close(); err != nil {
			t.Error(err)
		}
	})
	concurrentStore, err := NewStoreWithTablePrefix(concurrentDB, "market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results := make([]LPCheckpointSaveResult, len(params))
	errs := make([]error, len(params))
	var wg sync.WaitGroup
	for i := range params {
		wg.Go(func() {
			results[i], errs[i] = concurrentStore.SaveLPCheckpoint(ctx, params[i])
		})
	}
	wg.Wait()
	if barrier.arrived.Load() != 2 {
		t.Fatalf("both writers must reach the ownership read: arrived=%d errors=%v", barrier.arrived.Load(), errs)
	}
	for i, err := range errs {
		if err != nil {
			t.Errorf("independent source %d failed to save: %v", i, err)
		}
	}
	if t.Failed() {
		return
	}
	for i, p := range params {
		cursor := currentLPCursor(t, s, p.Source)
		if cursor.Revision != before[i].Revision+1 || cursor.Revision != results[i].Metadata.Ref.Revision || !bytes.Equal(cursor.Position, before[i].Position) {
			t.Fatal("independent source cursor was not advanced exactly once")
		}
		entries := loadLPCheckpoint(t, s, p.Source, results[i].Metadata.Ref)
		if len(entries) != 1 || entries[0].Checkpoint.Pool != p.Checkpoint.Pool || entries[0].Checkpoint.CreationEventID != p.Checkpoint.CreationEventID {
			t.Fatal("independent checkpoint was not persisted")
		}
		if len(loadLPCheckpoint(t, s, params[1-i].Source, results[i].Metadata.Ref)) != 0 {
			t.Fatal("checkpoint crossed source ownership")
		}
		parent, err := s.Get(t.Context(), p.Checkpoint.Pool)
		if err != nil {
			t.Fatal(err)
		}
		if parent.Revision != p.ExpectedParentRevision {
			t.Fatal("checkpoint advanced the parent revision")
		}
	}
}
