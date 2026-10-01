//go:build mysqlintegration

package ammpool

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/go-sql-driver/mysql"
)

var errLPCommitAcknowledgment = errors.New("test commit acknowledgment lost")

type lpCommitConnector struct {
	driver.Connector
	fail atomic.Bool
}

// Connect decorates a test connection with a one-shot ambiguous commit.
//
// Version:
//   - 2026-10-01: Added.
func (c *lpCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &lpCommitConnection{Conn: conn, owner: c}, nil
}

type lpCommitConnection struct {
	driver.Conn
	owner *lpCommitConnector
}

// BeginTx wraps transactions while retaining the real MySQL transaction behavior.
//
// Version:
//   - 2026-10-01: Added.
func (c *lpCommitConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &lpCommitTransaction{Tx: tx, owner: c.owner}, nil
}

type lpCommitTransaction struct {
	driver.Tx
	owner *lpCommitConnector
}

// Commit injects an error after the real database has committed successfully.
//
// Version:
//   - 2026-10-01: Added.
func (t *lpCommitTransaction) Commit() error {
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	if t.owner.fail.Swap(false) {
		return errLPCommitAcknowledgment
	}
	return nil
}

// TestMySQLLPCheckpointUncertainCommit rejects false success and blind replay after lost acknowledgment.
//
// Version:
//   - 2026-10-01: Added.
func TestMySQLLPCheckpointUncertainCommit(t *testing.T) {
	s, db := senderMySQL(t)
	_, ps := seedLPCheckpointPools(t, s, 1)
	p := ps[0]
	cfg, err := mysql.ParseDSN(os.Getenv("AMMPOOL_TEST_DSN"))
	if err != nil {
		t.Fatal("invalid test connection")
	}
	if err := db.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&cfg.DBName); err != nil {
		t.Fatal(err)
	}
	cfg.ParseTime = true
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal("invalid isolated test connector")
	}
	fault := &lpCommitConnector{Connector: connector}
	fault.fail.Store(true)
	faultDB := sql.OpenDB(fault)
	t.Cleanup(func() {
		if err := faultDB.Close(); err != nil {
			t.Error(err)
		}
	})
	faultStore, err := NewStoreWithTablePrefix(faultDB, "market_hub_")
	if err != nil {
		t.Fatal(err)
	}
	out, err := faultStore.SaveLPCheckpoint(t.Context(), p)
	if !errors.Is(err, errLPCommitAcknowledgment) || out.Cursor.Revision != 0 || out.Metadata.Ref.Revision != 0 {
		t.Fatal("ambiguous commit exposed a success receipt", err)
	}
	cursor := currentLPCursor(t, s, p.Source)
	page, err := s.ListLPCheckpointMetadata(t.Context(), LPCheckpointListParams{Source: p.Source, Limit: 1})
	if err != nil || len(page.Items) != 1 || cursor.Revision != p.ExpectedCursorRevision+1 || page.Items[0].Ref.Revision != cursor.Revision {
		t.Fatal("read-back did not expose committed state", err)
	}
	if _, err := faultStore.SaveLPCheckpoint(t.Context(), p); !errors.Is(err, ErrCursorConflict) {
		t.Fatal("blind replay after uncertain commit accepted", err)
	}
}
