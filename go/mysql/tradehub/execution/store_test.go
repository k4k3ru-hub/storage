package execution

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

type executorStub struct {
	query   string
	args    []any
	queries []string
}

func (e *executorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}
func (e *executorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	e.queries = append(e.queries, query)
	return resultStub(1), nil
}

func TestStoreCreateTables(t *testing.T) {
	store, err := NewDefaultStore()
	if err != nil {
		t.Fatal(err)
	}
	executor := new(executorStub)
	if err := store.CreateTables(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	if len(executor.queries) != 3 {
		t.Fatalf("CreateTables() executed %d queries, want 3", len(executor.queries))
	}
	if !strings.Contains(executor.queries[1], "FOREIGN KEY (execution_id) REFERENCES trade_hub_executions") || !strings.Contains(executor.queries[2], "FOREIGN KEY (execution_leg_id) REFERENCES trade_hub_execution_legs") {
		t.Fatal("CreateTables() did not create the expected hierarchy")
	}
}
func (*executorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected query")
}
func (*executorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected query")
}
func (*executorStub) QueryRow(string, ...any) *sql.Row                         { return &sql.Row{} }
func (*executorStub) QueryRowContext(context.Context, string, ...any) *sql.Row { return &sql.Row{} }

type resultStub int64

func (resultStub) LastInsertId() (int64, error)   { return 0, nil }
func (r resultStub) RowsAffected() (int64, error) { return int64(r), nil }

func TestStoreInsertSnapshotHierarchy(t *testing.T) {
	store, err := NewDefaultStore()
	if err != nil {
		t.Fatal(err)
	}
	executor := new(executorStub)
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	if err := store.InsertExecution(context.Background(), executor, ExecutionInsertParams{ID: "exec_1", Status: StatusPrepared, Kind: "swap", RequestSnapshot: []byte(`{"amount":"100"}`), PreparedAt: now, ExpiresAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(executor.query, "INSERT INTO trade_hub_executions") {
		t.Fatalf("unexpected query: %s", executor.query)
	}
	leg := &LegInsertParams{ExecutionID: "exec_1", Category: LegCategoryOnchainTransaction, Status: LegStatusAwaitingSignature, Venue: "uniswap-v3"}
	if err := store.InsertLeg(context.Background(), executor, leg); err != nil {
		t.Fatal(err)
	}
	if leg.ID == 0 {
		t.Fatal("InsertLeg() did not generate an ID")
	}
	if !strings.Contains(executor.query, "INSERT INTO trade_hub_execution_legs") {
		t.Fatalf("unexpected query: %s", executor.query)
	}
	if err := store.InsertOnchainTransaction(context.Background(), executor, OnchainTransactionInsertParams{ExecutionLegID: leg.ID, ChainFamily: "evm", Chain: "base", Network: "sepolia", Signer: "0x1", PayloadDigest: "0x2"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(executor.query, "INSERT INTO trade_hub_execution_onchain_transactions") {
		t.Fatalf("unexpected query: %s", executor.query)
	}
}

func TestNewStoreRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewStore("executions; DROP TABLE executions", "legs", "transactions"); err == nil {
		t.Fatal("NewStore() error = nil, want unsafe table name error")
	}
}

func TestExecutionInsertParamsRejectsInvalidSnapshot(t *testing.T) {
	now := time.Now().UTC()
	params := ExecutionInsertParams{ID: "exec_1", Status: StatusPrepared, Kind: "swap", RequestSnapshot: []byte(`[]`), PreparedAt: now, ExpiresAt: now.Add(time.Minute)}
	if err := params.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want request snapshot error")
	}
}
