//
// usage_balance_test.go
//
package app

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type usageBalanceExecutorStub struct {
	query string
	args  []any
}

func (e *usageBalanceExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}

func (e *usageBalanceExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query = query
	e.args = args
	return nil, nil
}

func (*usageBalanceExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

func (*usageBalanceExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}

func (*usageBalanceExecutorStub) QueryRow(string, ...any) *sql.Row { return &sql.Row{} }

func (*usageBalanceExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return &sql.Row{}
}

func TestUsageBalanceDeleteByAccountID(t *testing.T) {
	store, err := NewUsageBalanceStore(DefaultUsageBalanceTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &usageBalanceExecutorStub{}

	if err := store.DeleteByAccountID(context.Background(), executor, 42); err != nil {
		t.Fatal(err)
	}

	wantQuery := "DELETE FROM " + DefaultUsageBalanceTableName + " WHERE " + ColAccountID + " = ?;"
	if executor.query != wantQuery {
		t.Fatalf("query = %q, want %q", executor.query, wantQuery)
	}
	if len(executor.args) != 1 || executor.args[0] != uint64(42) {
		t.Fatalf("args = %#v, want [42]", executor.args)
	}
}

func TestUsageBalanceInsertRejectsNilParams(t *testing.T) {
	store, err := NewUsageBalanceStore(DefaultUsageBalanceTableName)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Insert(context.Background(), &sql.DB{}, nil)
	if err == nil || !strings.Contains(err.Error(), "usage_balance_insert_params=null") {
		t.Fatalf("error = %v, want usage_balance_insert_params=null error", err)
	}
}

func TestUsageBalanceBuildQueryWithoutConditions(t *testing.T) {
	params := UsageBalanceSelectParams{OrderBy: ColCreatedAt, OrderByDesc: true, Limit: 10, Offset: 20}
	query, args := params.BuildQuery("SELECT * FROM balances")

	wantQuery := "SELECT * FROM balances ORDER BY created_at DESC LIMIT ? OFFSET ?"
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 2 || args[0] != 10 || args[1] != 20 {
		t.Fatalf("args = %#v, want [10 20]", args)
	}
}

func TestUsageBalanceRejectsUnsafeSQLIdentifiers(t *testing.T) {
	if _, err := NewUsageBalanceStore("balances; DROP TABLE balances"); err == nil {
		t.Fatal("NewUsageBalanceStore accepted an unsafe table name")
	}

	params := UsageBalanceSelectParams{OrderBy: "created_at; DROP TABLE balances", Limit: 1}
	query, _ := params.BuildQuery("SELECT * FROM balances")
	if strings.Contains(query, "DROP") || strings.Contains(query, "ORDER BY") {
		t.Fatalf("unsafe order by was included in query: %q", query)
	}
}
