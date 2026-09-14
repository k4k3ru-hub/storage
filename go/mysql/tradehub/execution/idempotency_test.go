package execution

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	storageapi "github.com/k4k3ru-hub/storage/go/api"
)

// TestExecutionInsertParamsOptionalMetadata verifies nullable fields and limits.
//
// Version:
//   - 2026-09-14: Added.
func TestExecutionInsertParamsOptionalMetadata(t *testing.T) {
	accountID := uint64(1)
	zeroID := uint64(0)
	for _, tt := range []struct {
		name   string
		mutate func(*ExecutionInsertParams)
		valid  bool
	}{
		{"null metadata and times", func(*ExecutionInsertParams) {}, true},
		{"binary key", func(p *ExecutionInsertParams) { p.AccountID = &accountID; p.IdempotencyKey = []byte{0, 255} }, true},
		{"maximum key", func(p *ExecutionInsertParams) { p.IdempotencyKey = bytes.Repeat([]byte{'a'}, 128) }, true},
		{"long key", func(p *ExecutionInsertParams) { p.IdempotencyKey = bytes.Repeat([]byte{'a'}, 129) }, false},
		{"empty key", func(p *ExecutionInsertParams) { p.IdempotencyKey = []byte{} }, false},
		{"zero account", func(p *ExecutionInsertParams) { p.AccountID = &zeroID }, false},
		{"result object", func(p *ExecutionInsertParams) { p.ResultSnapshot = []byte(`{"status":"ready"}`) }, true},
		{"invalid result", func(p *ExecutionInsertParams) { p.ResultSnapshot = []byte(`[]`) }, false},
		{"invalid expiry", func(p *ExecutionInsertParams) { p.PreparedAt = time.Unix(10, 0); p.ExpiresAt = time.Unix(9, 0) }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := ExecutionInsertParams{ID: "exec_1", Status: StatusPrepared, Kind: "swap", RequestSnapshot: []byte(`{}`)}
			tt.mutate(&params)
			if err := params.Validate(); (err == nil) != tt.valid {
				t.Fatalf("Validate() error = %v, want valid=%t", err, tt.valid)
			}
		})
	}
}

// TestExecutionSchemaRoundTrip verifies MySQL uniqueness and nullable snapshot reads.
// Set K4K3RU_EXECUTION_TEST_DSN to an isolated MySQL test database to run it.
//
// Version:
//   - 2026-09-14: Added.
func TestExecutionSchemaRoundTrip(t *testing.T) {
	dsn := os.Getenv("K4K3RU_EXECUTION_TEST_DSN")
	if dsn == "" {
		t.Skip("K4K3RU_EXECUTION_TEST_DSN is not configured")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})
	prefix := fmt.Sprintf("execution_test_%d", time.Now().UnixNano())
	store, err := NewStore(prefix, prefix+"_legs", prefix+"_transactions")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, table := range []string{prefix + "_transactions", prefix + "_legs", prefix} {
			if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
				t.Errorf("drop test table: %v", err)
			}
		}
	})
	if err := store.CreateTables(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	accountID, otherAccountID := uint64(1), uint64(2)
	params := ExecutionInsertParams{
		ID: "first", AccountID: &accountID, IdempotencyKey: []byte("Key"),
		Status: StatusPrepared, Kind: "swap", RequestSnapshot: []byte(`{"amount":"100"}`),
		ResultSnapshot: []byte(`{"status":"ready"}`),
	}
	if err := store.InsertExecution(t.Context(), db, params); err != nil {
		t.Fatal(err)
	}
	duplicate := params
	duplicate.ID = "duplicate"
	if err := store.InsertExecution(t.Context(), db, duplicate); !errors.Is(err, storageapi.ErrDuplicateKey) {
		t.Fatalf("duplicate insert error = %v", err)
	}
	for _, tt := range []struct {
		id      string
		account *uint64
		key     []byte
		kind    string
	}{
		{"null1", nil, nil, "swap"}, {"null2", nil, nil, "swap"},
		{"null_account1", nil, []byte("Key"), "swap"}, {"null_account2", nil, []byte("Key"), "swap"},
		{"null_key1", &accountID, nil, "swap"}, {"null_key2", &accountID, nil, "swap"},
		{"case", &accountID, []byte("key"), "swap"},
		{"account", &otherAccountID, []byte("Key"), "swap"},
		{"kind", &accountID, []byte("Key"), "spread"},
	} {
		value := params
		value.ID, value.AccountID, value.IdempotencyKey, value.Kind = tt.id, tt.account, tt.key, tt.kind
		if err := store.InsertExecution(t.Context(), db, value); err != nil {
			t.Fatalf("insert %s: %v", tt.id, err)
		}
	}
	for _, id := range []string{"first", "null1"} {
		leg := &LegInsertParams{ExecutionID: id, Category: LegCategoryOnchainTransaction, Status: LegStatusAwaitingSignature, Venue: "uniswap-v3"}
		if err := store.InsertLeg(t.Context(), db, leg); err != nil {
			t.Fatal(err)
		}
		if err := store.InsertOnchainTransaction(t.Context(), db, OnchainTransactionInsertParams{ExecutionLegID: leg.ID, ChainFamily: "evm", Chain: "base", Network: "sepolia", Signer: "test-signer", PayloadDigest: "test-digest"}); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		value, _, _, selectErr := store.SelectOnchainSubmissionForUpdate(t.Context(), tx, id)
		rollbackErr := tx.Rollback()
		if selectErr != nil || rollbackErr != nil {
			t.Fatalf("read execution: select=%v rollback=%v", selectErr, rollbackErr)
		}
		if !value.PreparedAt.IsZero() || !value.ExpiresAt.IsZero() || !strings.Contains(string(value.ResultSnapshot), "ready") {
			t.Fatal("nullable timestamps or result snapshot did not round trip")
		}
		if id == "first" {
			if value.AccountID == nil || *value.AccountID != accountID || !bytes.Equal(value.IdempotencyKey, params.IdempotencyKey) {
				t.Fatal("idempotency metadata did not round trip")
			}
		} else if value.AccountID != nil || value.IdempotencyKey != nil {
			t.Fatal("NULL metadata did not round trip")
		}
	}
}
