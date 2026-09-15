package execution

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestSubmissionTransitionsMySQL checks committed submission transitions and conflict rollback in MySQL.
//
// Version:
//   - 2026-09-15: Added.
func TestSubmissionTransitionsMySQL(t *testing.T) {
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
			t.Error(err)
		}
	})
	prefix := fmt.Sprintf("submission_test_%d", time.Now().UnixNano())
	store, err := NewStore(prefix, prefix+"_legs", prefix+"_transactions")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, table := range []string{prefix + "_transactions", prefix + "_legs", prefix} {
			if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
				t.Error(err)
			}
		}
	})
	if err := store.CreateTables(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, tc := range []struct {
		name             string
		status           LegStatus
		expired, missing bool
	}{
		{name: "awaiting", status: LegStatusAwaitingSignature},
		{name: "prepared", status: LegStatusPrepared},
		{name: "expired", status: LegStatusAwaitingSignature, expired: true},
		{name: "missing-onchain", status: LegStatusAwaitingSignature, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expiry := now.Add(time.Minute)
			if tc.expired {
				expiry = now.Add(-time.Second)
			}
			if err := store.InsertExecution(t.Context(), db, ExecutionInsertParams{ID: tc.name, Status: StatusPrepared, Kind: "swap", RequestSnapshot: []byte(`{}`), PreparedAt: now.Add(-time.Minute), ExpiresAt: expiry}); err != nil {
				t.Fatal(err)
			}
			leg := &LegInsertParams{ExecutionID: tc.name, Category: LegCategoryOnchainTransaction, Status: tc.status, Venue: "uniswap-v3"}
			if err := store.InsertLeg(t.Context(), db, leg); err != nil {
				t.Fatal(err)
			}
			if !tc.missing {
				if err := store.InsertOnchainTransaction(t.Context(), db, OnchainTransactionInsertParams{ExecutionLegID: leg.ID, ChainFamily: "evm", Chain: "base", Network: "sepolia", Signer: "test", PayloadDigest: "digest"}); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			err = store.MarkOnchainSubmitting(t.Context(), tx, tc.name, leg.ID, now)
			if tc.expired || tc.missing {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Fatal(rollbackErr)
				}
				if err == nil {
					t.Fatal("expected transition conflict")
				}
				var status Status
				if err := db.QueryRow("SELECT status FROM "+prefix+" WHERE id=?", tc.name).Scan(&status); err != nil {
					t.Fatal(err)
				}
				if status != StatusPrepared {
					t.Fatal("parent transition was not rolled back")
				}
				return
			}
			if err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Error(rollbackErr)
				}
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			tx, err = db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			execution, loadedLeg, onchain, err := store.SelectOnchainSubmissionForUpdate(t.Context(), tx, tc.name)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				t.Fatal(rollbackErr)
			}
			if err != nil {
				t.Fatal(err)
			}
			if execution.Status != StatusSubmitting || loadedLeg.Status != LegStatusSubmitting || onchain.SubmissionStartedAt == nil || !onchain.SubmissionStartedAt.Equal(now) {
				t.Fatal("submitting state was not persisted")
			}
			tx, err = db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.MarkOnchainSubmitted(t.Context(), tx, tc.name, leg.ID, "0xtest"+tc.name, now.Add(time.Second)); err != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Error(rollbackErr)
				}
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
