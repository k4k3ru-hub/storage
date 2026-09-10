package app

import (
	"context"
	"database/sql"
	"errors"
	"github.com/go-sql-driver/mysql"
	api "github.com/k4k3ru-hub/storage/go/api"
	"strings"
	"testing"
	"time"
)

type paymentGrantExecutor struct {
	usageBalanceExecutorStub
	err error
}

// ExecContext records statements and returns the configured error.
//
// Version:
//   - 2026-09-10: Added.
func (e *paymentGrantExecutor) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	e.query = q
	e.args = args
	return nil, e.err
}

func TestPaymentGrantStoreIdentityAndInsert(t *testing.T) {
	store, err := NewPaymentGrantStore(DefaultPaymentGrantTableName)
	if err != nil {
		t.Fatal(err)
	}
	db := &paymentGrantExecutor{}
	if err := store.CreateTable(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(db.query, "PRIMARY KEY (source,intent_kind,intent_id)") {
		t.Fatal("missing payment uniqueness constraint")
	}
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	expiry := now.AddDate(0, 0, 30)
	p := PaymentGrant{PaymentGrantKey: PaymentGrantKey{Source: "payment", IntentKind: "onchain", IntentID: 10}, AccountID: 20, CreditTicks: 12500000, ExpiresInDays: 30, ExpiresAt: &expiry, CreditID: 30, OperationID: 40, CreatedAt: now}
	if err := store.Insert(t.Context(), db, p); err != nil {
		t.Fatal(err)
	}
	if len(db.args) != 10 || db.args[2] != uint64(10) || db.args[4] != uint64(12500000) || db.args[7] != uint64(30) {
		t.Fatalf("incorrect insert arguments: %#v", db.args)
	}
	duplicate := &mysql.MySQLError{Number: 1062}
	db.err = duplicate
	err = store.Insert(t.Context(), db, p)
	if !errors.Is(err, api.ErrDuplicateKey) || !errors.Is(err, duplicate) {
		t.Fatalf("duplicate chain lost: %v", err)
	}
	p.ExpiresAt = nil
	if err := store.Insert(t.Context(), db, p); err == nil {
		t.Fatal("accepted inconsistent expiration")
	}
}
func TestPaymentGrantKeyValidation(t *testing.T) {
	for _, key := range []PaymentGrantKey{{}, {Source: "Payment", IntentKind: "onchain", IntentID: 1}, {Source: "payment", IntentKind: "onchain"}, {Source: strings.Repeat("a", 33), IntentKind: "onchain", IntentID: 1}, {Source: "payment", IntentKind: "onchain'", IntentID: 1}} {
		if err := key.Validate(); err == nil {
			t.Fatalf("accepted invalid key: %#v", key)
		}
	}
	if _, err := NewPaymentGrantStore("grants; DROP TABLE grants"); err == nil {
		t.Fatal("accepted unsafe table")
	}
}
