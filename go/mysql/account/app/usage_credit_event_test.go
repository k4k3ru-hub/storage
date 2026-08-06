//
// usage_credit_event_test.go
//
package app

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestUsageCreditEventInsertRejectsNilParams(t *testing.T) {
	store, err := NewUsageCreditEventStore(DefaultUsageCreditEventTableName, DefaultUsageCreditTableName)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Insert(context.Background(), &sql.DB{}, nil)
	if err == nil || !strings.Contains(err.Error(), "usage_credit_event_insert_params=null") {
		t.Fatalf("error = %v, want usage_credit_event_insert_params=null error", err)
	}
}

func TestUsageCreditEventRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewUsageCreditEventStore("events", "credits JOIN secrets"); err == nil {
		t.Fatal("NewUsageCreditEventStore accepted an unsafe credit table name")
	}
}
