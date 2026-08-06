//
// usage_credit_test.go
//
package app

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestUsageCreditInsertRejectsNilParams(t *testing.T) {
	store, err := NewUsageCreditStore(DefaultUsageCreditTableName)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Insert(context.Background(), &sql.DB{}, nil)
	if err == nil || !strings.Contains(err.Error(), "usage_credit_insert_params=null") {
		t.Fatalf("error = %v, want usage_credit_insert_params=null error", err)
	}
}

func TestUsageCreditRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewUsageCreditStore("credits; DROP TABLE credits"); err == nil {
		t.Fatal("NewUsageCreditStore accepted an unsafe table name")
	}
}
