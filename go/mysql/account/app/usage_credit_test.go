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

func TestUsageCreditTypeString(t *testing.T) {
	tests := []struct {
		name string
		got  UsageCreditType
		want string
	}{
		{name: "purchased", got: UsageCreditTypePurchased, want: "purchased"},
		{name: "campaign", got: UsageCreditTypeCampaign, want: "campaign"},
		{name: "compensation", got: UsageCreditTypeCompensation, want: "compensation"},
		{name: "adjustment", got: UsageCreditTypeAdjustment, want: "adjustment"},
		{name: "invalid", got: UsageCreditType(0), want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.got.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
