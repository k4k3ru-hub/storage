//
// product_test.go
//
package app

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func TestValidateProductNameAccepts128Characters(t *testing.T) {
	if err := ValidateProductName(strings.Repeat("a", 128)); err != nil {
		t.Fatalf("ValidateProductName() error = %v", err)
	}
}

func TestValidateProductNameRejects129Characters(t *testing.T) {
	err := ValidateProductName(strings.Repeat("a", 129))
	if err == nil || !strings.Contains(err.Error(), "name=too_long") {
		t.Fatalf("ValidateProductName() error = %v, want name=too_long error", err)
	}
}

func TestValidateProductMetaDataRejectsInvalidJSON(t *testing.T) {
	metaData := "{"
	err := ValidateProductMetaData(&metaData)
	if err == nil || !strings.Contains(err.Error(), "meta_data=invalid") {
		t.Fatalf("ValidateProductMetaData() error = %v, want meta_data=invalid error", err)
	}
}

func TestProductSelectParamsBuildQuery(t *testing.T) {
	status := ProductStatusActive
	typeNE := ProductTypeSystem
	params := ProductSelectParams{
		Status:  &status,
		TypeNE:  &typeNE,
		OrderBy: ColID,
		Limit:   20,
		Offset:  40,
	}

	query, args := params.BuildQuery("SELECT * FROM account_app_products")
	wantQuery := "SELECT * FROM account_app_products WHERE status=? AND type!=? ORDER BY id LIMIT ? OFFSET ?"
	wantArgs := []any{ProductStatusActive, ProductTypeSystem, 20, 40}
	if query != wantQuery {
		t.Fatalf("BuildQuery() query = %q, want %q", query, wantQuery)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("BuildQuery() args = %#v, want %#v", args, wantArgs)
	}
}

func TestProductTypeValues(t *testing.T) {
	tests := []struct {
		name string
		got  ProductType
		want ProductType
	}{
		{name: "system", got: ProductTypeSystem, want: 0},
		{name: "general", got: ProductTypeGeneral, want: 1},
		{name: "campaign", got: ProductTypeCampaign, want: 2},
		{name: "trial", got: ProductTypeTrial, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("product type = %d, want %d", tt.got, tt.want)
			}
		})
	}
}

func TestProductSelectParamsRejectsEqualTypeFilters(t *testing.T) {
	productType := ProductTypeCampaign
	params := ProductSelectParams{
		Type:   &productType,
		TypeNE: &productType,
	}

	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "product_type_filter=invalid") {
		t.Fatalf("Validate() error = %v, want product_type_filter=invalid error", err)
	}
}

func TestProductInsertRejectsNilParams(t *testing.T) {
	store, err := NewProductStore(DefaultProductTableName)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Insert(context.Background(), &sql.DB{}, nil)
	if err == nil || !strings.Contains(err.Error(), "product_insert_params=null") {
		t.Fatalf("Insert() error = %v, want product_insert_params=null error", err)
	}
}

func TestProductRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewProductStore("products; DROP TABLE products"); err == nil {
		t.Fatal("NewProductStore() accepted an unsafe table name")
	}
}
