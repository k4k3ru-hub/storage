// product_test.go
package app

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
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

func TestProductTypeString(t *testing.T) {
	tests := []struct {
		name        string
		productType ProductType
		want        string
	}{
		{name: "system", productType: ProductTypeSystem, want: "system"},
		{name: "general", productType: ProductTypeGeneral, want: "general"},
		{name: "campaign", productType: ProductTypeCampaign, want: "campaign"},
		{name: "trial", productType: ProductTypeTrial, want: "trial"},
		{name: "unknown", productType: ProductType(255), want: "unknown"},
	}

	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.productType.String(); got != testCase.want {
				t.Fatalf("String() = %q, want %q", got, testCase.want)
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

func TestProductTotalCreditSQL(t *testing.T) {
	store, err := NewProductStore(DefaultProductTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &usageBalanceExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(executor.query, "bonus_ticks") || strings.Contains(executor.query, "%!") || !strings.Contains(executor.query, "credit_ticks BIGINT UNSIGNED") {
		t.Fatalf("invalid product DDL: %s", executor.query)
	}
	created := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	params := &ProductInsertParams{ID: 1002, Name: "usdc-base-sepolia-10", Status: ProductStatusActive, Type: ProductTypeCampaign, CreditTicks: 12500000, PriceAmount: 10, PriceCurrency: PriceCurrencyUSD, ExpiresInDays: 30, CreatedAt: created}
	if err := store.Insert(context.Background(), executor, params); err != nil {
		t.Fatal(err)
	}
	wantQuery := "INSERT INTO account_app_products (id, name, status, type, credit_ticks, price_amount, price_currency, expires_in_days, purchase_limit, description, meta_data, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
	wantArgs := []any{uint64(1002), params.Name, ProductStatusActive, ProductTypeCampaign, uint64(12500000), uint64(10), PriceCurrencyUSD, uint32(30), uint32(0), (*string)(nil), (*string)(nil), created}
	if executor.query != wantQuery || !reflect.DeepEqual(executor.args, wantArgs) {
		t.Fatalf("insert query=%q args=%#v", executor.query, executor.args)
	}
	total, price := uint64(150000000), uint64(100)
	assignments, args := (ProductUpdateParams{CreditTicks: &total, PriceAmount: &price}).BuildAssignments()
	if !reflect.DeepEqual(assignments, []string{"credit_ticks=?", "price_amount=?"}) || !reflect.DeepEqual(args, []any{total, price}) {
		t.Fatalf("update assignments=%v args=%v", assignments, args)
	}
}

func TestProductTotalCreditFilters(t *testing.T) {
	min, max := uint64(12500000), uint64(150000000)
	params := ProductSelectParams{CreditTicksGTE: &min, CreditTicksLTE: &max, OrderBy: ColCreditTicks}
	if err := params.Validate(); err != nil {
		t.Fatal(err)
	}
	query, args := params.BuildQuery("SELECT " + productSelectColumns + " FROM account_app_products")
	if strings.Contains(query, "bonus_ticks") || !strings.HasSuffix(query, "WHERE credit_ticks>=? AND credit_ticks<=? ORDER BY credit_ticks") || !reflect.DeepEqual(args, []any{min, max}) {
		t.Fatalf("query=%q args=%v", query, args)
	}
	params.CreditTicksGTE, params.CreditTicksLTE = &max, &min
	if err := params.Validate(); err == nil {
		t.Fatal("accepted inverted credit range")
	}
	if err := (ProductSelectParams{OrderBy: "bonus_ticks"}).Validate(); err == nil {
		t.Fatal("accepted removed bonus column")
	}
}

type productValueScanner []any

// Scan assigns test row values to the product scanner destinations.
//
// Version:
//   - 2026-09-10: Added.
func (s productValueScanner) Scan(dest ...any) error {
	if len(dest) != len(s) {
		return fmt.Errorf("failed to scan test product: column_count=%d expected=%d", len(dest), len(s))
	}
	for i, v := range s {
		target := reflect.ValueOf(dest[i]).Elem()
		value := reflect.ValueOf(v)
		if !value.Type().AssignableTo(target.Type()) {
			return fmt.Errorf("failed to scan test product: column=%d", i)
		}
		target.Set(value)
	}
	return nil
}

func TestScanProductTotalCredits(t *testing.T) {
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	description, metadata := "Includes purchase incentive", `{"Chain":"base"}`
	want := Product{ID: 1002, Name: "usdc-base-sepolia-10", Status: ProductStatusActive, Type: ProductTypeCampaign, CreditTicks: 12500000, PriceAmount: 10, PriceCurrency: PriceCurrencyUSD, ExpiresInDays: 30, PurchaseLimit: 2, Description: &description, MetaData: &metadata, CreatedAt: now, UpdatedAt: now.Add(time.Hour)}
	values := productValueScanner{want.ID, want.Name, want.Status, want.Type, want.CreditTicks, want.PriceAmount, want.PriceCurrency, want.ExpiresInDays, want.PurchaseLimit, want.Description, want.MetaData, want.CreatedAt, want.UpdatedAt}
	if len(strings.Split(productSelectColumns, ", ")) != len(values) {
		t.Fatal("SELECT and scan column counts differ")
	}
	var got Product
	if err := scanProduct(values, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scan = %#v, want %#v", got, want)
	}
}
