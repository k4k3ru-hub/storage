package market

import (
	"math"
	"testing"
	"time"
)

// TestOpenInterestValidate verifies normalized open-interest invariants.
//
// Version:
//   - 2026-08-26: Added.
func TestOpenInterestValidate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	conversionPrice := 118000.25
	contractSize := 0.001
	contractSizeUnit := ContractSizeUnitBaseAsset
	contractSizeCurrency := "BTC"
	valid := OpenInterest{
		EventTimestamp:           now,
		ReceivedTimestamp:        now.Add(time.Millisecond),
		RawQuantity:              1000,
		RawUnit:                  OpenInterestUnitContracts,
		Quantity:                 1,
		NotionalValue:            118000.25,
		NotionalCurrency:         "USDT",
		ConversionPrice:          &conversionPrice,
		ConversionPriceType:      OpenInterestPriceTypeMark,
		ConversionPriceTimestamp: &now,
		ContractSize:             &contractSize,
		ContractSizeUnit:         &contractSizeUnit,
		ContractSizeCurrency:     &contractSizeCurrency,
	}

	tests := []struct {
		name    string
		mutate  func(*OpenInterest)
		wantErr bool
	}{
		{name: "valid"},
		{name: "zero open interest", mutate: func(r *OpenInterest) {
			r.RawQuantity = 0
			r.Quantity = 0
			r.NotionalValue = 0
		}},
		{name: "event timestamp missing", mutate: func(r *OpenInterest) { r.EventTimestamp = time.Time{} }, wantErr: true},
		{name: "received timestamp missing", mutate: func(r *OpenInterest) { r.ReceivedTimestamp = time.Time{} }, wantErr: true},
		{name: "raw quantity negative", mutate: func(r *OpenInterest) { r.RawQuantity = -1 }, wantErr: true},
		{name: "raw quantity non-finite", mutate: func(r *OpenInterest) { r.RawQuantity = math.NaN() }, wantErr: true},
		{name: "raw unit invalid", mutate: func(r *OpenInterest) { r.RawUnit = "unknown" }, wantErr: true},
		{name: "quantity negative", mutate: func(r *OpenInterest) { r.Quantity = -1 }, wantErr: true},
		{name: "notional non-finite", mutate: func(r *OpenInterest) { r.NotionalValue = math.Inf(1) }, wantErr: true},
		{name: "notional currency missing", mutate: func(r *OpenInterest) { r.NotionalCurrency = " " }, wantErr: true},
		{name: "conversion type without price", mutate: func(r *OpenInterest) { r.ConversionPrice = nil }, wantErr: true},
		{name: "conversion price without type", mutate: func(r *OpenInterest) { r.ConversionPriceType = "" }, wantErr: true},
		{name: "conversion price without timestamp", mutate: func(r *OpenInterest) { r.ConversionPriceTimestamp = nil }, wantErr: true},
		{name: "conversion timestamp without price", mutate: func(r *OpenInterest) {
			r.ConversionPrice = nil
			r.ConversionPriceType = ""
		}, wantErr: true},
		{name: "conversion price non-positive", mutate: func(r *OpenInterest) {
			value := 0.0
			r.ConversionPrice = &value
		}, wantErr: true},
		{name: "contract size non-positive", mutate: func(r *OpenInterest) {
			value := -1.0
			r.ContractSize = &value
		}, wantErr: true},
		{name: "contract size without unit", mutate: func(r *OpenInterest) { r.ContractSizeUnit = nil }, wantErr: true},
		{name: "contract size without currency", mutate: func(r *OpenInterest) { r.ContractSizeCurrency = nil }, wantErr: true},
		{name: "contracts without contract size", mutate: func(r *OpenInterest) {
			r.ContractSize = nil
			r.ContractSizeUnit = nil
			r.ContractSizeCurrency = nil
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := valid
			if tt.mutate != nil {
				tt.mutate(&record)
			}
			err := record.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}
