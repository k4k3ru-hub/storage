package market

import (
	"math"
	"testing"
	"time"
)

func TestFundingRateValidate(t *testing.T) {
	event := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	valid := FundingRate{
		EventTimestamp:    event,
		ReceivedTimestamp: event.Add(time.Millisecond),
		FundingTimestamp:  event.Add(time.Hour),
		Rate:              0.0001,
		Kind:              FundingRateKindCurrentEstimate,
		IntervalMinutes:   60,
	}

	tests := []struct {
		name   string
		mutate func(*FundingRate)
	}{
		{name: "valid", mutate: func(*FundingRate) {}},
		{name: "missing event timestamp", mutate: func(r *FundingRate) { r.EventTimestamp = time.Time{} }},
		{name: "missing received timestamp", mutate: func(r *FundingRate) { r.ReceivedTimestamp = time.Time{} }},
		{name: "missing funding timestamp", mutate: func(r *FundingRate) { r.FundingTimestamp = time.Time{} }},
		{name: "invalid rate", mutate: func(r *FundingRate) { r.Rate = math.NaN() }},
		{name: "invalid kind", mutate: func(r *FundingRate) { r.Kind = "unknown" }},
		{name: "invalid interval", mutate: func(r *FundingRate) { r.IntervalMinutes = 0 }},
		{name: "invalid mark price", mutate: func(r *FundingRate) { value := 0.0; r.MarkPrice = &value }},
		{name: "invalid index price", mutate: func(r *FundingRate) { value := math.Inf(1); r.IndexPrice = &value }},
		{name: "invalid premium rate", mutate: func(r *FundingRate) { value := math.NaN(); r.PremiumRate = &value }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid
			test.mutate(&record)
			err := record.Validate()
			if test.name == "valid" && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if test.name != "valid" && err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
		})
	}
}
