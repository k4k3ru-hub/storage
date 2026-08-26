// additional_datasets_test.go
package market

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store/local"
)

func TestAdditionalDatasetsWriteRead(t *testing.T) {
	event := time.Date(2026, 8, 16, 12, 34, 56, 789000000, time.UTC)
	received := event.Add(time.Millisecond)
	partition := dataset.Partition{"asset_class": "crypto", "venue": "hyperliquid", "instrument_type": "perpetual", "symbol": "BTC-USDC", "date": "2026-08-16", "hour": "12"}

	tests := []struct {
		name string
		run  func(*testing.T, *client.Client)
	}{
		{"trade", func(t *testing.T, c *client.Client) {
			d, err := NewTradeDataset(c, TradeDatasetParams{Root: "market-data/trades", FileName: "trades"})
			if err != nil {
				t.Fatal(err)
			}
			want := []Trade{{event, received, "trade-1", "buy", 118000.25, 1.5}}
			result, err := d.Write(context.Background(), TradeWriteParams{Partition: partition, Records: want})
			if err != nil {
				t.Fatal(err)
			}
			if result.Key != "market-data/trades/asset_class=crypto/venue=hyperliquid/instrument_type=perpetual/symbol=BTC-USDC/date=2026-08-16/hour=12/trades.parquet" {
				t.Fatalf("key = %q", result.Key)
			}
			got, err := d.Read(context.Background(), TradeReadParams{Partition: partition})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Records, want) {
				t.Fatalf("records = %+v, want %+v", got.Records, want)
			}
		}},
		{"open_interest", func(t *testing.T, c *client.Client) {
			d, err := NewOpenInterestDataset(c, OpenInterestDatasetParams{Root: "open-interest", FileName: "part"})
			if err != nil {
				t.Fatal(err)
			}
			conversionPrice := 117664.19
			want := []OpenInterest{{
				EventTimestamp:      event,
				ReceivedTimestamp:   received,
				RawQuantity:         23456.75,
				RawUnit:             OpenInterestUnitBaseAsset,
				Quantity:            23456.75,
				NotionalValue:       2760000000,
				NotionalCurrency:    "USDC",
				ConversionPrice:     &conversionPrice,
				ConversionPriceType: OpenInterestPriceTypeMark,
			}}
			if _, err := d.Write(context.Background(), OpenInterestWriteParams{Partition: partition, Records: want}); err != nil {
				t.Fatal(err)
			}
			got, err := d.Read(context.Background(), OpenInterestReadParams{Partition: partition})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Records, want) {
				t.Fatalf("records = %+v, want %+v", got.Records, want)
			}
		}},
		{"funding_rate", func(t *testing.T, c *client.Client) {
			d, err := NewFundingRateDataset(c, FundingRateDatasetParams{Root: "funding", FileName: "part"})
			if err != nil {
				t.Fatal(err)
			}
			markPrice := 118001.0
			indexPrice := 117995.0
			premiumRate := 0.00005
			want := []FundingRate{{
				EventTimestamp:    event,
				ReceivedTimestamp: received,
				FundingTimestamp:  event.Add(time.Hour),
				Rate:              0.0001,
				Kind:              FundingRateKindCurrentEstimate,
				IntervalMinutes:   60,
				MarkPrice:         &markPrice,
				IndexPrice:        &indexPrice,
				PremiumRate:       &premiumRate,
			}}
			if _, err := d.Write(context.Background(), FundingRateWriteParams{Partition: partition, Records: want}); err != nil {
				t.Fatal(err)
			}
			got, err := d.Read(context.Background(), FundingRateReadParams{Partition: partition})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Records, want) {
				t.Fatalf("records = %+v, want %+v", got.Records, want)
			}
		}},
		{"liquidation", func(t *testing.T, c *client.Client) {
			d, err := NewLiquidationDataset(c, LiquidationDatasetParams{Root: "liquidations", FileName: "part"})
			if err != nil {
				t.Fatal(err)
			}
			want := []Liquidation{{event, received, "liq-1", "sell", 117500, 3.25}}
			if _, err := d.Write(context.Background(), LiquidationWriteParams{Partition: partition, Records: want}); err != nil {
				t.Fatal(err)
			}
			got, err := d.Read(context.Background(), LiquidationReadParams{Partition: partition})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Records, want) {
				t.Fatalf("records = %+v, want %+v", got.Records, want)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := local.New(local.Params{Root: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			c, err := client.New(client.Params{Store: store})
			if err != nil {
				t.Fatal(err)
			}
			test.run(t, c)
		})
	}
}
