package market

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k4k3ru-hub/storage/go/parquet/client"
	"github.com/k4k3ru-hub/storage/go/parquet/dataset"
	"github.com/k4k3ru-hub/storage/go/parquet/store/local"
)

func observationContext(network, run string) ObservationContext {
	id := strings.Repeat("a", 64)
	return ObservationContext{Network: &network, SourceID: &id, CollectionRunID: &run}
}
func observationFunding() FundingRate {
	return FundingRate{EventTimestamp: time.Unix(1_800_000_000, 0).UTC(), ReceivedTimestamp: time.Unix(1_800_000_000, 0).UTC(), Kind: FundingRateKindCurrentEstimate, IntervalMinutes: 60, Rate: -0.00001}
}

// TestObservationCodecsPreserveLegacyAndRejectIncomplete checks backward reads without changing legacy encoders.
//
// Version:
//   - 2026-11-02: Added.
func TestObservationCodecsPreserveLegacyAndRejectIncomplete(t *testing.T) {
	ctx := t.Context()
	value := FundingRateObservation{Value: observationFunding(), ObservationContext: observationContext("mainnet", "run-one")}
	codec := NewFundingRateObservationCodec()
	var b bytes.Buffer
	if err := codec.Encode(ctx, &b, []FundingRateObservation{value}); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(ctx, bytes.NewReader(b.Bytes()), int64(b.Len()))
	if err != nil || !reflect.DeepEqual(got, []FundingRateObservation{value}) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	var legacy bytes.Buffer
	if err := NewFundingRateCodec().Encode(ctx, &legacy, []FundingRate{value.Value}); err != nil {
		t.Fatal(err)
	}
	got, err = codec.Decode(ctx, bytes.NewReader(legacy.Bytes()), int64(legacy.Len()))
	if err != nil || len(got) != 1 || got[0].Network != nil || got[0].SourceID != nil || got[0].CollectionRunID != nil || !reflect.DeepEqual(got[0].Value, value.Value) {
		t.Fatalf("legacy unknown: %+v %v", got, err)
	}
	var invalid bytes.Buffer
	if err := codec.Encode(ctx, &invalid, got); err == nil {
		t.Fatal("legacy context was writable as v2")
	}
	oi := OpenInterest{EventTimestamp: value.Value.EventTimestamp, ReceivedTimestamp: value.Value.ReceivedTimestamp, RawQuantity: 0, RawUnit: OpenInterestUnitBaseAsset, NotionalCurrency: "USDC"}
	o := OpenInterestObservation{Value: oi, ObservationContext: value.ObservationContext}
	var ob bytes.Buffer
	oc := NewOpenInterestObservationCodec()
	if err := oc.Encode(ctx, &ob, []OpenInterestObservation{o}); err != nil {
		t.Fatal(err)
	}
	back, err := oc.Decode(ctx, bytes.NewReader(ob.Bytes()), int64(ob.Len()))
	if err != nil || !reflect.DeepEqual(back, []OpenInterestObservation{o}) {
		t.Fatalf("oi round trip: %+v %v", back, err)
	}
	var oldOI bytes.Buffer
	if err := NewOpenInterestCodec().Encode(ctx, &oldOI, []OpenInterest{oi}); err != nil {
		t.Fatal(err)
	}
	back, err = oc.Decode(ctx, bytes.NewReader(oldOI.Bytes()), int64(oldOI.Len()))
	if err != nil || len(back) != 1 || back[0].Network != nil || !reflect.DeepEqual(back[0].Value, oi) {
		t.Fatalf("oi legacy: %+v %v", back, err)
	}
}

// TestObservationCompactionScopesAndPublication proves context isolation and failure-safe predelete publication.
//
// Version:
//   - 2026-11-02: Added.
func TestObservationCompactionScopesAndPublication(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "rollback"}[fail], func(t *testing.T) {
			ctx := context.Background()
			objects, err := local.New(local.Params{Root: t.TempDir(), FileMode: 0600})
			if err != nil {
				t.Fatal(err)
			}
			c, err := client.New(client.Params{Store: objects})
			if err != nil {
				t.Fatal(err)
			}
			called := false
			sentinel := errors.New("publication unavailable")
			d, err := NewFundingRateObservationDataset(c, FundingRateObservationDatasetParams{Root: "market-data/funding-rates-v2", BeforeCompactionInputDeletion: func(ctx context.Context, result dataset.CompactResult) error {
				called = true
				for _, key := range append(append([]string{}, result.InputFiles...), result.OutputFiles...) {
					o, e := objects.Open(ctx, key)
					if e != nil {
						t.Fatalf("not present at publication: %s: %v", key, e)
					}
					if e = o.Close(); e != nil {
						t.Fatal(e)
					}
				}
				if fail {
					return sentinel
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			p := dataset.Partition{"asset_class": "crypto", "venue": "hyperliquid", "instrument_type": "perpetual", "symbol": "SUI-USDC", "date": "2027-01-15", "hour": "08"}
			contexts := []ObservationContext{observationContext("mainnet", "one"), observationContext("mainnet", "one"), observationContext("testnet", "two"), observationContext("mainnet", "three")}
			var inputs []string
			for _, scope := range contexts {
				r, e := d.Write(ctx, dataset.WriteParams[FundingRateObservation]{Partition: p, Records: []FundingRateObservation{{Value: observationFunding(), ObservationContext: scope}}})
				if e != nil {
					t.Fatal(e)
				}
				inputs = append(inputs, r.Key)
			}
			result, err := d.Compact(ctx, dataset.CompactParams{Partition: p, TargetFileSizeBytes: 1 << 20})
			if !called {
				t.Fatal("publication hook missing")
			}
			if fail {
				if !errors.Is(err, sentinel) {
					t.Fatalf("missing error chain: %v", err)
				}
				for _, key := range inputs {
					o, e := objects.Open(ctx, key)
					if e != nil {
						t.Fatal("input lost", e)
					}
					if e = o.Close(); e != nil {
						t.Fatal(e)
					}
				}
				return
			}
			if err != nil || result.OutputRows != 3 || result.DeduplicatedRows != 1 {
				t.Fatalf("context dedup: %+v %v", result, err)
			}
			read, err := d.Read(ctx, dataset.ReadParams{Partition: p})
			if err != nil || len(read.Records) != 3 {
				t.Fatalf("read: %+v %v", read, err)
			}
		})
	}
}
