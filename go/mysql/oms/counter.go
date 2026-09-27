package oms

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

type QuantityAsset struct {
	Namespace string `json:"namespace"`
	Chain     string `json:"chain,omitempty"`
	Network   string `json:"network,omitempty"`
	AssetID   string `json:"assetId"`
	Symbol    string `json:"symbol"`
	Decimals  uint16 `json:"decimals"`
}

// CounterQuantityAsset reads the chain-neutral counter unit from immutable order conditions.
// Missing or null metadata disables aggregation, including before the first fill.
//
// Version:
//   - 2026-09-27: Added.
func (o Order) CounterQuantityAsset() (*QuantityAsset, error) {
	var envelope struct {
		Asset *QuantityAsset `json:"counterQuantityAsset"`
	}
	if err := json.Unmarshal(o.Specification, &envelope); err != nil {
		return nil, fmt.Errorf("failed to read oms counter asset: %w", err)
	}
	a := envelope.Asset
	if a == nil {
		return nil, nil
	}
	for _, f := range []struct {
		name, value string
		max         int
	}{
		{"namespace", a.Namespace, 32}, {"asset_id", a.AssetID, 65535},
	} {
		if err := textValue(f.name, f.value, f.max, true); err != nil {
			return nil, fmt.Errorf("failed to read oms counter asset: %w", err)
		}
	}
	if a.Decimals > 255 {
		return nil, invalid("counter_decimals", "out_of_range")
	}
	if a.Namespace == "onchain" {
		if err := textValue("chain", a.Chain, 64, true); err != nil {
			return nil, err
		}
		if err := textValue("network", a.Network, 64, true); err != nil {
			return nil, err
		}
	} else if a.Chain != "" || a.Network != "" {
		return nil, invalid("counter_asset_scope", "invalid")
	}
	return a, nil
}

func validateCounterContribution(asset *QuantityAsset, e Event) error {
	if e.OrderCounterQuantity == nil {
		return nil
	}
	if asset == nil {
		return fmt.Errorf("failed to validate counter contribution: %w: counter_asset=null", ErrConflict)
	}
	if err := unitDecimal("order_counter_quantity", *e.OrderCounterQuantity, asset.Decimals, false); err != nil {
		return err
	}
	if *e.OrderCounterQuantity == "0" {
		return nil
	}
	if e.QuantityAssetID != nil && *e.QuantityAssetID == asset.AssetID && *e.QuantityDecimals == asset.Decimals {
		return nil
	}
	if e.CounterAssetID != nil && *e.CounterAssetID == asset.AssetID && *e.CounterDecimals == asset.Decimals {
		return nil
	}
	return fmt.Errorf("failed to validate counter contribution: %w: counter_asset=mismatch", ErrConflict)
}

func sumCounter(asset *QuantityAsset, fills []Event, executionID uint64) (*string, error) {
	if asset == nil {
		return nil, nil
	}
	total := new(big.Rat)
	for _, f := range fills {
		if executionID != 0 && f.ExecutionRecordID != executionID {
			continue
		}
		if f.OrderCounterQuantity == nil {
			return nil, nil
		}
		n, ok := new(big.Rat).SetString(*f.OrderCounterQuantity)
		if !ok {
			return nil, invalid("order_counter_quantity", "invalid")
		}
		total.Add(total, n)
	}
	value := total.FloatString(int(asset.Decimals))
	if asset.Decimals > 0 {
		value = strings.TrimRight(strings.TrimRight(value, "0"), ".")
	}
	if err := unitDecimal("filled_counter_quantity", value, asset.Decimals, false); err != nil {
		return nil, err
	}
	return &value, nil
}
