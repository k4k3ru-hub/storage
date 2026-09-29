package oms

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	generator "github.com/k4k3ru-hub/storage/go/internal/generator"
)

type PnLSubjectType string

const (
	PnLSubjectSpotInventory PnLSubjectType = "spot_inventory"
	PnLSubjectSpotRoundTrip PnLSubjectType = "spot_round_trip"
	PnLSubjectPositionGroup PnLSubjectType = "position_group"
	maxPnLStateBytes                       = 64 * 1024
)

var pnlIDGenerator generator.ID

// PnLScope fixes the exact accounting subject and valuation unit of a checkpoint.
// Round-trip inventory also keys by AccountingAsset, its original funding unit.
// Position instrument metadata remains on the immutable representative OMS order.
type PnLScope struct {
	AccountRef      string
	SubjectType     PnLSubjectType
	PositionOrderID *uint64
	InventoryAsset  *QuantityAsset
	AccountingAsset QuantityAsset
}

// PnLCheckpoint stores only a complete order prefix, never a partially evaluated order.
// Nullable values mean unknown. State preserves calculator-owned exact precision and
// economic ordering; the decimal columns alone are not sufficient to resume calculation.
type PnLCheckpoint struct {
	LastOrderID                                                      *uint64
	RemainingQuantity, RemainingCost, AverageEntryPrice, RealizedPnL *string
	CalculationMethod                                                string
	CalculationVersion                                               uint16
	CalculationState                                                 json.RawMessage
	CalculatedAt                                                     time.Time
}

// PnL is derived accounting state, not a wallet balance or an authoritative execution record.
// NeedsRebuild=false permits prefix reuse; it does not mean newer orders were evaluated.
type PnL struct {
	ID, AccountID uint64
	PnLScope
	PnLCheckpoint
	NeedsRebuild         bool
	Version              uint64
	CreatedAt, UpdatedAt time.Time
}

type pnlDefinition struct {
	InventoryAsset  *QuantityAsset `json:"inventoryAsset,omitempty"`
	AccountingAsset QuantityAsset  `json:"accountingAsset"`
}

func validatePnLAsset(asset QuantityAsset) error {
	if err := textValue("asset_id", asset.AssetID, 65535, true); err != nil {
		return err
	}
	if asset.Decimals > 255 {
		return invalid("asset_decimals", "out_of_range")
	}
	if asset.Symbol != "" {
		if err := textValue("symbol", asset.Symbol, 128, false); err != nil {
			return err
		}
	}
	switch asset.Namespace {
	case "onchain":
		if err := textValue("chain", asset.Chain, 64, true); err != nil {
			return err
		}
		return textValue("network", asset.Network, 64, true)
	case "venue", "currency":
		if asset.Chain != "" || asset.Network != "" {
			return invalid("asset_scope", "invalid")
		}
	default:
		return invalid("asset_namespace", "invalid")
	}
	return nil
}

func (v PnLScope) validate() error {
	if err := textValue("account_ref", v.AccountRef, 128, true); err != nil {
		return err
	}
	if err := validatePnLAsset(v.AccountingAsset); err != nil {
		return err
	}
	switch v.SubjectType {
	case PnLSubjectSpotInventory, PnLSubjectSpotRoundTrip:
		if v.PositionOrderID != nil || v.InventoryAsset == nil {
			return invalid("inventory_scope", "invalid")
		}
		return validatePnLAsset(*v.InventoryAsset)
	case PnLSubjectPositionGroup:
		if v.PositionOrderID == nil || *v.PositionOrderID == 0 || v.InventoryAsset != nil {
			return invalid("position_scope", "invalid")
		}
	default:
		return invalid("subject_type", "invalid")
	}
	return nil
}

func (v PnLScope) key() ([32]byte, error) {
	// JSON array boundaries prevent concatenation ambiguity. Labels and valuation units
	// do not split one inventory into several independently mutable cost bases.
	parts := []any{v.SubjectType, v.AccountRef}
	if v.InventoryAsset != nil {
		a := v.InventoryAsset
		parts = append(parts, a.Namespace, a.Chain, a.Network, a.AssetID)
	} else {
		parts = append(parts, v.PositionOrderID)
	}
	// Round trips also identify the original funding unit; inventory identities stay unchanged.
	if v.SubjectType == PnLSubjectSpotRoundTrip {
		a := v.AccountingAsset
		parts = append(parts, a.Namespace, a.Chain, a.Network, a.AssetID)
	}
	b, err := json.Marshal(parts)
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to encode pnl identity: %w", err)
	}
	return sha256.Sum256(b), nil
}

func samePnLScope(a, b PnLScope) bool {
	return a.SubjectType == b.SubjectType && a.AccountRef == b.AccountRef && sameUint(a.PositionOrderID, b.PositionOrderID) &&
		samePnLAsset(a.InventoryAsset, b.InventoryAsset) && samePnLAsset(&a.AccountingAsset, &b.AccountingAsset)
}

func samePnLAsset(a, b *QuantityAsset) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Namespace == b.Namespace && a.Chain == b.Chain &&
		a.Network == b.Network && a.AssetID == b.AssetID && a.Decimals == b.Decimals
}

func (v PnLCheckpoint) validate(kind PnLSubjectType) error {
	if v.LastOrderID != nil && *v.LastOrderID == 0 {
		return invalid("last_order_id", "empty")
	}
	if err := validTime("calculated_at", v.CalculatedAt); err != nil {
		return err
	}
	if err := textValue("calculation_method", v.CalculationMethod, 64, true); err != nil {
		return err
	}
	if v.CalculationVersion == 0 {
		return invalid("calculation_version", "empty")
	}
	if len(v.CalculationState) > maxPnLStateBytes {
		return invalid("calculation_state", "too_long")
	}
	if err := jsonObject("calculation_state", v.CalculationState, true); err != nil {
		return err
	}
	if (kind == PnLSubjectSpotInventory || kind == PnLSubjectSpotRoundTrip) && v.AverageEntryPrice != nil || kind == PnLSubjectPositionGroup && v.RemainingCost != nil {
		return invalid("calculation_subject", "invalid")
	}
	for _, f := range []struct {
		name  string
		value *string
	}{
		{"remaining_quantity", v.RemainingQuantity}, {"remaining_cost", v.RemainingCost}, {"average_entry_price", v.AverageEntryPrice},
	} {
		if f.value != nil {
			if err := decimal(f.name, *f.value, true); err != nil {
				return err
			}
		}
	}
	if v.LastOrderID == nil {
		for _, amount := range []*string{v.RemainingQuantity, v.RemainingCost, v.AverageEntryPrice, v.RealizedPnL} {
			if amount != nil && *amount != "0" {
				return invalid("empty_prefix", "invalid")
			}
		}
	}
	if v.RealizedPnL != nil {
		if len(*v.RealizedPnL) > 384 {
			return invalid("realized_pnl", "too_long")
		}
		digits := strings.TrimPrefix(*v.RealizedPnL, "-")
		if *v.RealizedPnL == "-0" {
			return invalid("realized_pnl", "invalid")
		}
		if err := decimal("realized_pnl", digits, true); err != nil {
			return err
		}
	}
	return nil
}
