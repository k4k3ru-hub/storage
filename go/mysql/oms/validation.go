package oms

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

var decimalPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]*[1-9])?$`)

func invalid(field, state string) error {
	return fmt.Errorf("%w: %s=%s", ErrInvalidParameter, field, state)
}
func textValue(field, value string, max int, ascii bool) error {
	if value == "" {
		return invalid(field, "empty")
	}
	if !utf8.ValidString(value) {
		return invalid(field, "invalid")
	}
	if utf8.RuneCountInString(value) > max {
		return invalid(field, "too_long")
	}
	if strings.TrimSpace(value) != value {
		return invalid(field, "invalid")
	}
	for _, r := range value {
		if unicode.IsControl(r) || (ascii && (r < 33 || r > 126)) {
			return invalid(field, "invalid")
		}
	}
	return nil
}
func binaryKey(field string, key []byte) error {
	if len(key) == 0 {
		return invalid(field, "empty")
	}
	if len(key) > 128 {
		return invalid(field, "too_long")
	}
	return nil
}
func decimal(field, value string, zero bool) error {
	if value == "" {
		return invalid(field, "empty")
	}
	if len(value) > 384 {
		return invalid(field, "too_long")
	}
	if !decimalPattern.MatchString(value) {
		return invalid(field, "invalid")
	}
	if !zero && value == "0" {
		return invalid(field, "out_of_range")
	}
	return nil
}
func optionalDecimal(field string, value *string) error {
	if value == nil {
		return nil
	}
	return decimal(field, *value, false)
}
func condition(kind, value *string, tp bool) error {
	if kind == nil && value == nil {
		return nil
	}
	if kind == nil || value == nil {
		return invalid("condition", "invalid")
	}
	if *kind == "price" {
		return decimal("condition_value", *value, false)
	}
	if *kind != "return_bps" {
		return invalid("condition_type", "invalid")
	}
	digits := *value
	if !tp {
		if !strings.HasPrefix(digits, "-") {
			return invalid("stop_loss_value", "invalid")
		}
		digits = digits[1:]
	}
	if err := decimal("return_bps", digits, false); err != nil {
		return err
	}
	if strings.Contains(digits, ".") {
		return invalid("return_bps", "invalid")
	}
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok || (!tp && n.Cmp(big.NewInt(10000)) > 0) {
		return invalid("return_bps", "out_of_range")
	}
	return nil
}

// Validate validates order fields without deciding a trading state transition.
//
// Version:
//   - 2026-09-20: Added.
func (o Order) Validate() error {
	if o.ID == 0 || o.AccountID == 0 {
		return invalid("identity", "empty")
	}
	if o.ParentOrderID != nil && (*o.ParentOrderID == 0 || *o.ParentOrderID == o.ID) {
		return invalid("parent_order_id", "invalid")
	}
	for _, f := range []struct {
		name, value string
		max         int
	}{
		{"account_ref", o.AccountRef, 128}, {"asset_class", o.AssetClass, 32}, {"domain", o.Domain, 64},
		{"venue", o.Venue, 64}, {"side", o.Side, 16}, {"order_type", o.OrderType, 32},
	} {
		if err := textValue(f.name, f.value, f.max, true); err != nil {
			return err
		}
	}
	if err := textValue("symbol", o.Symbol, 128, false); err != nil {
		return err
	}
	if err := o.OrderState.Validate(); err != nil {
		return err
	}
	if err := optionalDecimal("limit_price", o.LimitPrice); err != nil {
		return err
	}
	if err := condition(o.TakeProfitType, o.TakeProfitValue, true); err != nil {
		return err
	}
	if err := condition(o.StopLossType, o.StopLossValue, false); err != nil {
		return err
	}
	if err := binaryKey("idempotency_key", o.IdempotencyKey); err != nil {
		return err
	}
	if o.ExpiresAt != nil && o.ExpiresAt.IsZero() {
		return invalid("expires_at", "invalid")
	}
	return nil
}

// Validate validates a caller-supplied order snapshot without computing fills or transitions.
//
// Version:
//   - 2026-09-20: Added.
func (s OrderState) Validate() error {
	switch s.Status {
	case OrderStatusPending, OrderStatusProcessing, OrderStatusPartiallyFilled, OrderStatusFilled, OrderStatusCanceled, OrderStatusExpired, OrderStatusFailed, OrderStatusRejected:
	default:
		return invalid("status", "invalid")
	}
	if err := optionalDecimal("quantity", s.Quantity); err != nil {
		return err
	}
	if err := decimal("filled_quantity", s.FilledQuantity, true); err != nil {
		return err
	}
	if s.CompletedAt != nil && s.CompletedAt.IsZero() {
		return invalid("completed_at", "invalid")
	}
	return nil
}

// Validate validates the chain-neutral AMM swap storage fields, not onchain identity.
//
// Version:
//   - 2026-09-20: Added.
func (s OnchainAMMPoolSwap) Validate() error {
	if s.OrderID == 0 {
		return invalid("order_id", "empty")
	}
	for _, f := range []struct {
		name, value string
		max         int
	}{
		{"chain_family", s.ChainFamily, 16}, {"chain", s.Chain, 64}, {"network", s.Network, 64},
		{"pool_id", s.PoolID, 128}, {"token_in_id", s.TokenInID, 65535}, {"token_out_id", s.TokenOutID, 65535},
		{"signer", s.Signer, 128}, {"recipient", s.Recipient, 128},
	} {
		if err := textValue(f.name, f.value, f.max, true); err != nil {
			return err
		}
	}
	if s.TokenInID == s.TokenOutID {
		return invalid("tokens", "invalid")
	}
	if s.SwapKind != "exact-input" && s.SwapKind != "exact-output" {
		return invalid("swap_kind", "invalid")
	}
	if s.MaximumSlippageBPS > 10000 {
		return invalid("maximum_slippage_bps", "out_of_range")
	}
	if s.ExecutionTTLMS == 0 {
		return invalid("execution_ttl_ms", "empty")
	}
	return nil
}

// Validate validates a stage record; repeated fills and transition decisions belong to the caller.
//
// Version:
//   - 2026-09-20: Added.
func (e Execution) Validate() error {
	if e.ID == 0 || e.OrderID == 0 || e.AttemptNumber == 0 {
		return invalid("identity", "empty")
	}
	switch e.ExecType {
	case ExecutionTypePrepared, ExecutionTypeSubmitted, ExecutionTypeFilled:
	default:
		return invalid("exec_type", "invalid")
	}
	switch e.Purpose {
	case ExecutionPurposeApproval, ExecutionPurposeTrade:
	default:
		return invalid("purpose", "invalid")
	}
	switch e.Status {
	case ExecutionStatusPending, ExecutionStatusSucceeded, ExecutionStatusFailed, ExecutionStatusReversed, ExecutionStatusRejected:
	default:
		return invalid("status", "invalid")
	}
	if err := binaryKey("record_key", e.RecordKey); err != nil {
		return err
	}
	if err := textValue("execution_system", e.ExecutionSystem, 64, true); err != nil {
		return err
	}
	if e.ExecutionID != nil {
		if err := textValue("execution_id", *e.ExecutionID, 64, true); err != nil {
			return err
		}
	}
	if e.OccurredAt.IsZero() {
		return invalid("occurred_at", "empty")
	}
	if e.ExecType == ExecutionTypeFilled {
		if e.Purpose != ExecutionPurposeTrade || e.Quantity == nil || e.CounterQuantity == nil {
			return invalid("filled", "invalid")
		}
		if err := decimal("quantity", *e.Quantity, false); err != nil {
			return err
		}
		if err := decimal("counter_quantity", *e.CounterQuantity, false); err != nil {
			return err
		}
	} else if e.Quantity != nil || e.CounterQuantity != nil {
		return invalid("quantity", "invalid")
	}
	if e.ExpiresAt != nil && (e.ExecType != ExecutionTypePrepared || e.ExpiresAt.IsZero()) {
		return invalid("expires_at", "invalid")
	}
	return nil
}
