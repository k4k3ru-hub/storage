package oms

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"
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

func optionalText(field string, value *string, max int) error {
	if value == nil {
		return nil
	}
	return textValue(field, *value, max, true)
}
func validTime(field string, value time.Time) error {
	if value.IsZero() {
		return invalid(field, "empty")
	}
	if value.UTC().Year() < 1000 || value.UTC().Year() > 9999 {
		return invalid(field, "out_of_range")
	}
	return nil
}
func jsonObject(field string, value json.RawMessage, required bool) error {
	if len(value) == 0 && !required {
		return nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(value, &object); err != nil || object == nil {
		return invalid(field, "invalid")
	}
	return nil
}
func unitDecimal(field, value string, decimals uint16, signed bool) error {
	if decimals > 255 {
		return invalid("asset_decimals", "out_of_range")
	}
	if len(value) > 384 {
		return invalid(field, "too_long")
	}
	digits := value
	if signed && strings.HasPrefix(value, "-") {
		digits = value[1:]
		if digits == "0" {
			return invalid(field, "invalid")
		}
	}
	if err := decimal(field, digits, true); err != nil {
		return err
	}
	if i := strings.IndexByte(digits, '.'); i >= 0 && len(digits)-i-1 > int(decimals) {
		return invalid(field, "out_of_range")
	}
	return nil
}
func isFill(t EventType) bool   { return t == EventTypeFilled || t == EventTypeFillCorrected }
func isResult(t EventType) bool { return t == EventTypeSucceeded || t == EventTypeFailed }
func isOrderFact(t EventType) bool {
	return t == EventTypeOrderCanceled || t == EventTypeOrderExpired || t == EventTypeOrderRejected || t == EventTypeOrderFailed
}
func needsReference(t EventType) bool {
	switch t {
	case EventTypeReversed, EventTypeFillReversed, EventTypeFillCorrected, EventTypeFeesRecorded, EventTypeFeesAdjusted, EventTypeEvidenceRecorded:
		return true
	}
	return false
}

// Validate validates the generic order snapshot and original specification envelope.
// Protocol-specific specification validation belongs to the submitting adapter.
//
// Version:
//   - 2026-09-26: Validate the new snapshot schema.
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
		{"account_ref", o.AccountRef, 128}, {"asset_class", o.AssetClass, 32}, {"domain", o.Domain, 64}, {"side", o.Side, 16}, {"order_type", o.OrderType, 32},
	} {
		if err := textValue(f.name, f.value, f.max, true); err != nil {
			return err
		}
	}
	if err := optionalText("venue", o.Venue, 64); err != nil {
		return err
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
	if o.ExpiresAt != nil {
		if err := validTime("expires_at", *o.ExpiresAt); err != nil {
			return err
		}
	}
	if o.SpecificationVersion == 0 {
		return invalid("specification_version", "empty")
	}
	return jsonObject("specification", o.Specification, true)
}

// Validate validates snapshot values without inferring completion from a receipt.
//
// Version:
//   - 2026-09-26: Remove processing and include the history sequence.
func (s OrderState) Validate() error {
	switch s.Status {
	case OrderStatusPending, OrderStatusPartiallyFilled, OrderStatusFilled, OrderStatusCanceled, OrderStatusExpired, OrderStatusFailed, OrderStatusRejected:
	default:
		return invalid("status", "invalid")
	}
	if err := optionalDecimal("quantity", s.Quantity); err != nil {
		return err
	}
	if err := decimal("filled_quantity", s.FilledQuantity, true); err != nil {
		return err
	}
	if s.CompletedAt != nil {
		return validTime("completed_at", *s.CompletedAt)
	}
	return nil
}

// Validate validates one immutable event; references are checked during replay.
//
// Version:
//   - 2026-09-26: Validate immutable events owned by an execution snapshot.
func (e Event) Validate() error {
	if e.RequestedQuantity != nil {
		if e.EventType != EventTypeSubmissionAccepted {
			return invalid("requested_quantity", "invalid")
		}
		if err := optionalDecimal("requested_quantity", e.RequestedQuantity); err != nil {
			return err
		}
	}
	if e.ID == 0 || e.OrderID == 0 || e.ExecutionRecordID == 0 || e.Sequence == 0 {
		return invalid("identity", "empty")
	}
	switch e.EventType {
	case EventTypeSubmissionAccepted, EventTypeSubmitted, EventTypeSubmissionRejected, EventTypeSucceeded, EventTypeFailed, EventTypeReversed, EventTypeFilled, EventTypeFillReversed, EventTypeFillCorrected, EventTypeFeesRecorded, EventTypeFeesAdjusted, EventTypeEvidenceRecorded, EventTypeOrderCanceled, EventTypeOrderExpired, EventTypeOrderRejected, EventTypeOrderFailed:
	default:
		return invalid("event_type", "invalid")
	}
	if err := binaryKey("record_key", e.RecordKey); err != nil {
		return err
	}
	if err := textValue("execution_system", e.ExecutionSystem, 64, true); err != nil {
		return err
	}
	if err := optionalText("execution_id", e.ExecutionID, 64); err != nil {
		return err
	}
	if err := optionalText("venue", e.Venue, 64); err != nil {
		return err
	}
	if err := validTime("occurred_at", e.OccurredAt); err != nil {
		return err
	}
	if e.SubmissionEventID != nil && (*e.SubmissionEventID == 0 || *e.SubmissionEventID == e.ID) {
		return invalid("submission_event_id", "invalid")
	}
	if e.ReferenceEventID != nil && (*e.ReferenceEventID == 0 || *e.ReferenceEventID == e.ID) {
		return invalid("reference_event_id", "invalid")
	}
	if e.EventType == EventTypeSubmissionAccepted {
		if e.SubmissionEventID != nil || e.ExecutionID == nil || e.Venue == nil {
			return invalid("submission", "invalid")
		}
	} else if e.SubmissionEventID == nil || e.ExecutionID == nil {
		return invalid("submission", "null")
	}
	if needsReference(e.EventType) && e.ReferenceEventID == nil {
		return invalid("reference_event_id", "null")
	}
	if isFill(e.EventType) {
		if e.Quantity == nil || e.CounterQuantity == nil || e.OrderQuantity == nil || e.QuantityAssetID == nil || e.CounterAssetID == nil || e.QuantityDecimals == nil || e.CounterDecimals == nil {
			return invalid("fill", "null")
		}
		if err := unitDecimal("quantity", *e.Quantity, *e.QuantityDecimals, false); err != nil {
			return err
		}
		if *e.Quantity == "0" {
			return invalid("quantity", "out_of_range")
		}
		if err := unitDecimal("counter_quantity", *e.CounterQuantity, *e.CounterDecimals, false); err != nil {
			return err
		}
		if err := decimal("order_quantity", *e.OrderQuantity, true); err != nil {
			return err
		}
		if err := optionalDecimal("price", e.Price); err != nil {
			return err
		}
		if err := textValue("quantity_asset_id", *e.QuantityAssetID, 65535, true); err != nil {
			return err
		}
		if err := textValue("counter_asset_id", *e.CounterAssetID, 65535, true); err != nil {
			return err
		}
	} else if e.Quantity != nil || e.CounterQuantity != nil || e.OrderQuantity != nil || e.QuantityAssetID != nil || e.CounterAssetID != nil || e.QuantityDecimals != nil || e.CounterDecimals != nil || e.Price != nil {
		return invalid("fill", "invalid")
	}
	return nil
}

// Validate validates chain-neutral evidence and the accepted recovery payload envelope.
// Chain identity, signature and finality policy must be checked by the adapter.
//
// Version:
//   - 2026-09-26: Added.
func (d OnchainEvidence) Validate() error {
	if d.EventID == 0 || d.OrderID == 0 {
		return invalid("identity", "empty")
	}
	unit := map[string]string{"evm": "block", "solana": "slot", "sui": "checkpoint"}[d.ChainFamily]
	if unit == "" {
		return invalid("chain_family", "invalid")
	}
	for _, f := range []struct {
		name, value string
		max         int
	}{{"chain", d.Chain, 64}, {"network", d.Network, 64}, {"tx_id", d.TxID, 255}} {
		if err := textValue(f.name, f.value, f.max, true); err != nil {
			return err
		}
	}
	for _, f := range []struct {
		name  string
		value *string
		max   int
	}{
		{"ledger_id", d.LedgerID, 255}, {"emitter_id", d.EmitterID, 255}, {"pool_id", d.PoolID, 255}, {"signer_id", d.SignerID, 128}, {"recipient_id", d.RecipientID, 128}, {"payload_digest", d.PayloadDigest, 255}, {"payload_encoding", d.PayloadEncoding, 32},
	} {
		if err := optionalText(f.name, f.value, f.max); err != nil {
			return err
		}
	}
	if d.ProtocolVersion == 0 {
		return invalid("protocol_version", "empty")
	}
	if err := jsonObject("protocol_data", d.ProtocolData, false); err != nil {
		return err
	}
	if d.LedgerUnit == nil {
		if d.LedgerSequence != nil || d.LedgerID != nil || d.TxPosition != nil || d.FinalityLevel != nil {
			return invalid("ledger", "invalid")
		}
	} else {
		if *d.LedgerUnit != unit || d.LedgerSequence == nil {
			return invalid("ledger", "invalid")
		}
		if d.FinalityLevel != nil && *d.FinalityLevel != "observed" && *d.FinalityLevel != "confirmed" && *d.FinalityLevel != "finalized" {
			return invalid("finality_level", "invalid")
		}
	}
	if d.EventType == EventTypeSubmissionAccepted {
		if d.LedgerUnit != nil || d.EventPosition != nil || d.SignerID == nil || d.PayloadDigest == nil || d.PayloadEncoding == nil || len(d.TxPayload) == 0 {
			return invalid("acceptance", "invalid")
		}
		if len(d.TxPayload) > 16777215 {
			return invalid("tx_payload", "too_long")
		}
	} else if d.SignerID != nil || d.RecipientID != nil || d.PayloadDigest != nil || d.PayloadEncoding != nil || d.TxPayload != nil {
		return invalid("acceptance", "invalid")
	}
	if isResult(d.EventType) || isFill(d.EventType) {
		if d.LedgerUnit == nil || d.FinalityLevel == nil {
			return invalid("ledger", "null")
		}
	}
	if isFill(d.EventType) {
		if d.EventPosition == nil {
			return invalid("event_position", "null")
		}
		if err := eventPosition(d.ChainFamily, *d.EventPosition); err != nil {
			return err
		}
	} else if d.EventPosition != nil {
		return invalid("event_position", "invalid")
	}
	return nil
}
func eventPosition(family, value string) error {
	if err := textValue("event_position", value, 512, true); err != nil {
		return err
	}
	index := `(0|[1-9][0-9]*)`
	pattern := map[string]string{"evm": `^v1/log/` + index + `$`, "sui": `^v1/event/` + index + `$`, "solana": `^v1/(instruction/` + index + `(/inner/` + index + `)?|program-log/` + index + `)$`}[family]
	if pattern == "" || !regexp.MustCompile(pattern).MatchString(value) {
		return invalid("event_position", "invalid")
	}
	return nil
}

// Validate validates one signed asset-unit cost or adjustment component.
//
// Version:
//   - 2026-09-26: Added.
func (f ExecutionFee) Validate() error {
	if f.ID == 0 || f.OrderID == 0 || f.ExecutionRecordID == 0 || f.EventID == 0 {
		return invalid("identity", "empty")
	}
	if f.AdjustmentOfFeeID != nil && (*f.AdjustmentOfFeeID == 0 || *f.AdjustmentOfFeeID == f.ID) {
		return invalid("adjustment_of_fee_id", "invalid")
	}
	if err := binaryKey("record_key", f.RecordKey); err != nil {
		return err
	}
	for _, v := range []struct {
		name, value string
		max         int
	}{{"fee_type", f.FeeType, 32}, {"asset_id", f.AssetID, 65535}, {"source_reference", f.SourceReference, 512}} {
		if err := textValue(v.name, v.value, v.max, true); err != nil {
			return err
		}
	}
	switch f.AccountingTreatment {
	case "additional", "included_in_input", "included_in_output", "unknown":
	default:
		return invalid("accounting_treatment", "invalid")
	}
	switch f.AssetNamespace {
	case "onchain":
		if f.AssetChain == nil || f.AssetNetwork == nil {
			return invalid("asset_chain", "null")
		}
		if err := optionalText("asset_chain", f.AssetChain, 64); err != nil {
			return err
		}
		if err := optionalText("asset_network", f.AssetNetwork, 64); err != nil {
			return err
		}
	case "venue", "currency":
		if f.AssetChain != nil || f.AssetNetwork != nil {
			return invalid("asset_chain", "invalid")
		}
	default:
		return invalid("asset_namespace", "invalid")
	}
	if err := unitDecimal("amount", f.Amount, f.AssetDecimals, true); err != nil {
		return err
	}
	return validTime("occurred_at", f.OccurredAt)
}
