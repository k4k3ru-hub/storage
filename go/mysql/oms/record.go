package oms

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"
)

func normalizeRecord(input OnchainEvent, sequence uint64, existing *OnchainEvent) (OnchainEvent, error) {
	r := input
	r.Fees = append([]ExecutionFee(nil), input.Fees...)
	e := &r.Event
	if e.ExecutionRecordID == 0 && existing != nil {
		e.ExecutionRecordID = existing.Event.ExecutionRecordID
	}
	if e.ExecutionRecordID == 0 && e.EventType == EventTypeSubmissionAccepted {
		e.ExecutionRecordID = GenerateExecutionID()
	}
	if existing != nil {
		sequence = existing.Event.Sequence
		if e.ID == 0 {
			e.ID = existing.Event.ID
		}
	}
	if e.Sequence != 0 && e.Sequence != sequence {
		return r, invalid("sequence", "invalid")
	}
	e.Sequence = sequence
	if e.ID == 0 {
		e.ID = GenerateEventID()
	}
	e.OccurredAt = utc(e.OccurredAt)
	e.RecordedAt = created(e.RecordedAt)
	if err := e.Validate(); err != nil {
		return r, err
	}
	if r.Onchain != nil {
		d := *r.Onchain
		r.Onchain = &d
		if d.EventID != 0 && d.EventID != e.ID || d.OrderID != 0 && d.OrderID != e.OrderID || d.EventType != "" && d.EventType != e.EventType {
			return r, invalid("onchain_parent", "invalid")
		}
		d.EventID = e.ID
		d.OrderID = e.OrderID
		d.EventType = e.EventType
		r.Onchain = &d
		if err := d.Validate(); err != nil {
			return r, err
		}
	}
	keys := map[string]bool{}
	ids := map[uint64]bool{}
	for i := range r.Fees {
		f := &r.Fees[i]
		if f.EventID != 0 && f.EventID != e.ID || f.OrderID != 0 && f.OrderID != e.OrderID {
			return r, invalid("fee_parent", "invalid")
		}
		if f.ExecutionRecordID != 0 && f.ExecutionRecordID != e.ExecutionRecordID {
			return r, invalid("fee_parent", "invalid")
		}
		f.ExecutionRecordID = e.ExecutionRecordID
		f.EventID = e.ID
		f.OrderID = e.OrderID
		if f.ID == 0 && existing != nil {
			for _, old := range existing.Fees {
				if bytes.Equal(old.RecordKey, f.RecordKey) {
					f.ID = old.ID
					break
				}
			}
		}
		if f.ID == 0 {
			f.ID = GenerateFeeID()
		}
		f.OccurredAt = utc(f.OccurredAt)
		f.RecordedAt = created(f.RecordedAt)
		if err := f.Validate(); err != nil {
			return r, err
		}
		if keys[string(f.RecordKey)] || ids[f.ID] {
			return r, invalid("fees", "invalid")
		}
		keys[string(f.RecordKey)] = true
		ids[f.ID] = true
		for _, previous := range r.Fees[:i] {
			if previous.SourceReference == f.SourceReference && sameFeeAsset(previous, *f) {
				return r, invalid("source_reference", "invalid")
			}
		}
	}
	return r, nil
}
func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	// All inputs have been validated; UseNumber preserves arbitrarily large evidence integers.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("failed to decode oms evidence: %w", err)
	}
	result, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode oms evidence: %w", err)
	}
	return result, nil
}
func comparisonRecord(r OnchainEvent) (OnchainEvent, error) {
	r.Event.RecordedAt = time.Time{}
	r.Event.OccurredAt = utc(r.Event.OccurredAt)
	if r.Onchain != nil {
		d := *r.Onchain
		var err error
		d.ProtocolData, err = canonicalJSON(d.ProtocolData)
		if err != nil {
			return r, err
		}
		r.Onchain = &d
	}
	r.Fees = append([]ExecutionFee(nil), r.Fees...)
	for i := range r.Fees {
		r.Fees[i].RecordedAt = time.Time{}
		r.Fees[i].OccurredAt = utc(r.Fees[i].OccurredAt)
	}
	sort.Slice(r.Fees, func(i, j int) bool { return bytes.Compare(r.Fees[i].RecordKey, r.Fees[j].RecordKey) < 0 })
	return r, nil
}
func recordsEqual(a, b OnchainEvent) (bool, error) {
	a, err := comparisonRecord(a)
	if err != nil {
		return false, err
	}
	b, err = comparisonRecord(b)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(a, b), nil
}
func sameFeeAsset(a, b ExecutionFee) bool {
	return a.FeeType == b.FeeType && a.AccountingTreatment == b.AccountingTreatment && a.AssetNamespace == b.AssetNamespace && sameString(a.AssetChain, b.AssetChain) && sameString(a.AssetNetwork, b.AssetNetwork) && a.AssetID == b.AssetID && a.AssetDecimals == b.AssetDecimals
}
func validateChildren(r OnchainEvent, history []OnchainEvent) error {
	e := r.Event
	records := map[uint64]OnchainEvent{}
	fees := map[uint64]ExecutionFee{}
	for _, old := range history {
		records[old.Event.ID] = old
		for _, f := range old.Fees {
			fees[f.ID] = f
		}
	}
	conflict := func(field string) error { return fmt.Errorf("%w: %s=invalid", ErrConflict, field) }
	root := OnchainEvent{}
	if e.SubmissionEventID != nil {
		root = records[*e.SubmissionEventID]
	}
	if d := r.Onchain; d != nil {
		if e.SubmissionEventID != nil {
			rd := root.Onchain
			if rd == nil || rd.ChainFamily != d.ChainFamily || rd.Chain != d.Chain || rd.Network != d.Network || rd.TxID != d.TxID {
				return conflict("tx_id")
			}
		}
		if isFill(e.EventType) {
			origin := func(f Event) uint64 {
				for f.EventType == EventTypeFillCorrected {
					f = records[*f.ReferenceEventID].Event
				}
				return f.ID
			}
			for _, old := range history {
				od := old.Onchain
				if od == nil || !isFill(old.Event.EventType) {
					continue
				}
				if od.Chain == d.Chain && od.Network == d.Network && od.TxID == d.TxID && sameString(od.LedgerUnit, d.LedgerUnit) && sameUint(od.LedgerSequence, d.LedgerSequence) && compatibleString(od.LedgerID, d.LedgerID) && sameString(od.EventPosition, d.EventPosition) {
					if e.EventType != EventTypeFillCorrected || origin(old.Event) != origin(records[*e.ReferenceEventID].Event) {
						return conflict("event_position")
					}
				}
			}
		}
		if err := validateLedgerEvidence(r, history); err != nil {
			return err
		}
	} else if root.Onchain != nil && (isFill(e.EventType) || isResult(e.EventType) || e.EventType == EventTypeSubmitted || e.EventType == EventTypeReversed || e.EventType == EventTypeSubmissionRejected || e.EventType == EventTypeFillReversed) {
		return conflict("onchain_detail")
	}
	if len(r.Fees) > 0 && !isResult(e.EventType) && !isFill(e.EventType) && e.EventType != EventTypeFeesRecorded && e.EventType != EventTypeFeesAdjusted {
		return conflict("fee_parent")
	}
	if e.EventType == EventTypeFeesAdjusted && len(r.Fees) == 0 {
		return invalid("fees", "empty")
	}
	for _, f := range r.Fees {
		owner := e
		if e.EventType == EventTypeFeesRecorded || e.EventType == EventTypeFeesAdjusted {
			owner = records[*e.ReferenceEventID].Event
		}
		if f.FeeType == "gas" && !isResult(owner.EventType) {
			return conflict("fee_parent")
		}
		if f.FeeType == "gas" && root.Onchain != nil {
			if f.AssetNamespace != "onchain" || f.AssetChain == nil || *f.AssetChain != root.Onchain.Chain || f.AssetNetwork == nil || *f.AssetNetwork != root.Onchain.Network {
				return conflict("gas_asset")
			}
		}
		if f.FeeType != "gas" && !isFill(owner.EventType) {
			return conflict("fee_parent")
		}
		if e.EventType == EventTypeFeesAdjusted {
			if f.AdjustmentOfFeeID == nil {
				return invalid("adjustment_of_fee_id", "null")
			}
			original, ok := fees[*f.AdjustmentOfFeeID]
			if !ok || original.AdjustmentOfFeeID != nil || !sameFeeAsset(original, f) {
				return conflict("adjustment_of_fee_id")
			}
			originalOwner := records[original.EventID].Event
			if originalOwner.EventType == EventTypeFeesRecorded {
				originalOwner = records[*originalOwner.ReferenceEventID].Event
			}
			if originalOwner.ID != owner.ID {
				return conflict("fee_parent")
			}
			if f.SourceVersion != nil {
				for _, old := range fees {
					if old.ID == original.ID || old.AdjustmentOfFeeID != nil && *old.AdjustmentOfFeeID == original.ID {
						if old.SourceVersion != nil && *f.SourceVersion <= *old.SourceVersion {
							return conflict("source_version")
						}
					}
				}
			}
		} else if f.AdjustmentOfFeeID != nil {
			return conflict("adjustment_of_fee_id")
		}
		for _, old := range fees {
			if bytes.Equal(old.RecordKey, f.RecordKey) {
				return conflict("fee_record_key")
			}
			// Source identity prevents counting a cost again under a different transport key.
			if old.SourceReference == f.SourceReference && sameFeeAsset(old, f) {
				if f.AdjustmentOfFeeID == nil || sameUint(old.SourceVersion, f.SourceVersion) {
					return conflict("source_reference")
				}
			}
		}
	}
	return nil
}

func compatibleString(a, b *string) bool { return a == nil || b == nil || *a == *b }
func compatibleUint(a, b *uint64) bool   { return a == nil || b == nil || *a == *b }
func sameLedger(a, b *OnchainEvidence) bool {
	return sameString(a.LedgerUnit, b.LedgerUnit) && sameUint(a.LedgerSequence, b.LedgerSequence) && compatibleString(a.LedgerID, b.LedgerID) && compatibleUint(a.TxPosition, b.TxPosition)
}

func validateLedgerEvidence(r OnchainEvent, history []OnchainEvent) error {
	e := r.Event
	if e.SubmissionEventID == nil || (!isFill(e.EventType) && !isResult(e.EventType)) {
		return nil
	}
	var result *OnchainEvidence
	fills := map[uint64]*OnchainEvidence{}
	for _, old := range history {
		if !sameUint(old.Event.SubmissionEventID, e.SubmissionEventID) {
			continue
		}
		switch old.Event.EventType {
		case EventTypeSucceeded, EventTypeFailed:
			result = old.Onchain
		case EventTypeReversed:
			result = nil
			clear(fills)
		case EventTypeFilled:
			fills[old.Event.ID] = old.Onchain
		case EventTypeFillCorrected:
			delete(fills, *old.Event.ReferenceEventID)
			fills[old.Event.ID] = old.Onchain
		case EventTypeFillReversed:
			delete(fills, *old.Event.ReferenceEventID)
		}
	}
	if result != nil && !sameLedger(result, r.Onchain) {
		return fmt.Errorf("%w: ledger=invalid", ErrConflict)
	}
	// A late result or another leg must agree with all active evidence for the same Tx.
	for id, detail := range fills {
		if e.EventType == EventTypeFillCorrected && id == *e.ReferenceEventID {
			continue
		}
		if detail == nil || !sameLedger(detail, r.Onchain) {
			return fmt.Errorf("%w: ledger=invalid", ErrConflict)
		}
	}
	return nil
}
