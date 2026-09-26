package oms

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"
)

func normalizeRecord(input ExecutionRecord, sequence uint64, existing *ExecutionRecord) (ExecutionRecord, error) {
	r := input
	r.Fees = append([]ExecutionFee(nil), input.Fees...)
	e := &r.Execution
	if existing != nil {
		sequence = existing.Execution.Sequence
		if e.ID == 0 {
			e.ID = existing.Execution.ID
		}
	}
	if e.Sequence != 0 && e.Sequence != sequence {
		return r, invalid("sequence", "invalid")
	}
	e.Sequence = sequence
	if e.ID == 0 {
		e.ID = GenerateExecutionID()
	}
	e.OccurredAt = utc(e.OccurredAt)
	e.RecordedAt = created(e.RecordedAt)
	if err := e.Validate(); err != nil {
		return r, err
	}
	if r.Onchain != nil {
		d := *r.Onchain
		r.Onchain = &d
		if d.ExecutionRecordID != 0 && d.ExecutionRecordID != e.ID || d.OrderID != 0 && d.OrderID != e.OrderID || d.ExecType != "" && d.ExecType != e.ExecType {
			return r, invalid("onchain_parent", "invalid")
		}
		d.ExecutionRecordID = e.ID
		d.OrderID = e.OrderID
		d.ExecType = e.ExecType
		r.Onchain = &d
		if err := d.Validate(); err != nil {
			return r, err
		}
	}
	keys := map[string]bool{}
	ids := map[uint64]bool{}
	for i := range r.Fees {
		f := &r.Fees[i]
		if f.ExecutionRecordID != 0 && f.ExecutionRecordID != e.ID || f.OrderID != 0 && f.OrderID != e.OrderID {
			return r, invalid("fee_parent", "invalid")
		}
		f.ExecutionRecordID = e.ID
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
func comparisonRecord(r ExecutionRecord) (ExecutionRecord, error) {
	r.Execution.RecordedAt = time.Time{}
	r.Execution.OccurredAt = utc(r.Execution.OccurredAt)
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
func recordsEqual(a, b ExecutionRecord) (bool, error) {
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
func validateChildren(r ExecutionRecord, history []ExecutionRecord) error {
	e := r.Execution
	records := map[uint64]ExecutionRecord{}
	fees := map[uint64]ExecutionFee{}
	for _, old := range history {
		records[old.Execution.ID] = old
		for _, f := range old.Fees {
			fees[f.ID] = f
		}
	}
	conflict := func(field string) error { return fmt.Errorf("%w: %s=invalid", ErrConflict, field) }
	root := ExecutionRecord{}
	if e.SubmissionRecordID != nil {
		root = records[*e.SubmissionRecordID]
	}
	if d := r.Onchain; d != nil {
		if isOrderFact(e.ExecType) {
			return conflict("onchain_parent")
		}
		if e.SubmissionRecordID != nil {
			rd := root.Onchain
			if rd == nil || rd.ChainFamily != d.ChainFamily || rd.Chain != d.Chain || rd.Network != d.Network || rd.TxID != d.TxID {
				return conflict("tx_id")
			}
		}
		if isFill(e.ExecType) {
			origin := func(f Execution) uint64 {
				for f.ExecType == ExecutionTypeFillCorrected {
					f = records[*f.ReferenceRecordID].Execution
				}
				return f.ID
			}
			for _, old := range history {
				od := old.Onchain
				if od == nil || !isFill(old.Execution.ExecType) {
					continue
				}
				if od.Chain == d.Chain && od.Network == d.Network && od.TxID == d.TxID && sameString(od.LedgerUnit, d.LedgerUnit) && sameUint(od.LedgerSequence, d.LedgerSequence) && compatibleString(od.LedgerID, d.LedgerID) && sameString(od.EventPosition, d.EventPosition) {
					if e.ExecType != ExecutionTypeFillCorrected || origin(old.Execution) != origin(records[*e.ReferenceRecordID].Execution) {
						return conflict("event_position")
					}
				}
			}
		}
		if err := validateLedgerEvidence(r, history); err != nil {
			return err
		}
	} else if root.Onchain != nil && (isFill(e.ExecType) || isResult(e.ExecType) || e.ExecType == ExecutionTypeSubmitted || e.ExecType == ExecutionTypeReversed || e.ExecType == ExecutionTypeSubmissionRejected || e.ExecType == ExecutionTypeFillReversed) {
		return conflict("onchain_detail")
	}
	if len(r.Fees) > 0 && !isResult(e.ExecType) && !isFill(e.ExecType) && e.ExecType != ExecutionTypeFeesRecorded && e.ExecType != ExecutionTypeFeesAdjusted {
		return conflict("fee_parent")
	}
	if e.ExecType == ExecutionTypeFeesAdjusted && len(r.Fees) == 0 {
		return invalid("fees", "empty")
	}
	for _, f := range r.Fees {
		owner := e
		if e.ExecType == ExecutionTypeFeesRecorded || e.ExecType == ExecutionTypeFeesAdjusted {
			owner = records[*e.ReferenceRecordID].Execution
		}
		if f.FeeType == "gas" && !isResult(owner.ExecType) {
			return conflict("fee_parent")
		}
		if f.FeeType == "gas" && root.Onchain != nil {
			if f.AssetNamespace != "onchain" || f.AssetChain == nil || *f.AssetChain != root.Onchain.Chain || f.AssetNetwork == nil || *f.AssetNetwork != root.Onchain.Network {
				return conflict("gas_asset")
			}
		}
		if f.FeeType != "gas" && !isFill(owner.ExecType) {
			return conflict("fee_parent")
		}
		if e.ExecType == ExecutionTypeFeesAdjusted {
			if f.AdjustmentOfFeeID == nil {
				return invalid("adjustment_of_fee_id", "null")
			}
			original, ok := fees[*f.AdjustmentOfFeeID]
			if !ok || original.AdjustmentOfFeeID != nil || !sameFeeAsset(original, f) {
				return conflict("adjustment_of_fee_id")
			}
			originalOwner := records[original.ExecutionRecordID].Execution
			if originalOwner.ExecType == ExecutionTypeFeesRecorded {
				originalOwner = records[*originalOwner.ReferenceRecordID].Execution
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
func sameLedger(a, b *OnchainDetail) bool {
	return sameString(a.LedgerUnit, b.LedgerUnit) && sameUint(a.LedgerSequence, b.LedgerSequence) && compatibleString(a.LedgerID, b.LedgerID) && compatibleUint(a.TxPosition, b.TxPosition)
}

func validateLedgerEvidence(r ExecutionRecord, history []ExecutionRecord) error {
	e := r.Execution
	if e.SubmissionRecordID == nil || (!isFill(e.ExecType) && !isResult(e.ExecType)) {
		return nil
	}
	var result *OnchainDetail
	fills := map[uint64]*OnchainDetail{}
	for _, old := range history {
		if !sameUint(old.Execution.SubmissionRecordID, e.SubmissionRecordID) {
			continue
		}
		switch old.Execution.ExecType {
		case ExecutionTypeSucceeded, ExecutionTypeFailed:
			result = old.Onchain
		case ExecutionTypeReversed:
			result = nil
			clear(fills)
		case ExecutionTypeFilled:
			fills[old.Execution.ID] = old.Onchain
		case ExecutionTypeFillCorrected:
			delete(fills, *old.Execution.ReferenceRecordID)
			fills[old.Execution.ID] = old.Onchain
		case ExecutionTypeFillReversed:
			delete(fills, *old.Execution.ReferenceRecordID)
		}
	}
	if result != nil && !sameLedger(result, r.Onchain) {
		return fmt.Errorf("%w: ledger=invalid", ErrConflict)
	}
	// A late result or another leg must agree with all active evidence for the same Tx.
	for id, detail := range fills {
		if e.ExecType == ExecutionTypeFillCorrected && id == *e.ReferenceRecordID {
			continue
		}
		if detail == nil || !sameLedger(detail, r.Onchain) {
			return fmt.Errorf("%w: ledger=invalid", ErrConflict)
		}
	}
	return nil
}
