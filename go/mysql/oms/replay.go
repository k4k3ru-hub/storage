package oms

import (
	"fmt"
	"math/big"
	"strings"
)

type OrderProjection struct {
	State       OrderState
	ActiveFills []Execution
}

// ReplayOrder rebuilds the snapshot and effective fills from the complete sequence-ordered history.
// Corrections replace the referenced active fill; reversals never delete facts or fees.
// Missing quantity leaves a nonzero fill partially filled. Order termination requires an order fact.
//
// Version:
//   - 2026-09-26: Added.
func ReplayOrder(order Order, history []Execution) (*OrderProjection, error) {
	const op = "failed to replay oms order"
	if err := order.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	state := OrderState{Status: OrderStatusPending, Quantity: order.Quantity, FilledQuantity: "0"}
	seen := map[uint64]Execution{}
	keys := map[string]bool{}
	publicIDs := map[string]bool{}
	active := map[uint64]Execution{}
	results := map[uint64]uint64{}
	reversed := map[uint64]bool{}
	submitted := map[uint64]bool{}
	total := new(big.Rat)
	scale := 0
	var terminal *Execution
	conflict := func(field string) (*OrderProjection, error) {
		return nil, fmt.Errorf("%s: %w: %s=invalid", op, ErrConflict, field)
	}
	for i, e := range history {
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		if e.OrderID != order.ID || e.Sequence != uint64(i)+1 {
			return conflict("sequence")
		}
		if _, exists := seen[e.ID]; exists || keys[string(e.RecordKey)] {
			return conflict("record")
		}
		var root, ref Execution
		if e.SubmissionRecordID != nil {
			var exists bool
			root, exists = seen[*e.SubmissionRecordID]
			if !exists || root.ExecType != ExecutionTypeSubmissionAccepted || root.ExecutionSystem != e.ExecutionSystem || !sameString(root.ExecutionID, e.ExecutionID) {
				return conflict("submission_record_id")
			}
		}
		if e.ReferenceRecordID != nil {
			var exists bool
			ref, exists = seen[*e.ReferenceRecordID]
			if !exists || ref.ExecutionSystem != e.ExecutionSystem {
				return conflict("reference_record_id")
			}
			if e.SubmissionRecordID != nil {
				refRoot := ref.SubmissionRecordID
				if ref.ExecType == ExecutionTypeSubmissionAccepted {
					refRoot = &ref.ID
				}
				if !sameUint(e.SubmissionRecordID, refRoot) || !sameString(ref.ExecutionID, e.ExecutionID) {
					return conflict("reference_record_id")
				}
			}
			if (e.ExecType == ExecutionTypeFillCorrected || e.ExecType == ExecutionTypeFillReversed || e.ExecType == ExecutionTypeReversed) && ref.SourceVersion != nil && e.SourceVersion != nil && *e.SourceVersion <= *ref.SourceVersion {
				return conflict("source_version")
			}
		}
		add := func(fill Execution) {
			v, _ := new(big.Rat).SetString(*fill.OrderQuantity)
			total.Add(total, v)
			active[fill.ID] = fill
			if p := strings.IndexByte(*fill.OrderQuantity, '.'); p >= 0 && len(*fill.OrderQuantity)-p-1 > scale {
				scale = len(*fill.OrderQuantity) - p - 1
			}
		}
		remove := func(fill Execution) {
			v, _ := new(big.Rat).SetString(*fill.OrderQuantity)
			total.Sub(total, v)
			delete(active, fill.ID)
		}
		switch e.ExecType {
		case ExecutionTypeSubmissionAccepted:
			key := e.ExecutionSystem + "\x00" + *e.ExecutionID
			if publicIDs[key] {
				return conflict("execution_id")
			}
			publicIDs[key] = true
		case ExecutionTypeSubmitted:
			if submitted[root.ID] {
				return conflict("submitted")
			}
			submitted[root.ID] = true
		case ExecutionTypeSucceeded, ExecutionTypeFailed, ExecutionTypeSubmissionRejected:
			if results[root.ID] != 0 {
				return conflict("result")
			}
			if e.ExecType != ExecutionTypeSucceeded {
				for _, f := range active {
					if *f.SubmissionRecordID == root.ID {
						return conflict("result")
					}
				}
			}
			results[root.ID] = e.ID
			reversed[root.ID] = false
		case ExecutionTypeReversed:
			if !isResult(ref.ExecType) || results[root.ID] != ref.ID {
				return conflict("reference_record_id")
			}
			delete(results, root.ID)
			reversed[root.ID] = true
			for _, f := range active {
				if *f.SubmissionRecordID == root.ID {
					remove(f)
				}
			}
		case ExecutionTypeFilled, ExecutionTypeFillCorrected:
			if reversed[root.ID] {
				return conflict("result")
			}
			if resultID := results[root.ID]; resultID != 0 && seen[resultID].ExecType != ExecutionTypeSucceeded {
				return conflict("result")
			}
			if e.ExecType == ExecutionTypeFillCorrected {
				original, ok := active[ref.ID]
				if !ok || !sameString(original.QuantityAssetID, e.QuantityAssetID) || !sameString(original.CounterAssetID, e.CounterAssetID) || *original.QuantityDecimals != *e.QuantityDecimals || *original.CounterDecimals != *e.CounterDecimals {
					return conflict("reference_record_id")
				}
				remove(original)
			}
			add(e)
		case ExecutionTypeFillReversed:
			original, ok := active[ref.ID]
			if !ok {
				return conflict("reference_record_id")
			}
			remove(original)
		case ExecutionTypeFeesRecorded, ExecutionTypeFeesAdjusted:
			if !isFill(ref.ExecType) && !isResult(ref.ExecType) {
				return conflict("reference_record_id")
			}
		case ExecutionTypeOrderCanceled, ExecutionTypeOrderExpired, ExecutionTypeOrderRejected, ExecutionTypeOrderFailed:
			if terminal != nil {
				return conflict("order_termination")
			}
			copy := e
			terminal = &copy
		}
		seen[e.ID] = e
		keys[string(e.RecordKey)] = true
		next := OrderStatusPending
		if total.Sign() > 0 {
			next = OrderStatusPartiallyFilled
		}
		var terminalTime = e.OccurredAt
		if terminal != nil {
			next = map[ExecutionType]OrderStatus{ExecutionTypeOrderCanceled: OrderStatusCanceled, ExecutionTypeOrderExpired: OrderStatusExpired, ExecutionTypeOrderRejected: OrderStatusRejected, ExecutionTypeOrderFailed: OrderStatusFailed}[terminal.ExecType]
			terminalTime = terminal.OccurredAt
		}
		if order.Quantity != nil {
			target, _ := new(big.Rat).SetString(*order.Quantity)
			if total.Cmp(target) >= 0 {
				next = OrderStatusFilled
				terminalTime = e.OccurredAt
			}
		}
		if next == OrderStatusPending || next == OrderStatusPartiallyFilled {
			state.CompletedAt = nil
		} else if next != state.Status || state.CompletedAt == nil {
			v := utc(terminalTime)
			state.CompletedAt = &v
		}
		state.Status = next
		state.LastExecutionSequence = e.Sequence
	}
	state.FilledQuantity = strings.TrimRight(strings.TrimRight(total.FloatString(scale), "0"), ".")
	// Only strip fractional zeros; integer zeros are significant.
	if scale == 0 {
		state.FilledQuantity = total.Num().String()
	}
	if state.FilledQuantity == "" {
		state.FilledQuantity = "0"
	}
	if err := state.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	projection := &OrderProjection{State: state}
	for _, e := range history {
		if _, ok := active[e.ID]; ok {
			projection.ActiveFills = append(projection.ActiveFills, e)
		}
	}
	return projection, nil
}
