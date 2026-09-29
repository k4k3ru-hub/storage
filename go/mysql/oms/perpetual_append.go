package oms

import (
	"context"
	"database/sql"
	"fmt"
)

// AppendPerpetualEvent appends a Perpetual event and fees and updates both locked snapshots.
// It allocates sequence and zero IDs. An identical key/content returns the original record.
// The caller owns commit and must roll back the entire transaction on any error.
// No RPC or network operation may be performed while holding this transaction.
//
// Version:
//   - 2026-09-29: Add atomic Perpetual history, fees and snapshot projection.
func (s *Store) AppendPerpetualEvent(ctx context.Context, tx *sql.Tx, accountID uint64, record PerpetualEvent) (*AppendResult, error) {
	const op = "failed to append oms perpetual event"
	order, err := s.SelectOrderForUpdate(ctx, tx, accountID, record.Event.OrderID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if s.perpetualEventTable == "" || order.Domain != DomainPerpetual {
		return nil, fmt.Errorf("%s: %w: event_family=invalid", op, ErrConflict)
	}
	asset, err := order.CounterQuantityAsset()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if asset == nil || asset.Namespace != "venue" {
		return nil, fmt.Errorf("%s: %w: counter_asset=invalid", op, ErrConflict)
	}
	if !sameString(order.Venue, record.Event.Venue) || record.Event.EventType == EventTypeSubmissionAccepted && !sameString(order.Quantity, record.Event.RequestedQuantity) {
		return nil, fmt.Errorf("%s: %w: order_binding=invalid", op, ErrConflict)
	}
	history, err := s.loadPerpetualHistory(ctx, tx, order.ID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	facts := make([]Event, len(history))
	for i, r := range history {
		facts[i] = r.Event
	}
	before, err := ReplayOrder(*order, facts)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if !orderStateEqual(order.OrderState, before.State) {
		return nil, fmt.Errorf("%s: %w: snapshot=invalid", op, ErrConflict)
	}
	snapshots, err := s.loadSnapshots(ctx, tx, order.ID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	roots := 0
	for _, r := range history {
		if r.Event.EventType == EventTypeSubmissionAccepted {
			roots++
		}
	}
	if len(snapshots) != roots {
		return nil, fmt.Errorf("%s: %w: executions=invalid", op, ErrConflict)
	}
	for _, saved := range snapshots {
		projected, err := projectPerpetualExecution(saved.ID, history, before)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		if !executionEqual(saved, *projected) {
			return nil, fmt.Errorf("%s: %w: execution_snapshot=invalid", op, ErrConflict)
		}
	}
	var existing *PerpetualEvent
	for i := range history {
		if string(history[i].Event.RecordKey) == string(record.Event.RecordKey) {
			existing = &history[i]
			break
		}
	}
	if record.Event.SubmissionEventID != nil {
		for _, old := range history {
			if old.Event.ID == *record.Event.SubmissionEventID {
				if record.Event.ExecutionRecordID != 0 && record.Event.ExecutionRecordID != old.Event.ExecutionRecordID {
					return nil, fmt.Errorf("%s: %w", op, ErrConflict)
				}
				record.Event.ExecutionRecordID = old.Event.ExecutionRecordID
				break
			}
		}
	}
	normalized, err := normalizePerpetualRecord(record, order.LastEventSequence+1, existing)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if existing != nil {
		equal, err := perpetualRecordsEqual(normalized, *existing)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		if !equal {
			return nil, fmt.Errorf("%s: %w: record_key=invalid", op, ErrConflict)
		}
		snapshot, err := scanExecution(tx.QueryRowContext(ctx, "SELECT "+executionColumns+" FROM "+quoted(s.executionTable)+" WHERE order_id=? AND id=? FOR UPDATE", order.ID, existing.Event.ExecutionRecordID))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		return &AppendResult{EventID: existing.Event.ID, ExecutionRecordID: existing.Event.ExecutionRecordID, Execution: *snapshot, Sequence: existing.Event.Sequence, Duplicate: true, State: order.OrderState}, nil
	}
	if normalized.Event.EventType == EventTypeSubmissionAccepted && (len(history) > 0 || order.Status != OrderStatusPending) {
		return nil, fmt.Errorf("%s: %w: order_status=invalid", op, ErrConflict)
	}
	facts = append(facts, normalized.Event)
	next, err := ReplayOrder(*order, facts)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := validatePerpetualChildren(normalized, history); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	snapshot, err := projectPerpetualExecution(normalized.Event.ExecutionRecordID, append(history, normalized), next)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if normalized.Event.EventType == EventTypeSubmissionAccepted {
		if _, err = tx.ExecContext(ctx, insertSQL(s.executionTable, executionColumns), executionArgs(*snapshot)...); err != nil {
			return nil, writeError(op, err)
		}
	} else {
		if _, err = tx.ExecContext(ctx, "UPDATE "+quoted(s.executionTable)+" SET venue_order_id=?,client_order_id=?,status=?,filled_quantity=?,filled_counter_quantity=?,fees_complete=?,last_event_sequence=?,completed_at=?,updated_at=? WHERE id=? AND order_id=?", snapshot.VenueOrderID, snapshot.ClientOrderID, snapshot.Status, snapshot.FilledQuantity, snapshot.FilledCounterQuantity, snapshot.FeesComplete, snapshot.LastEventSequence, snapshot.CompletedAt, snapshot.UpdatedAt, snapshot.ID, order.ID); err != nil {
			return nil, writeError(op, err)
		}
	}
	if _, err = tx.ExecContext(ctx, insertSQL(s.perpetualEventTable, perpetualEventColumns), perpetualEventArgs(normalized)...); err != nil {
		return nil, writeError(op, err)
	}
	for _, f := range normalized.Fees {
		if _, err := tx.ExecContext(ctx, insertSQL(s.feeTable, executionFeeColumns), executionFeeArgs(f)...); err != nil {
			return nil, writeError(op, err)
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE "+quoted(s.orderTable)+" SET status=?,filled_quantity=?,filled_counter_quantity=?,completed_at=?,last_event_sequence=? WHERE account_id=? AND id=?", next.State.Status, next.State.FilledQuantity, next.State.FilledCounterQuantity, optionalTime(next.State.CompletedAt), next.State.LastEventSequence, accountID, order.ID)
	if err != nil {
		return nil, writeError(op, err)
	}
	return &AppendResult{EventID: normalized.Event.ID, ExecutionRecordID: normalized.Event.ExecutionRecordID, Execution: *snapshot, Sequence: normalized.Event.Sequence, State: next.State}, nil
}
