package oms

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	api "github.com/k4k3ru-hub/storage/go/api"
	"strings"
)

// AppendOnchainEvent appends an onchain event and fees and updates both locked snapshots.
// It allocates sequence and zero IDs. An identical key/content returns the original record.
// The caller owns commit and must roll back the entire transaction on any error.
// No RPC or network operation may be performed while holding this transaction.
//
// Version:
//   - 2026-09-27: Persist both counter snapshots and validate the asset scope.
//   - 2026-09-26: Update execution and order snapshots atomically with onchain events.
func (s *Store) AppendOnchainEvent(ctx context.Context, tx *sql.Tx, accountID uint64, record OnchainEvent) (*AppendResult, error) {
	const op = "failed to append oms onchain event"
	order, err := s.SelectOrderForUpdate(ctx, tx, accountID, record.Event.OrderID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	asset, err := order.CounterQuantityAsset()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if record.Event.OrderCounterQuantity != nil && asset != nil && asset.Namespace == "onchain" {
		d := record.Onchain
		if d == nil || asset.Chain != d.Chain || asset.Network != d.Network {
			return nil, fmt.Errorf("%s: %w: counter_asset_scope=mismatch", op, ErrConflict)
		}
	}
	history, err := s.loadHistory(ctx, tx, order.ID)
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
		projected, err := projectExecution(saved.ID, history, before)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		if !executionEqual(saved, *projected) {
			return nil, fmt.Errorf("%s: %w: execution_snapshot=invalid", op, ErrConflict)
		}
	}
	var existing *OnchainEvent
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
				// Administrative and fee events carry the accepted Tx identity as well.
				if record.Onchain == nil && (isOrderFact(record.Event.EventType) || record.Event.EventType == EventTypeFeesRecorded || record.Event.EventType == EventTypeFeesAdjusted || record.Event.EventType == EventTypeEvidenceRecorded) {
					d := old.Onchain
					if d != nil {
						record.Onchain = &OnchainEvidence{ChainFamily: d.ChainFamily, Chain: d.Chain, Network: d.Network, TxID: d.TxID, ProtocolVersion: 1}
					}
				}
				break
			}
		}
	}
	if record.Onchain == nil {
		return nil, fmt.Errorf("%s: %w", op, fmt.Errorf("%w: onchain=null", ErrConflict))
	}
	normalized, err := normalizeRecord(record, order.LastEventSequence+1, existing)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if existing != nil {
		equal, err := recordsEqual(normalized, *existing)
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
	if normalized.Event.EventType == EventTypeSubmissionAccepted && order.Status != OrderStatusPending && order.Status != OrderStatusPartiallyFilled {
		return nil, fmt.Errorf("%s: %w: order_status=invalid", op, ErrConflict)
	}
	facts = append(facts, normalized.Event)
	next, err := ReplayOrder(*order, facts)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := validateChildren(normalized, history); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	snapshot, err := projectExecution(normalized.Event.ExecutionRecordID, append(history, normalized), next)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if normalized.Event.EventType == EventTypeSubmissionAccepted {
		if _, err = tx.ExecContext(ctx, insertSQL(s.executionTable, executionColumns), executionArgs(*snapshot)...); err != nil {
			return nil, writeError(op, err)
		}
	} else {
		if _, err = tx.ExecContext(ctx, "UPDATE "+quoted(s.executionTable)+" SET status=?,filled_quantity=?,filled_counter_quantity=?,fees_complete=?,last_event_sequence=?,completed_at=?,updated_at=? WHERE id=? AND order_id=?", snapshot.Status, snapshot.FilledQuantity, snapshot.FilledCounterQuantity, snapshot.FeesComplete, snapshot.LastEventSequence, snapshot.CompletedAt, snapshot.UpdatedAt, snapshot.ID, order.ID); err != nil {
			return nil, writeError(op, err)
		}
	}
	if _, err = tx.ExecContext(ctx, insertSQL(s.onchainEventTable, onchainEventColumns), onchainEventArgs(normalized)...); err != nil {
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

// SelectEventByKey retrieves an owned fact by its stable order-local key.
//
// Version:
//   - 2026-09-26: Read immutable facts from the new schema.
func (s *Store) SelectEventByKey(ctx context.Context, executor api.Executor, accountID, orderID uint64, key []byte) (*Event, error) {
	const op = "failed to select oms execution"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := binaryKey("record_key", key); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	value, err := scanEvent(executor.QueryRowContext(ctx, "SELECT "+prefixed("e", eventColumns)+" FROM "+quoted(s.onchainEventTable)+" e JOIN "+quoted(s.orderTable)+" o ON o.id=e.order_id WHERE o.account_id=? AND o.id=? AND e.record_key=?", accountID, orderID, key))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}

// ListEvents lists immutable facts ordered by sequence, after an exclusive sequence cursor.
// Limits range from 1 through 200; use a transaction for a consistent multi-page view.
//
// Version:
//   - 2026-09-26: Replace ID pagination with order-local sequence pagination.
func (s *Store) ListEvents(ctx context.Context, executor api.Executor, accountID, orderID, afterSequence uint64, limit int) ([]Event, error) {
	const op = "failed to list oms executions"
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	if _, err := s.SelectOrder(ctx, executor, accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	rows, err := executor.QueryContext(ctx, "SELECT "+eventColumns+" FROM "+quoted(s.onchainEventTable)+" WHERE order_id=? AND sequence>? ORDER BY sequence LIMIT ?", orderID, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanEvent)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

// SelectOnchainEvidence retrieves owned evidence without loading the recovery payload.
//
// Version:
//   - 2026-09-26: Added.
func (s *Store) SelectOnchainEvidence(ctx context.Context, executor api.Executor, accountID, orderID, recordID uint64) (*OnchainEvidence, error) {
	const op = "failed to select oms onchain detail"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	columns := strings.Replace(prefixed("d", onchainEvidenceColumns), "d.tx_payload", "NULL", 1)
	value, err := scanOnchainEvidence(executor.QueryRowContext(ctx, "SELECT "+columns+" FROM "+quoted(s.onchainEventTable)+" d JOIN "+quoted(s.orderTable)+" o ON o.id=d.order_id WHERE o.account_id=? AND o.id=? AND d.id=?", accountID, orderID, recordID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return value, nil
}

// SelectSubmissionPayload retrieves owned recovery material for an accepted submission only.
// Keep the returned bytes inside the recovery worker; never include them in API responses or logs.
//
// Version:
//   - 2026-09-26: Added.
func (s *Store) SelectSubmissionPayload(ctx context.Context, executor api.Executor, accountID, orderID, recordID uint64) ([]byte, error) {
	const op = "failed to select oms submission payload"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	var payload []byte
	err := executor.QueryRowContext(ctx, "SELECT d.tx_payload FROM "+quoted(s.onchainEventTable)+" d JOIN "+quoted(s.orderTable)+" o ON o.id=d.order_id WHERE o.account_id=? AND o.id=? AND d.id=? AND d.event_type='submission_accepted'", accountID, orderID, recordID).Scan(&payload)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return payload, nil
}

// ListEventFees lists fee components owned by one execution history record.
// Original costs and signed adjustments are returned unchanged; no currency conversion is inferred.
//
// Version:
//   - 2026-09-26: Added.
func (s *Store) ListEventFees(ctx context.Context, executor api.Executor, accountID, orderID, recordID uint64) ([]ExecutionFee, error) {
	const op = "failed to list oms execution fees"
	if err := s.guard(ctx, executor); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	var id uint64
	err := executor.QueryRowContext(ctx, "SELECT e.id FROM "+quoted(s.onchainEventTable)+" e JOIN "+quoted(s.orderTable)+" o ON o.id=e.order_id WHERE o.account_id=? AND o.id=? AND e.id=?", accountID, orderID, recordID).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	rows, err := executor.QueryContext(ctx, "SELECT "+executionFeeColumns+" FROM "+quoted(s.feeTable)+" WHERE order_id=? AND event_id=? ORDER BY id", orderID, recordID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanExecutionFee)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}
func prefixed(alias, columns string) string {
	return alias + "." + strings.ReplaceAll(columns, ",", ","+alias+".")
}
func readRows[T any](rows *sql.Rows, scan func(scanner) (*T, error)) (values []T, err error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	for rows.Next() {
		v, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		values = append(values, *v)
	}
	return values, rows.Err()
}
func (s *Store) loadHistory(ctx context.Context, tx *sql.Tx, orderID uint64) ([]OnchainEvent, error) {
	rows, err := tx.QueryContext(ctx, "SELECT "+onchainEventColumns+" FROM "+quoted(s.onchainEventTable)+" WHERE order_id=? ORDER BY sequence FOR UPDATE", orderID)
	if err != nil {
		return nil, err
	}
	history, err := readRows(rows, scanOnchainEvent)
	if err != nil {
		return nil, err
	}
	positions := map[uint64]int{}
	for i, r := range history {
		positions[r.Event.ID] = i
	}
	rows, err = tx.QueryContext(ctx, "SELECT "+executionFeeColumns+" FROM "+quoted(s.feeTable)+" WHERE order_id=? ORDER BY id FOR UPDATE", orderID)
	if err != nil {
		return nil, err
	}
	fees, err := readRows(rows, scanExecutionFee)
	if err != nil {
		return nil, err
	}
	for _, f := range fees {
		pos, ok := positions[f.EventID]
		if !ok || f.ExecutionRecordID != history[pos].Event.ExecutionRecordID {
			return nil, ErrConflict
		}
		history[pos].Fees = append(history[pos].Fees, f)
	}
	return history, nil
}
