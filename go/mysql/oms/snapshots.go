package oms

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"

	api "github.com/k4k3ru-hub/storage/go/api"
)

func projectExecution(id uint64, history []OnchainEvent, projection *OrderProjection) (*Execution, error) {
	var snapshot *Execution
	var result *Event
	completeness := map[uint64]bool{}
	for _, r := range history {
		e := r.Event
		if e.ExecutionRecordID != id {
			continue
		}
		if e.EventType == EventTypeSubmissionAccepted {
			snapshot = &Execution{ID: id, OrderID: e.OrderID, ExecutionSystem: e.ExecutionSystem, ExecutionID: *e.ExecutionID, EventFamily: "onchain", Venue: *e.Venue, Status: ExecutionStatusPending, Quantity: e.RequestedQuantity, FilledQuantity: "0", CreatedAt: e.RecordedAt}
		}
		if snapshot == nil {
			return nil, fmt.Errorf("failed to project oms execution: %w: acceptance=null", ErrConflict)
		}
		snapshot.LastEventSequence = e.Sequence
		snapshot.UpdatedAt = e.RecordedAt
		switch e.EventType {
		case EventTypeSucceeded, EventTypeFailed, EventTypeSubmissionRejected:
			v := e
			result = &v
		case EventTypeReversed:
			result = nil
		}
		if e.FeesComplete != nil {
			target := e.ID
			if e.EventType == EventTypeFeesRecorded || e.EventType == EventTypeFeesAdjusted {
				target = *e.ReferenceEventID
			}
			completeness[target] = *e.FeesComplete
		}
	}
	if snapshot == nil {
		return nil, fmt.Errorf("failed to project oms execution: %w: execution_id=invalid", ErrConflict)
	}
	total := new(big.Rat)
	scale := 0
	complete := result != nil && completeness[result.ID]
	for _, fill := range projection.ActiveFills {
		if fill.ExecutionRecordID != id {
			continue
		}
		amount, _ := new(big.Rat).SetString(*fill.OrderQuantity)
		total.Add(total, amount)
		if i := strings.IndexByte(*fill.OrderQuantity, '.'); i >= 0 && len(*fill.OrderQuantity)-i-1 > scale {
			scale = len(*fill.OrderQuantity) - i - 1
		}
		complete = complete && completeness[fill.ID]
	}
	snapshot.FilledQuantity = total.FloatString(scale)
	if scale > 0 {
		snapshot.FilledQuantity = strings.TrimRight(strings.TrimRight(snapshot.FilledQuantity, "0"), ".")
	}
	var err error
	snapshot.FilledCounterQuantity, err = sumCounter(projection.counterAsset, projection.ActiveFills, id)
	if err != nil {
		return nil, fmt.Errorf("failed to project oms execution: %w", err)
	}
	snapshot.FeesComplete = complete
	if result != nil {
		at := utc(result.OccurredAt)
		snapshot.CompletedAt = &at
		switch result.EventType {
		case EventTypeSucceeded:
			snapshot.Status = ExecutionStatusSucceeded
		case EventTypeFailed:
			snapshot.Status = ExecutionStatusFailed
		case EventTypeSubmissionRejected:
			snapshot.Status = ExecutionStatusRejected
		}
	}
	if total.Sign() > 0 {
		snapshot.Status = ExecutionStatusPartiallyFilled
		if snapshot.Quantity != nil {
			quantity, _ := new(big.Rat).SetString(*snapshot.Quantity)
			if total.Cmp(quantity) >= 0 {
				snapshot.Status = ExecutionStatusFilled
			}
		}
	}
	return snapshot, nil
}

// SelectExecution retrieves one owned execution snapshot, without loading its event history.
//
// Version:
//   - 2026-09-26: Read the mutable execution snapshot.
func (s *Store) SelectExecution(ctx context.Context, q api.Executor, accountID, orderID, executionID uint64) (*Execution, error) {
	const op = "failed to select oms execution"
	if err := s.guard(ctx, q); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if err := identity(accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	v, err := scanExecution(q.QueryRowContext(ctx, "SELECT "+prefixed("e", executionColumns)+" FROM "+quoted(s.executionTable)+" e JOIN "+quoted(s.orderTable)+" o ON o.id=e.order_id WHERE o.account_id=? AND e.order_id=? AND e.id=?", accountID, orderID, executionID))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return v, nil
}

// ListExecutions lists owned execution snapshots after an exclusive ID cursor.
//
// Version:
//   - 2026-09-26: Separate execution snapshots from adapter events.
func (s *Store) ListExecutions(ctx context.Context, q api.Executor, accountID, orderID, afterID uint64, limit int) ([]Execution, error) {
	const op = "failed to list oms executions"
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("%s: %w", op, invalid("limit", "out_of_range"))
	}
	if _, err := s.SelectOrder(ctx, q, accountID, orderID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	rows, err := q.QueryContext(ctx, "SELECT "+executionColumns+" FROM "+quoted(s.executionTable)+" WHERE order_id=? AND id>? ORDER BY id LIMIT ?", orderID, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanExecution)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

// ListExecutionFees lists all cost components and adjustments owned by an execution snapshot.
// Each component also identifies its source event; amounts are not converted or netted.
//
// Version:
//   - 2026-09-26: Retain execution ownership independently of source events.
func (s *Store) ListExecutionFees(ctx context.Context, q api.Executor, accountID, orderID, executionID uint64) ([]ExecutionFee, error) {
	const op = "failed to list oms execution fees"
	if _, err := s.SelectExecution(ctx, q, accountID, orderID, executionID); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	rows, err := q.QueryContext(ctx, "SELECT "+executionFeeColumns+" FROM "+quoted(s.feeTable)+" WHERE order_id=? AND execution_record_id=? ORDER BY id", orderID, executionID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	values, err := readRows(rows, scanExecutionFee)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return values, nil
}

func (s *Store) loadSnapshots(ctx context.Context, tx *sql.Tx, orderID uint64) ([]Execution, error) {
	rows, err := tx.QueryContext(ctx, "SELECT "+executionColumns+" FROM "+quoted(s.executionTable)+" WHERE order_id=? ORDER BY id FOR UPDATE", orderID)
	if err != nil {
		return nil, err
	}
	return readRows(rows, scanExecution)
}
func executionEqual(a, b Execution) bool {
	return a.ID == b.ID && a.OrderID == b.OrderID && a.ExecutionSystem == b.ExecutionSystem && a.ExecutionID == b.ExecutionID && a.EventFamily == b.EventFamily && a.Venue == b.Venue && a.Status == b.Status && sameString(a.Quantity, b.Quantity) && a.FilledQuantity == b.FilledQuantity && sameString(a.FilledCounterQuantity, b.FilledCounterQuantity) && a.FeesComplete == b.FeesComplete && a.LastEventSequence == b.LastEventSequence && sameTime(a.CompletedAt, b.CompletedAt) && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt)
}
