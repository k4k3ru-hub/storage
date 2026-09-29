package oms

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	api "github.com/k4k3ru-hub/storage/go/api"
)

type PerpetualEvidence struct {
	Network, TradingAccount, MarketID        string
	ClientOrderID, VenueOrderID, VenueStatus *string
	VenueStatusTimeMS                        *uint64
	ObservedAt                               time.Time
	FillID                                   *string
	FillIdentityHash                         []byte
	FillTimeMS                               *uint64
	StartPosition, Direction, Liquidity      *string
	ClosedPnL, ClosedPnLAssetID              *string
	ClosedPnLDecimals                        *uint16
	ProtocolVersion                          uint16
	ProtocolData                             json.RawMessage
}

type PerpetualEvent struct {
	Event     Event
	Perpetual PerpetualEvidence
	Fees      []ExecutionFee
}

// PerpetualCompletion carries adapter-verified quantity evidence, never inferred from a timeout.
type PerpetualCompletion struct {
	ExpectedQuantity string `json:"expectedQuantity"`
	FillsComplete    bool   `json:"fillsComplete"`
	FeesComplete     bool   `json:"feesComplete"`
}

// NewStoreWithPerpetual composes explicit Perpetual history alongside the existing store tables.
// An empty pnlTable retains the four-table store's PnL behavior.
//
// Version:
//   - 2026-09-29: Added.
func NewStoreWithPerpetual(orderTable, executionTable, onchainTable, feeTable, pnlTable, perpetualTable string) (*Store, error) {
	s, err := NewStore(orderTable, executionTable, onchainTable, feeTable)
	if err != nil {
		return nil, fmt.Errorf("failed to create perpetual oms store: %w", err)
	}
	if pnlTable != "" {
		s, err = NewStoreWithPnL(orderTable, executionTable, onchainTable, feeTable, pnlTable)
		if err != nil {
			return nil, fmt.Errorf("failed to create perpetual oms store: %w", err)
		}
	}
	if _, err := NewStore(orderTable, executionTable, onchainTable, perpetualTable); err != nil {
		return nil, fmt.Errorf("failed to create perpetual oms store: %w", err)
	}
	if strings.EqualFold(perpetualTable, feeTable) || strings.EqualFold(perpetualTable, pnlTable) {
		return nil, fmt.Errorf("failed to create perpetual oms store: %w", invalid("perpetual_table", "invalid"))
	}
	s.perpetualEventTable = perpetualTable
	return s, nil
}

// Validate validates typed Perpetual evidence and common fill amounts.
//
// Version:
//   - 2026-09-29: Accept printable ASCII direction labels containing spaces.
//   - 2026-09-29: Added.
func (r PerpetualEvent) Validate() error {
	e, d := r.Event, r.Perpetual
	if err := e.Validate(); err != nil {
		return fmt.Errorf("failed to validate perpetual event: %w", err)
	}
	for _, f := range []struct {
		name, value string
		max         int
	}{{"network", d.Network, 64}, {"trading_account", d.TradingAccount, 255}, {"market_id", d.MarketID, 255}} {
		if err := textValue(f.name, f.value, f.max, true); err != nil {
			return err
		}
	}
	if e.Venue == nil {
		return invalid("venue", "null")
	}
	for _, f := range []struct {
		name  string
		value *string
		max   int
	}{{"client_order_id", d.ClientOrderID, 255}, {"venue_order_id", d.VenueOrderID, 255}, {"venue_status", d.VenueStatus, 64}, {"fill_id", d.FillID, 255}} {
		if err := optionalText(f.name, f.value, f.max); err != nil {
			return err
		}
	}
	if d.Direction != nil {
		if err := textValue("direction", *d.Direction, 64, false); err != nil {
			return err
		}
		for _, c := range *d.Direction {
			if c < 32 || c > 126 {
				return invalid("direction", "invalid")
			}
		}
	}
	if err := validTime("observed_at", d.ObservedAt); err != nil {
		return err
	}
	if d.ProtocolVersion == 0 {
		return invalid("protocol_version", "empty")
	}
	if err := jsonObject("protocol_data", d.ProtocolData, true); err != nil {
		return err
	}
	if e.EventType == EventTypeSubmissionAccepted && (e.RequestedQuantity == nil || d.ClientOrderID == nil) {
		return invalid("acceptance", "null")
	}
	if e.EventType == EventTypeFilled {
		if len(d.FillIdentityHash) != 32 {
			return invalid("fill_identity", "invalid")
		}
	} else if d.FillIdentityHash != nil {
		return invalid("fill_identity", "invalid")
	}
	if isFill(e.EventType) {
		if d.FillID == nil || d.FillTimeMS == nil || *d.FillTimeMS == 0 || e.OrderCounterQuantity == nil || e.Price == nil {
			return invalid("fill", "null")
		}
		if !sameString(e.Quantity, e.OrderQuantity) || !sameString(e.CounterQuantity, e.OrderCounterQuantity) {
			return invalid("fill_contribution", "invalid")
		}
		quantity, quantityOK := new(big.Rat).SetString(*e.Quantity)
		price, priceOK := new(big.Rat).SetString(*e.Price)
		counter, counterOK := new(big.Rat).SetString(*e.CounterQuantity)
		if !quantityOK || !priceOK || !counterOK || price.Sign() <= 0 || new(big.Rat).Mul(quantity, price).Cmp(counter) != 0 {
			return invalid("fill_notional", "invalid")
		}
		if d.Liquidity != nil && *d.Liquidity != "maker" && *d.Liquidity != "taker" && *d.Liquidity != "unknown" {
			return invalid("liquidity", "invalid")
		}
		if d.StartPosition != nil {
			if err := unitDecimal("start_position", *d.StartPosition, *e.QuantityDecimals, true); err != nil {
				return err
			}
		}
	} else if d.FillID != nil || d.FillTimeMS != nil || d.StartPosition != nil || d.Direction != nil || d.Liquidity != nil || d.ClosedPnL != nil {
		return invalid("fill", "invalid")
	}
	if d.ClosedPnL == nil {
		if d.ClosedPnLAssetID != nil || d.ClosedPnLDecimals != nil {
			return invalid("closed_pnl", "invalid")
		}
	} else {
		if d.ClosedPnLAssetID == nil || d.ClosedPnLDecimals == nil {
			return invalid("closed_pnl", "null")
		}
		if err := textValue("closed_pnl_asset_id", *d.ClosedPnLAssetID, 65535, true); err != nil {
			return err
		}
		if err := unitDecimal("closed_pnl", *d.ClosedPnL, *d.ClosedPnLDecimals, true); err != nil {
			return err
		}
	}
	return nil
}

func normalizePerpetualRecord(r PerpetualEvent, sequence uint64, old *PerpetualEvent) (PerpetualEvent, error) {
	var existing *OnchainEvent
	if old != nil {
		existing = &OnchainEvent{Event: old.Event, Fees: old.Fees}
	}
	// Only the common event/fee normalization is reused; no onchain evidence is constructed.
	v, err := normalizeRecord(OnchainEvent{Event: r.Event, Fees: r.Fees}, sequence, existing)
	if err != nil {
		return r, err
	}
	r.Event, r.Fees = v.Event, v.Fees
	r.Perpetual.ObservedAt = utc(r.Perpetual.ObservedAt)
	r.Perpetual.ProtocolData, err = canonicalJSON(r.Perpetual.ProtocolData)
	if err != nil {
		return r, err
	}
	return r, r.Validate()
}

func perpetualRecordsEqual(a, b PerpetualEvent) (bool, error) {
	equal, err := recordsEqual(OnchainEvent{Event: a.Event, Fees: a.Fees}, OnchainEvent{Event: b.Event, Fees: b.Fees})
	if err != nil || !equal {
		return equal, err
	}
	// Reobserving the same immutable fact changes only its observation time.
	a.Perpetual.ObservedAt, b.Perpetual.ObservedAt = time.Time{}, time.Time{}
	a.Perpetual.ProtocolData, err = canonicalJSON(a.Perpetual.ProtocolData)
	if err != nil {
		return false, err
	}
	b.Perpetual.ProtocolData, err = canonicalJSON(b.Perpetual.ProtocolData)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(a.Perpetual, b.Perpetual), nil
}

func validatePerpetualChildren(r PerpetualEvent, history []PerpetualEvent) error {
	common := make([]OnchainEvent, len(history))
	for i, old := range history {
		common[i] = OnchainEvent{Event: old.Event, Fees: old.Fees}
		if !sameString(r.Event.Venue, old.Event.Venue) || r.Perpetual.Network != old.Perpetual.Network || r.Perpetual.TradingAccount != old.Perpetual.TradingAccount || r.Perpetual.MarketID != old.Perpetual.MarketID || !sameString(r.Perpetual.ClientOrderID, old.Perpetual.ClientOrderID) || !compatibleString(r.Perpetual.VenueOrderID, old.Perpetual.VenueOrderID) {
			return fmt.Errorf("failed to validate perpetual scope: %w", ErrConflict)
		}
		if r.Event.EventType == EventTypeFilled && bytes.Equal(r.Perpetual.FillIdentityHash, old.Perpetual.FillIdentityHash) {
			return fmt.Errorf("failed to validate perpetual fill identity: %w", ErrConflict)
		}
	}
	for _, fee := range r.Fees {
		if fee.AssetNamespace != "venue" || fee.FeeType != "trading" || fee.AccountingTreatment != "additional" {
			return invalid("fee_scope", "invalid")
		}
	}
	return validateChildren(OnchainEvent{Event: r.Event, Fees: r.Fees}, common)
}

func projectPerpetualExecution(id uint64, history []PerpetualEvent, projection *OrderProjection) (*Execution, error) {
	var snapshot *Execution
	complete := false
	fees := map[uint64]bool{}
	for _, r := range history {
		e, d := r.Event, r.Perpetual
		if e.ExecutionRecordID != id {
			return nil, fmt.Errorf("failed to project perpetual execution: %w: executions=invalid", ErrConflict)
		}
		if e.EventType == EventTypeSubmissionAccepted {
			if snapshot != nil {
				return nil, ErrConflict
			}
			snapshot = &Execution{ID: id, OrderID: e.OrderID, ExecutionSystem: e.ExecutionSystem, ExecutionID: *e.ExecutionID, EventFamily: "perpetual", Venue: *e.Venue, ClientOrderID: d.ClientOrderID, Quantity: e.RequestedQuantity, CreatedAt: e.RecordedAt}
		}
		if snapshot == nil {
			return nil, ErrConflict
		}
		if d.VenueOrderID != nil {
			if !compatibleString(snapshot.VenueOrderID, d.VenueOrderID) {
				return nil, ErrConflict
			}
			snapshot.VenueOrderID = d.VenueOrderID
		}
		if e.FeesComplete != nil {
			target := e.ID
			if e.EventType == EventTypeFeesRecorded || e.EventType == EventTypeFeesAdjusted {
				target = *e.ReferenceEventID
			}
			fees[target] = *e.FeesComplete
		}
		var evidence struct {
			Completion *PerpetualCompletion `json:"completion"`
		}
		if err := json.Unmarshal(d.ProtocolData, &evidence); err != nil {
			return nil, fmt.Errorf("failed to decode perpetual completion: %w", err)
		}
		if c := evidence.Completion; c != nil {
			if e.EventType != EventTypeEvidenceRecorded || decimal("expected_quantity", c.ExpectedQuantity, true) != nil {
				return nil, invalid("completion", "invalid")
			}
			want, _ := new(big.Rat).SetString(c.ExpectedQuantity)
			have, _ := new(big.Rat).SetString(projection.State.FilledQuantity)
			complete = c.FillsComplete && c.FeesComplete && want.Cmp(have) == 0
		}
		snapshot.LastEventSequence, snapshot.UpdatedAt = e.Sequence, e.RecordedAt
	}
	if snapshot == nil {
		return nil, ErrConflict
	}
	snapshot.Status = ExecutionStatus(projection.State.Status)
	snapshot.FilledQuantity, snapshot.FilledCounterQuantity = projection.State.FilledQuantity, projection.State.FilledCounterQuantity
	snapshot.CompletedAt = projection.State.CompletedAt
	for _, fill := range projection.ActiveFills {
		complete = complete && fees[fill.ID]
	}
	snapshot.FeesComplete = complete && snapshot.CompletedAt != nil
	return snapshot, nil
}

// ListPerpetualEvents lists owned immutable facts using the shared order sequence cursor.
//
// Version:
//   - 2026-09-29: Added.
func (s *Store) ListPerpetualEvents(ctx context.Context, q api.Executor, accountID, orderID, afterSequence uint64, limit int) ([]PerpetualEvent, error) {
	if limit < 1 || limit > 200 {
		return nil, invalid("limit", "out_of_range")
	}
	o, err := s.SelectOrder(ctx, q, accountID, orderID)
	if err != nil {
		return nil, fmt.Errorf("failed to list perpetual events: %w", err)
	}
	if s.perpetualEventTable == "" || o.Domain != DomainPerpetual {
		return nil, fmt.Errorf("failed to list perpetual events: %w: event_family=invalid", ErrConflict)
	}
	rows, err := q.QueryContext(ctx, "SELECT "+perpetualEventColumns+" FROM "+quoted(s.perpetualEventTable)+" WHERE order_id=? AND sequence>? ORDER BY sequence LIMIT ?", orderID, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list perpetual events: %w", err)
	}
	values, err := readRows(rows, scanPerpetualEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to list perpetual events: %w", err)
	}
	return values, nil
}

// SelectPerpetualEventByKey reads an owned immutable event with its fee components.
//
// Version:
//   - 2026-09-29: Added.
func (s *Store) SelectPerpetualEventByKey(ctx context.Context, q api.Executor, accountID, orderID uint64, key []byte) (*PerpetualEvent, error) {
	o, err := s.SelectOrder(ctx, q, accountID, orderID)
	if err != nil {
		return nil, fmt.Errorf("failed to select perpetual event: %w", err)
	}
	if s.perpetualEventTable == "" || o.Domain != DomainPerpetual {
		return nil, fmt.Errorf("failed to select perpetual event: %w", ErrConflict)
	}
	if err := binaryKey("record_key", key); err != nil {
		return nil, fmt.Errorf("failed to select perpetual event: %w", err)
	}
	v, err := scanPerpetualEvent(q.QueryRowContext(ctx, "SELECT "+perpetualEventColumns+" FROM "+quoted(s.perpetualEventTable)+" WHERE order_id=? AND record_key=?", orderID, key))
	if err != nil {
		return nil, fmt.Errorf("failed to select perpetual event: %w", err)
	}
	v.Fees, err = s.ListEventFees(ctx, q, accountID, orderID, v.Event.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to select perpetual event: %w", err)
	}
	return v, nil
}

func (s *Store) loadPerpetualHistory(ctx context.Context, tx *sql.Tx, orderID uint64) ([]PerpetualEvent, error) {
	rows, err := tx.QueryContext(ctx, "SELECT "+perpetualEventColumns+" FROM "+quoted(s.perpetualEventTable)+" WHERE order_id=? ORDER BY sequence FOR UPDATE", orderID)
	if err != nil {
		return nil, err
	}
	history, err := readRows(rows, scanPerpetualEvent)
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
		if !ok || history[pos].Event.ExecutionRecordID != f.ExecutionRecordID {
			return nil, ErrConflict
		}
		history[pos].Fees = append(history[pos].Fees, f)
	}
	return history, nil
}
