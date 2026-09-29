package oms

import (
	"encoding/json"
	"errors"
	generator "github.com/k4k3ru-hub/storage/go/internal/generator"
	"time"
)

type OrderStatus string

const (
	OrderStatusPending         OrderStatus = "pending"
	OrderStatusPartiallyFilled OrderStatus = "partially_filled"
	OrderStatusFilled          OrderStatus = "filled"
	OrderStatusCanceled        OrderStatus = "canceled"
	OrderStatusExpired         OrderStatus = "expired"
	OrderStatusFailed          OrderStatus = "failed"
	OrderStatusRejected        OrderStatus = "rejected"
	DomainOnchainAMMPool                   = "onchain-amm-pool"
	DomainPerpetual                        = "perpetual"
)

type EventType string

const (
	EventTypeSubmissionAccepted EventType = "submission_accepted"
	EventTypeSubmitted          EventType = "submitted"
	EventTypeSubmissionRejected EventType = "submission_rejected"
	EventTypeSucceeded          EventType = "execution_succeeded"
	EventTypeFailed             EventType = "execution_failed"
	EventTypeReversed           EventType = "execution_reversed"
	EventTypeFilled             EventType = "filled"
	EventTypeFillReversed       EventType = "fill_reversed"
	EventTypeFillCorrected      EventType = "fill_corrected"
	EventTypeFeesRecorded       EventType = "fees_recorded"
	EventTypeFeesAdjusted       EventType = "fees_adjusted"
	EventTypeEvidenceRecorded   EventType = "evidence_recorded"
	EventTypeOrderCanceled      EventType = "order_canceled"
	EventTypeOrderExpired       EventType = "order_expired"
	EventTypeOrderRejected      EventType = "order_rejected"
	EventTypeOrderFailed        EventType = "order_failed"
)

var (
	ErrDuplicate         = errors.New("failed to write oms record: duplicate")
	ErrConflict          = errors.New("failed to append oms record: conflicting history")
	ErrInvalidParameter  = errors.New("failed to validate oms parameters")
	orderIDGenerator     generator.ID
	executionIDGenerator generator.ID
	eventIDGenerator     generator.ID
	feeIDGenerator       generator.ID
)

type OrderState struct {
	Status                OrderStatus
	Quantity              *string
	FilledQuantity        string
	FilledCounterQuantity *string
	CompletedAt           *time.Time
	LastEventSequence     uint64
}
type Order struct {
	ID, AccountID                                           uint64
	ParentOrderID                                           *uint64
	PositionOrderID                                         *uint64
	AccountRef, AssetClass, Domain, Symbol, Side, OrderType string
	Venue                                                   *string
	OrderState
	LimitPrice, TakeProfitType, TakeProfitValue, StopLossType, StopLossValue *string
	SpecificationVersion                                                     uint16
	Specification                                                            json.RawMessage
	IdempotencyKey                                                           []byte
	ExpiresAt                                                                *time.Time
	CreatedAt, UpdatedAt                                                     time.Time
}
type ExecutionStatus string

const (
	ExecutionStatusPending         ExecutionStatus = "pending"
	ExecutionStatusPartiallyFilled ExecutionStatus = "partially_filled"
	ExecutionStatusFilled          ExecutionStatus = "filled"
	ExecutionStatusSucceeded       ExecutionStatus = "succeeded"
	ExecutionStatusFailed          ExecutionStatus = "failed"
	ExecutionStatusRejected        ExecutionStatus = "rejected"
	ExecutionStatusCanceled        ExecutionStatus = "canceled"
	ExecutionStatusExpired         ExecutionStatus = "expired"
)

// Execution is the current state of one routed execution. Quantities use the order's unit.
type Execution struct {
	ID, OrderID                                      uint64
	ExecutionSystem, ExecutionID, EventFamily, Venue string
	VenueOrderID, ClientOrderID                      *string
	Status                                           ExecutionStatus
	Quantity                                         *string
	FilledQuantity                                   string
	FilledCounterQuantity                            *string
	FeesComplete                                     bool
	LastEventSequence                                uint64
	CompletedAt                                      *time.Time
	CreatedAt, UpdatedAt                             time.Time
}

// Event is the common projection input stored within an adapter's event table.
type Event struct {
	ExecutionRecordID                                          uint64
	RequestedQuantity                                          *string
	ID, OrderID, Sequence                                      uint64
	EventType                                                  EventType
	SubmissionEventID, ReferenceEventID                        *uint64
	RecordKey                                                  []byte
	ExecutionSystem                                            string
	ExecutionID, Venue                                         *string
	Quantity, CounterQuantity, QuantityAssetID, CounterAssetID *string
	QuantityDecimals, CounterDecimals                          *uint16
	OrderQuantity, OrderCounterQuantity, Price                 *string
	FeesComplete                                               *bool
	SourceVersion                                              *uint64
	OccurredAt, RecordedAt                                     time.Time
}
type OnchainEvidence struct {
	EventID, OrderID                                      uint64
	EventType                                             EventType
	ChainFamily, Chain, Network, TxID                     string
	LedgerUnit                                            *string
	LedgerSequence                                        *uint64
	LedgerID                                              *string
	TxPosition                                            *uint64
	EventPosition, EmitterID, PoolID                      *string
	SignerID, RecipientID, PayloadDigest, PayloadEncoding *string
	TxPayload                                             []byte `json:"-"`
	FinalityLevel                                         *string
	ProtocolVersion                                       uint16
	ProtocolData                                          json.RawMessage
}
type ExecutionFee struct {
	ID, OrderID, ExecutionRecordID, EventID      uint64
	AdjustmentOfFeeID                            *uint64
	RecordKey                                    []byte
	FeeType, AccountingTreatment, AssetNamespace string
	AssetChain, AssetNetwork                     *string
	AssetID                                      string
	AssetDecimals                                uint16
	Amount, SourceReference                      string
	SourceVersion                                *uint64
	OccurredAt, RecordedAt                       time.Time
}

// OnchainEvent is the atomic append unit. Zero child IDs are assigned by AppendOnchainEvent.
type OnchainEvent struct {
	Event   Event
	Onchain *OnchainEvidence
	Fees    []ExecutionFee
}
type AppendResult struct {
	EventID           uint64
	ExecutionRecordID uint64
	Execution         Execution
	Sequence          uint64
	Duplicate         bool
	State             OrderState
}

// GenerateOrderID generates an OMS order identifier.
//
// Version:
//   - 2026-09-20: Added.
func GenerateOrderID() uint64 { return orderIDGenerator.Generate() }

// GenerateEventID generates an immutable event identifier.
//
// Version:
//   - 2026-09-26: Distinguish event IDs from execution snapshot IDs.
func GenerateEventID() uint64 { return eventIDGenerator.Generate() }

// GenerateFeeID generates a fee-component identifier.
//
// Version:
//   - 2026-09-26: Added.
func GenerateFeeID() uint64 { return feeIDGenerator.Generate() }

// GenerateExecutionID generates a stable execution snapshot identifier.
//
// Version:
//   - 2026-09-26: Allocate snapshots separately from their events.
func GenerateExecutionID() uint64 { return executionIDGenerator.Generate() }
