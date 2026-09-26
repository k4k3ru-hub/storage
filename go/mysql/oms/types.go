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
)

type ExecutionType string

const (
	ExecutionTypeSubmissionAccepted ExecutionType = "submission_accepted"
	ExecutionTypeSubmitted          ExecutionType = "submitted"
	ExecutionTypeSubmissionRejected ExecutionType = "submission_rejected"
	ExecutionTypeSucceeded          ExecutionType = "execution_succeeded"
	ExecutionTypeFailed             ExecutionType = "execution_failed"
	ExecutionTypeReversed           ExecutionType = "execution_reversed"
	ExecutionTypeFilled             ExecutionType = "filled"
	ExecutionTypeFillReversed       ExecutionType = "fill_reversed"
	ExecutionTypeFillCorrected      ExecutionType = "fill_corrected"
	ExecutionTypeFeesRecorded       ExecutionType = "fees_recorded"
	ExecutionTypeFeesAdjusted       ExecutionType = "fees_adjusted"
	ExecutionTypeEvidenceRecorded   ExecutionType = "evidence_recorded"
	ExecutionTypeOrderCanceled      ExecutionType = "order_canceled"
	ExecutionTypeOrderExpired       ExecutionType = "order_expired"
	ExecutionTypeOrderRejected      ExecutionType = "order_rejected"
	ExecutionTypeOrderFailed        ExecutionType = "order_failed"
)

var (
	ErrDuplicate         = errors.New("failed to write oms record: duplicate")
	ErrConflict          = errors.New("failed to append oms record: conflicting history")
	ErrInvalidParameter  = errors.New("failed to validate oms parameters")
	orderIDGenerator     generator.ID
	executionIDGenerator generator.ID
	feeIDGenerator       generator.ID
)

type OrderState struct {
	Status                OrderStatus
	Quantity              *string
	FilledQuantity        string
	CompletedAt           *time.Time
	LastExecutionSequence uint64
}
type Order struct {
	ID, AccountID                                           uint64
	ParentOrderID                                           *uint64
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
type Execution struct {
	ID, OrderID, Sequence                                      uint64
	ExecType                                                   ExecutionType
	SubmissionRecordID, ReferenceRecordID                      *uint64
	RecordKey                                                  []byte
	ExecutionSystem                                            string
	ExecutionID, Venue                                         *string
	Quantity, CounterQuantity, QuantityAssetID, CounterAssetID *string
	QuantityDecimals, CounterDecimals                          *uint16
	OrderQuantity, Price                                       *string
	FeesComplete                                               *bool
	SourceVersion                                              *uint64
	OccurredAt, RecordedAt                                     time.Time
}
type OnchainDetail struct {
	ExecutionRecordID, OrderID                            uint64
	ExecType                                              ExecutionType
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
	ID, OrderID, ExecutionRecordID               uint64
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

// ExecutionRecord is the atomic append unit. Zero child IDs are assigned by AppendExecution.
type ExecutionRecord struct {
	Execution Execution
	Onchain   *OnchainDetail
	Fees      []ExecutionFee
}
type AppendResult struct {
	ExecutionID uint64
	Sequence    uint64
	Duplicate   bool
	State       OrderState
}

// GenerateOrderID generates an OMS order identifier.
//
// Version:
//   - 2026-09-20: Added.
func GenerateOrderID() uint64 { return orderIDGenerator.Generate() }

// GenerateExecutionID generates an immutable execution-record identifier.
//
// Version:
//   - 2026-09-26: Distinguish record IDs from public submission IDs.
func GenerateExecutionID() uint64 { return executionIDGenerator.Generate() }

// GenerateFeeID generates a fee-component identifier.
//
// Version:
//   - 2026-09-26: Added.
func GenerateFeeID() uint64 { return feeIDGenerator.Generate() }
