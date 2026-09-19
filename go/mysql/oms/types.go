package oms

import (
	"errors"
	"time"

	generator "github.com/k4k3ru-hub/storage/go/internal/generator"
)

type OrderStatus string

const (
	OrderStatusPending         OrderStatus = "pending"
	OrderStatusProcessing      OrderStatus = "processing"
	OrderStatusPartiallyFilled OrderStatus = "partially_filled"
	OrderStatusFilled          OrderStatus = "filled"
	OrderStatusCanceled        OrderStatus = "canceled"
	OrderStatusExpired         OrderStatus = "expired"
	OrderStatusFailed          OrderStatus = "failed"
	OrderStatusRejected        OrderStatus = "rejected"
)

type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusSucceeded ExecutionStatus = "succeeded"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusReversed  ExecutionStatus = "reversed"
	ExecutionStatusRejected  ExecutionStatus = "rejected"
)

type ExecutionType string

const (
	ExecutionTypePrepared  ExecutionType = "prepared"
	ExecutionTypeSubmitted ExecutionType = "submitted"
	ExecutionTypeFilled    ExecutionType = "filled"
)

type ExecutionPurpose string

const (
	ExecutionPurposeApproval ExecutionPurpose = "approval"
	ExecutionPurposeTrade    ExecutionPurpose = "trade"
	DomainOnchainAMMPool                      = "onchain-amm-pool"
)

var (
	ErrDuplicate         = errors.New("failed to write oms record: duplicate")
	ErrConflict          = errors.New("failed to update oms record: state conflict")
	ErrInvalidParameter  = errors.New("failed to validate oms parameters")
	orderIDGenerator     generator.ID
	executionIDGenerator generator.ID
)

type OrderState struct {
	Status         OrderStatus
	Quantity       *string
	FilledQuantity string
	CompletedAt    *time.Time
}
type Order struct {
	ID, AccountID                                                  uint64
	ParentOrderID                                                  *uint64
	AccountRef, AssetClass, Domain, Venue, Symbol, Side, OrderType string
	OrderState
	LimitPrice, TakeProfitType, TakeProfitValue, StopLossType, StopLossValue *string
	IdempotencyKey                                                           []byte
	ExpiresAt                                                                *time.Time
	CreatedAt, UpdatedAt                                                     time.Time
}
type OnchainAMMPoolSwap struct {
	OrderID                                                    uint64
	ChainFamily, Chain, Network, PoolID, TokenInID, TokenOutID string
	TokenInDecimals, TokenOutDecimals                          uint16
	SwapKind, Signer, Recipient                                string
	MaximumSlippageBPS                                         uint16
	ExecutionTTLMS                                             uint64
	CreatedAt, UpdatedAt                                       time.Time
}
type ExecutionState struct {
	Status                    ExecutionStatus
	ExecutionID               *string
	Quantity, CounterQuantity *string
	SourceVersion             *uint64
	OccurredAt                time.Time
	ExpiresAt                 *time.Time
}
type Execution struct {
	ID, OrderID, AttemptNumber uint64
	ExecType                   ExecutionType
	Purpose                    ExecutionPurpose
	ExecutionState
	RecordKey            []byte
	ExecutionSystem      string
	CreatedAt, UpdatedAt time.Time
}

// GenerateOrderID generates an OMS order identifier using the storage generator.
//
// Version:
//   - 2026-09-20: Added.
func GenerateOrderID() uint64 { return orderIDGenerator.Generate() }

// GenerateExecutionID generates an OMS execution-record identifier using the storage generator.
//
// Version:
//   - 2026-09-20: Added.
func GenerateExecutionID() uint64 { return executionIDGenerator.Generate() }
