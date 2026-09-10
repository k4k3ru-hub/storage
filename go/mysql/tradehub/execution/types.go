package execution

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	k4k3ruStorageGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
)

var executionLegIDGenerator = new(k4k3ruStorageGenerator.ID)

type Status uint8

const (
	StatusPrepared Status = iota + 1
	StatusSubmitting
	StatusSubmitted
	StatusPartiallyCompleted
	StatusCompleted
	StatusFailed
	StatusExpired
)

type LegCategory uint8

const (
	LegCategoryOnchainTransaction LegCategory = iota + 1
	LegCategoryCEXOrder
	LegCategoryDEXOrder
)

type LegStatus uint8

const (
	LegStatusPrepared LegStatus = iota + 1
	LegStatusAwaitingSignature
	LegStatusSubmitting
	LegStatusSubmitted
	LegStatusConfirmed
	LegStatusFailed
	LegStatusExpired
)

type Execution struct {
	ID                  string
	Status              Status
	Kind                string
	RequestSnapshot     json.RawMessage
	ConditionsSnapshot  json.RawMessage
	OpportunitySnapshot json.RawMessage
	ResultSnapshot      json.RawMessage
	PreparedAt          time.Time
	ExpiresAt           time.Time
	CompletedAt         *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Leg struct {
	ID          uint64
	ExecutionID string
	LegIndex    uint16
	Category    LegCategory
	Status      LegStatus
	Venue       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type OnchainTransaction struct {
	ExecutionLegID      uint64
	ChainFamily         string
	Chain               string
	Network             string
	Signer              string
	PayloadDigest       string
	TransactionID       *string
	BlockNumber         *string
	GasUsed             *string
	FeeAmount           *string
	FeeAsset            *string
	SubmissionStartedAt *time.Time
	SubmittedAt         *time.Time
	ConfirmedAt         *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type ExecutionInsertParams struct {
	ID                  string
	Status              Status
	Kind                string
	RequestSnapshot     json.RawMessage
	ConditionsSnapshot  json.RawMessage
	OpportunitySnapshot json.RawMessage
	PreparedAt          time.Time
	ExpiresAt           time.Time
	CreatedAt           time.Time
}

type LegInsertParams struct {
	ID          uint64
	ExecutionID string
	LegIndex    uint16
	Category    LegCategory
	Status      LegStatus
	Venue       string
	CreatedAt   time.Time
}

type OnchainTransactionInsertParams struct {
	ExecutionLegID uint64
	ChainFamily    string
	Chain          string
	Network        string
	Signer         string
	PayloadDigest  string
	CreatedAt      time.Time
}

// GenerateExecutionLegID generates an execution leg ID.
//
// Returns:
//   - Generated execution leg ID.
//
// Version:
//   - 2026-09-10: Added.
func GenerateExecutionLegID() uint64 {
	return executionLegIDGenerator.Generate()
}

// Validate validates an execution status.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func (s Status) Validate() error {
	if s < StatusPrepared || s > StatusExpired {
		return fmt.Errorf("failed to validate trade hub execution status: status=invalid")
	}
	return nil
}

// Validate validates an execution leg category.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func (c LegCategory) Validate() error {
	if c < LegCategoryOnchainTransaction || c > LegCategoryDEXOrder {
		return fmt.Errorf("failed to validate trade hub execution leg category: category=invalid")
	}
	return nil
}

// Validate validates an execution leg status.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func (s LegStatus) Validate() error {
	if s < LegStatusPrepared || s > LegStatusExpired {
		return fmt.Errorf("failed to validate trade hub execution leg status: status=invalid")
	}
	return nil
}

// Validate validates execution insertion parameters.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func (p ExecutionInsertParams) Validate() error {
	if strings.TrimSpace(p.ID) == "" || len(p.ID) > 64 {
		return fmt.Errorf("failed to validate trade hub execution insertion parameters: id=invalid")
	}
	if err := p.Status.Validate(); err != nil {
		return fmt.Errorf("failed to validate trade hub execution insertion parameters: %w", err)
	}
	if strings.TrimSpace(p.Kind) == "" || len(p.Kind) > 32 {
		return fmt.Errorf("failed to validate trade hub execution insertion parameters: kind=invalid")
	}
	if !validJSONObject(p.RequestSnapshot) {
		return fmt.Errorf("failed to validate trade hub execution insertion parameters: request_snapshot=invalid")
	}
	if !validOptionalJSONObject(p.ConditionsSnapshot) || !validOptionalJSONObject(p.OpportunitySnapshot) {
		return fmt.Errorf("failed to validate trade hub execution insertion parameters: snapshot=invalid")
	}
	if p.PreparedAt.IsZero() || p.ExpiresAt.IsZero() || !p.ExpiresAt.After(p.PreparedAt) {
		return fmt.Errorf("failed to validate trade hub execution insertion parameters: expires_at=out_of_range")
	}
	return nil
}

// Validate validates execution leg insertion parameters.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func (p *LegInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("failed to validate trade hub execution leg insertion parameters: params=null")
	}
	if p.ID == 0 || strings.TrimSpace(p.ExecutionID) == "" {
		return fmt.Errorf("failed to validate trade hub execution leg insertion parameters: identifier=empty")
	}
	if err := p.Category.Validate(); err != nil {
		return fmt.Errorf("failed to validate trade hub execution leg insertion parameters: %w", err)
	}
	if err := p.Status.Validate(); err != nil {
		return fmt.Errorf("failed to validate trade hub execution leg insertion parameters: %w", err)
	}
	if strings.TrimSpace(p.Venue) == "" || len(p.Venue) > 64 {
		return fmt.Errorf("failed to validate trade hub execution leg insertion parameters: venue=invalid")
	}
	return nil
}

// Validate validates onchain transaction insertion parameters.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func (p OnchainTransactionInsertParams) Validate() error {
	if p.ExecutionLegID == 0 {
		return fmt.Errorf("failed to validate trade hub onchain transaction insertion parameters: execution_leg_id=empty")
	}
	for field, value := range map[string]string{"chain_family": p.ChainFamily, "chain": p.Chain, "network": p.Network, "signer": p.Signer, "payload_digest": p.PayloadDigest} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("failed to validate trade hub onchain transaction insertion parameters: %s=empty", field)
		}
	}
	if len(p.ChainFamily) > 16 || len(p.Chain) > 32 || len(p.Network) > 32 || len(p.Signer) > 128 || len(p.PayloadDigest) > 255 {
		return fmt.Errorf("failed to validate trade hub onchain transaction insertion parameters: value=too_long")
	}
	return nil
}

func validJSONObject(value json.RawMessage) bool {
	if len(value) == 0 || !json.Valid(value) {
		return false
	}
	var object map[string]any
	return json.Unmarshal(value, &object) == nil && object != nil
}

func validOptionalJSONObject(value json.RawMessage) bool {
	return len(value) == 0 || validJSONObject(value)
}
