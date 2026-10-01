package ammpool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	MaxLPCheckpointBytes       = 512 << 10
	MaxLPCheckpointSourceBytes = 64 << 20
	MaxLPCheckpointPools       = 512
	MaxLPCheckpointReadBytes   = 4 << 20
	MaxLPCheckpointBatch       = 32
)

var (
	ErrLPCheckpointConflict = errors.New("lp checkpoint conflict")
	ErrLPCheckpointCapacity = errors.New("lp checkpoint capacity exceeded")
)

type LPCheckpointPosition struct {
	Kind   string
	Number uint64
	ID     string
	Index  *string
}

type LPCheckpoint struct {
	Pool            Identity
	CreationEventID [32]byte
	FormatVersion   uint16
	Position        LPCheckpointPosition
	Payload         json.RawMessage
	UpdatedAt       time.Time
}

type LPCheckpointRef struct {
	Pool     Identity
	Revision uint64
}

type LPCheckpointMetadata struct {
	Ref             LPCheckpointRef
	CreationEventID [32]byte
	FormatVersion   uint16
	Position        LPCheckpointPosition
	PayloadBytes    uint32
	UpdatedAt       time.Time
	PoolCreatedAt   time.Time
	PoolCanonical   bool
}

type LPCheckpointSaveParams struct {
	Source                 Source
	ExpectedCursorRevision uint64
	ExpectedParentRevision uint64
	Checkpoint             LPCheckpoint
}

type LPCheckpointSaveResult struct {
	Cursor   Cursor
	Metadata LPCheckpointMetadata
}

type LPCheckpointListParams struct {
	Source      Source
	AfterPoolID *[32]byte
	Limit       int
}

type LPCheckpointPage struct {
	Items       []LPCheckpointMetadata
	NextAfterID *[32]byte
}

type LPCheckpointEntry struct {
	Metadata   LPCheckpointMetadata
	Checkpoint LPCheckpoint
}

func validateLPCheckpoint(c LPCheckpoint) error {
	if err := c.Pool.Validate(); err != nil {
		return fmt.Errorf("failed to validate lp checkpoint: %w", err)
	}
	if c.CreationEventID == ([32]byte{}) || c.FormatVersion == 0 || !senderTime(c.UpdatedAt) {
		return fmt.Errorf("failed to validate lp checkpoint: identity_or_version_or_time=invalid")
	}
	if !validText(c.Position.Kind, 16) || !validText(c.Position.ID, 128) || c.Position.Index != nil && !validText(*c.Position.Index, 128) {
		return fmt.Errorf("failed to validate lp checkpoint: position=invalid")
	}
	if len(c.Payload) > MaxLPCheckpointBytes {
		return fmt.Errorf("failed to validate lp checkpoint: %w: max_bytes=%d", ErrLPCheckpointCapacity, MaxLPCheckpointBytes)
	}
	trimmed := bytes.TrimSpace(c.Payload)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return fmt.Errorf("failed to validate lp checkpoint: payload=invalid")
	}
	return nil
}

func validateLPCheckpointSave(p LPCheckpointSaveParams) error {
	if err := p.Source.Validate(); err != nil {
		return fmt.Errorf("failed to validate lp checkpoint save: %w", err)
	}
	if err := validateLPCheckpoint(p.Checkpoint); err != nil {
		return fmt.Errorf("failed to validate lp checkpoint save: %w", err)
	}
	if !sameScope(p.Checkpoint.Pool, p.Source) || p.ExpectedCursorRevision == ^uint64(0) || p.ExpectedParentRevision == 0 {
		return fmt.Errorf("failed to validate lp checkpoint save: scope_or_revision=invalid")
	}
	return nil
}

func (b Batch) validateLPCheckpointResets() error {
	if len(b.ResetLPCheckpoints) > MaxLPCheckpointBatch {
		return fmt.Errorf("failed to validate lp checkpoint resets: size=too_long max_rows=%d", MaxLPCheckpointBatch)
	}
	seen := make(map[Identity]bool, len(b.ResetLPCheckpoints))
	for _, pool := range b.ResetLPCheckpoints {
		if err := pool.Validate(); err != nil {
			return fmt.Errorf("failed to validate lp checkpoint reset: %w", err)
		}
		if seen[pool] || !sameScope(pool, b.Cursor.Source) {
			return fmt.Errorf("failed to validate lp checkpoint reset: scope_or_identity=invalid")
		}
		seen[pool] = true
	}
	return nil
}
