package execution

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	k4k3ruStorageAPI "github.com/k4k3ru-hub/storage/go/api"
	k4k3ruMySQLValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultExecutionTableName          = "trade_hub_executions"
	DefaultExecutionLegTableName       = "trade_hub_execution_legs"
	DefaultOnchainTransactionTableName = "trade_hub_execution_onchain_transactions"
)

type Store struct {
	executionTable          string
	legTable                string
	onchainTransactionTable string
}

// NewStore creates a Trade Hub execution store.
//
// Parameters:
//   - executionTable: Parent execution table name.
//   - legTable: Execution leg table name.
//   - onchainTransactionTable: Onchain transaction table name.
//
// Returns:
//   - Execution store.
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func NewStore(executionTable string, legTable string, onchainTransactionTable string) (*Store, error) {
	for field, value := range map[string]string{"execution_table": executionTable, "leg_table": legTable, "onchain_transaction_table": onchainTransactionTable} {
		if err := k4k3ruMySQLValidator.ValidateSQLIdentifier(strings.TrimSpace(value), field); err != nil {
			return nil, fmt.Errorf("failed to create trade hub execution store: %w", err)
		}
	}
	return &Store{executionTable: strings.TrimSpace(executionTable), legTable: strings.TrimSpace(legTable), onchainTransactionTable: strings.TrimSpace(onchainTransactionTable)}, nil
}

// NewDefaultStore creates a store using the standard Trade Hub table names.
//
// Returns:
//   - Execution store.
//   - Validation error.
//
// Version:
//   - 2026-09-10: Added.
func NewDefaultStore() (*Store, error) {
	return NewStore(DefaultExecutionTableName, DefaultExecutionLegTableName, DefaultOnchainTransactionTableName)
}

// InsertExecution inserts an immutable execution snapshot.
//
// Parameters:
//   - ctx: Operation context.
//   - executor: SQL executor.
//   - params: Snapshot values.
//
// Version:
//   - 2026-09-10: Added.
func (s *Store) InsertExecution(ctx context.Context, executor k4k3ruStorageAPI.Executor, params ExecutionInsertParams) error {
	const operation = "failed to insert trade hub execution"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	query := fmt.Sprintf("INSERT INTO %s (id, status, kind, request_snapshot, conditions_snapshot, opportunity_snapshot, prepared_at, expires_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", s.executionTable)
	_, err := executor.ExecContext(ctx, query, params.ID, params.Status, params.Kind, []byte(params.RequestSnapshot), nullableJSON(params.ConditionsSnapshot), nullableJSON(params.OpportunitySnapshot), params.PreparedAt.UTC(), params.ExpiresAt.UTC(), params.CreatedAt.UTC())
	return normalizeWriteError(operation, err)
}

// InsertLeg inserts one categorized execution leg.
//
// Parameters:
//   - ctx: Operation context.
//   - executor: SQL executor.
//   - params: Leg values.
//
// Version:
//   - 2026-09-10: Added.
func (s *Store) InsertLeg(ctx context.Context, executor k4k3ruStorageAPI.Executor, params *LegInsertParams) error {
	const operation = "failed to insert trade hub execution leg"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: params=null", operation)
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	}
	if params.ID == 0 {
		params.ID = GenerateExecutionLegID()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	query := fmt.Sprintf("INSERT INTO %s (id, execution_id, leg_index, category, status, venue, created_at) VALUES (?, ?, ?, ?, ?, ?, ?);", s.legTable)
	_, err := executor.ExecContext(ctx, query, params.ID, params.ExecutionID, params.LegIndex, params.Category, params.Status, params.Venue, params.CreatedAt.UTC())
	return normalizeWriteError(operation, err)
}

// InsertOnchainTransaction inserts the prepared onchain transaction metadata for a leg.
//
// Parameters:
//   - ctx: Operation context.
//   - executor: SQL executor.
//   - params: Onchain transaction metadata.
//
// Version:
//   - 2026-09-10: Added.
func (s *Store) InsertOnchainTransaction(ctx context.Context, executor k4k3ruStorageAPI.Executor, params OnchainTransactionInsertParams) error {
	const operation = "failed to insert trade hub execution onchain transaction"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	query := fmt.Sprintf("INSERT INTO %s (execution_leg_id, chain_family, chain, network, signer, payload_digest, created_at) VALUES (?, ?, ?, ?, ?, ?, ?);", s.onchainTransactionTable)
	_, err := executor.ExecContext(ctx, query, params.ExecutionLegID, params.ChainFamily, params.Chain, params.Network, params.Signer, params.PayloadDigest, params.CreatedAt.UTC())
	return normalizeWriteError(operation, err)
}

// SelectOnchainSubmissionForUpdate locks and returns one prepared onchain execution leg.
//
// Parameters:
//   - ctx: Operation context.
//   - tx: SQL transaction.
//   - executionID: Execution identifier.
//
// Returns:
//   - Parent execution, leg, and onchain transaction metadata.
//   - Selection error.
//
// Version:
//   - 2026-09-10: Added.
func (s *Store) SelectOnchainSubmissionForUpdate(ctx context.Context, tx *sql.Tx, executionID string) (*Execution, *Leg, *OnchainTransaction, error) {
	const operation = "failed to select trade hub onchain submission for update"
	if s == nil {
		return nil, nil, nil, fmt.Errorf("%s: store=null", operation)
	}
	if ctx == nil {
		return nil, nil, nil, fmt.Errorf("%s: context=null", operation)
	}
	if tx == nil {
		return nil, nil, nil, fmt.Errorf("%s: sql_tx=null", operation)
	}
	if strings.TrimSpace(executionID) == "" || len(executionID) > 64 {
		return nil, nil, nil, fmt.Errorf("%s: execution_id=invalid", operation)
	}
	query := fmt.Sprintf(`SELECT e.id, e.status, e.kind, e.request_snapshot, e.conditions_snapshot, e.opportunity_snapshot, e.result_snapshot, e.prepared_at, e.expires_at, e.completed_at, e.created_at, e.updated_at,
		l.id, l.execution_id, l.leg_index, l.category, l.status, l.venue, l.created_at, l.updated_at,
		t.execution_leg_id, t.chain_family, t.chain, t.network, t.signer, t.payload_digest, t.transaction_id, t.block_number, t.gas_used, t.fee_amount, t.fee_asset, t.submission_started_at, t.submitted_at, t.confirmed_at, t.created_at, t.updated_at
		FROM %s e JOIN %s l ON l.execution_id=e.id JOIN %s t ON t.execution_leg_id=l.id
		WHERE e.id=? AND l.category=? ORDER BY l.leg_index ASC LIMIT 1 FOR UPDATE;`, s.executionTable, s.legTable, s.onchainTransactionTable)
	execution := new(Execution)
	leg := new(Leg)
	onchain := new(OnchainTransaction)
	var conditions, opportunity, result []byte
	err := tx.QueryRowContext(ctx, query, executionID, LegCategoryOnchainTransaction).Scan(
		&execution.ID, &execution.Status, &execution.Kind, &execution.RequestSnapshot, &conditions, &opportunity, &result, &execution.PreparedAt, &execution.ExpiresAt, &execution.CompletedAt, &execution.CreatedAt, &execution.UpdatedAt,
		&leg.ID, &leg.ExecutionID, &leg.LegIndex, &leg.Category, &leg.Status, &leg.Venue, &leg.CreatedAt, &leg.UpdatedAt,
		&onchain.ExecutionLegID, &onchain.ChainFamily, &onchain.Chain, &onchain.Network, &onchain.Signer, &onchain.PayloadDigest, &onchain.TransactionID, &onchain.BlockNumber, &onchain.GasUsed, &onchain.FeeAmount, &onchain.FeeAsset, &onchain.SubmissionStartedAt, &onchain.SubmittedAt, &onchain.ConfirmedAt, &onchain.CreatedAt, &onchain.UpdatedAt,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%s: %w", operation, err)
	}
	execution.ConditionsSnapshot, execution.OpportunitySnapshot, execution.ResultSnapshot = conditions, opportunity, result
	return execution, leg, onchain, nil
}

// MarkOnchainSubmitting atomically marks a prepared leg and its parent as submitting.
//
// Parameters:
//   - ctx: Operation context.
//   - tx: SQL transaction.
//   - executionID: Parent execution identifier.
//   - executionLegID: Leg identifier.
//   - startedAt: Submission start time.
//
// Version:
//   - 2026-09-10: Added.
func (s *Store) MarkOnchainSubmitting(ctx context.Context, tx *sql.Tx, executionID string, executionLegID uint64, startedAt time.Time) error {
	const operation = "failed to mark trade hub onchain execution submitting"
	if s == nil || ctx == nil || tx == nil {
		return fmt.Errorf("%s: dependency=null", operation)
	}
	if executionID == "" || executionLegID == 0 || startedAt.IsZero() {
		return fmt.Errorf("%s: parameter=invalid", operation)
	}
	result, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET status=? WHERE id=? AND status=? AND expires_at>?;", s.executionTable), StatusSubmitting, executionID, StatusPrepared, startedAt.UTC())
	if err := requireOneRow(result, err, operation); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s l JOIN %s t ON t.execution_leg_id=l.id SET l.status=?, t.submission_started_at=? WHERE l.id=? AND l.execution_id=? AND l.status IN (?, ?);", s.legTable, s.onchainTransactionTable), LegStatusSubmitting, startedAt.UTC(), executionLegID, executionID, LegStatusPrepared, LegStatusAwaitingSignature)
	return requireOneRow(result, err, operation)
}

// MarkOnchainSubmitted records a transaction identifier and marks the execution submitted.
//
// Parameters:
//   - ctx: Operation context.
//   - tx: SQL transaction.
//   - executionID: Parent execution identifier.
//   - executionLegID: Leg identifier.
//   - transactionID: Chain transaction identifier.
//   - submittedAt: Submission completion time.
//
// Version:
//   - 2026-09-10: Added.
func (s *Store) MarkOnchainSubmitted(ctx context.Context, tx *sql.Tx, executionID string, executionLegID uint64, transactionID string, submittedAt time.Time) error {
	const operation = "failed to mark trade hub onchain execution submitted"
	if s == nil || ctx == nil || tx == nil {
		return fmt.Errorf("%s: dependency=null", operation)
	}
	if executionID == "" || executionLegID == 0 || strings.TrimSpace(transactionID) == "" || len(transactionID) > 255 || submittedAt.IsZero() {
		return fmt.Errorf("%s: parameter=invalid", operation)
	}
	result, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET transaction_id=?, submitted_at=? WHERE execution_leg_id=? AND transaction_id IS NULL;", s.onchainTransactionTable), transactionID, submittedAt.UTC(), executionLegID)
	if err := requireOneRow(result, err, operation); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET status=? WHERE id=? AND execution_id=? AND status=?;", s.legTable), LegStatusSubmitted, executionLegID, executionID, LegStatusSubmitting)
	if err := requireOneRow(result, err, operation); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET status=? WHERE id=? AND status=?;", s.executionTable), StatusSubmitted, executionID, StatusSubmitting)
	return requireOneRow(result, err, operation)
}

func (s *Store) validateOperation(ctx context.Context, executor k4k3ruStorageAPI.Executor, operation string) error {
	if s == nil || s.executionTable == "" || s.legTable == "" || s.onchainTransactionTable == "" {
		return fmt.Errorf("%s: store=null", operation)
	}
	if ctx == nil {
		return fmt.Errorf("%s: context=null", operation)
	}
	if executor == nil {
		return fmt.Errorf("%s: executor=null", operation)
	}
	return nil
}

func nullableJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func normalizeWriteError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return fmt.Errorf("%s: %w: %w", operation, k4k3ruStorageAPI.ErrDuplicateKey, err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func requireOneRow(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	if result == nil {
		return fmt.Errorf("%s: result=null", operation)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: failed to read affected rows: %w", operation, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: state_transition=conflict", operation)
	}
	return nil
}
