//
// usage_balance.go
//
package app

import (
    "context"
    "database/sql/driver"
    "database/sql"
    "errors"
    "fmt"
    "strings"
    "time"

    "github.com/go-sql-driver/mysql"

    k4k3ruAPI             "github.com/k4k3ru-hub/storage/go/api"
    k4k3ruInternalSQLScan "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
)


const (
    DefaultUsageBalanceTableName = "account_app_usage_balances"
)


type UsageBalance struct {
    AccountID    uint64
    Status       UsageBalanceStatus
    BalanceTicks uint64
    MetaData     *string
    CreatedAt    time.Time
    UpdatedAt    time.Time
}

type UsageBalanceStatus uint8

const (
    UsageBalanceStatusActive UsageBalanceStatus = iota + 1
    UsageBalanceStatusBlocked
)

type UsageBalanceStore struct {
    tableName string
}

type UsageBalanceInsertParams struct {
    AccountID    uint64
    Status       UsageBalanceStatus
    BalanceTicks uint64
    MetaData     *string
    CreatedAt    time.Time
    Ignore       bool
}

type UsageBalanceSelectParams struct {
    AccountID       *uint64
    Status          *UsageBalanceStatus
    BalanceTicksGTE *uint64
    BalanceTicksLTE *uint64
    OrderBy         string
    OrderByDesc     bool
    Limit           int
    Offset          int
}

type UsageBalanceUpdateParams struct {
    Status          *UsageBalanceStatus
    BalanceTicks    *uint64
    MetaData        *string
    SetNullMetaData bool
}


//
// Create new usage balance store.
//
// Version:
//   - 2026-07-26: Added.
//
func NewUsageBalanceStore(tableName, accountTableName string) (*UsageBalanceStore, error) {
    operationErr := errors.New("failed to create account app usage balance store")

    // Guard.
    tableName = strings.TrimSpace(tableName)
    if tableName == "" {
        return nil, fmt.Errorf("%w: invalid parameter: table_name=empty", operationErr)
    }

    return &UsageBalanceStore{
        tableName: tableName,
    }, nil
}

//
// Validate usage balance account ID.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageBalanceAccountID(accountID uint64) error {
    if accountID == 0 {
        return fmt.Errorf("invalid parameter: account_id=0")
    }
    return nil
}


//
// Validate usage balance account ID.
//
// Version:
//   - 2026-07-26: Added.
//
func (b *UsageBalance) ValidateAccountID() error {
    if b == nil {
        return fmt.Errorf("invalid parameter: usage_balance=null")
    }
    return ValidateUsageBalanceAccountID(b.AccountID)
}

//
// Validate usage balance status.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageBalanceStatus(s UsageBalanceStatus) error {
    if err := s.Validate(); err != nil {
        return err
    }
    return nil
}

//
// Validate usage balance status.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageBalance) ValidateStatus() error {
    if e == nil {
        return fmt.Errorf("invalid parameter: usage_balance=null")
    }
    return ValidateUsageBalanceStatus(e.Status)
}

//
// Validate usage balance meta data.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageBalanceMetaData(metaData *string) error {
    if metaData == nil {
        return nil
    }
    if len([]byte(*metaData)) > 4096 {
        return fmt.Errorf("invalid parameter: meta_data=too_long")
    }
    return nil
}

//
// Validate usage balance meta data.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageBalance) ValidateMetaData() error {
    if e == nil {
        return fmt.Errorf("invalid parameter: usage_balance=null")
    }
    return ValidateUsageBalanceMetaData(e.MetaData)
}

//
// Create usage balance table.
//
// Version:
//   - 2026-07-26: Added.
//
func (s *UsageBalanceStore) CreateTable(ctx context.Context, executor k4k3ruAPI.Executor) error {
    operationErr := errors.New("failed to create usage balance table")

    // Guard.
    if s == nil {
        return fmt.Errorf("%w: invalid parameter: usage_balance_store=null", operationErr)
    }
    if s.tableName == "" {
        return fmt.Errorf("%w: invalid parameter: table_name=empty", operationErr)
    }
    if ctx == nil {
        return fmt.Errorf("%w: invalid parameter: context=null", operationErr)
    }
    if executor == nil {
        return fmt.Errorf("%w: invalid parameter: executor=null", operationErr)
    }

    // Generate CREATE TABLE query.
    query := fmt.Sprintf(
        `CREATE TABLE IF NOT EXISTS %s (
            %s BIGINT UNSIGNED NOT NULL COMMENT 'Account ID',
            %s TINYINT UNSIGNED NOT NULL COMMENT 'Status',
            %s BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Balance ticks',
            %s TEXT NULL COMMENT 'Meta data',
            %s DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created at',
            %s DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Updated at',
            PRIMARY KEY (%s),
            KEY idx_status (%s)
        ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`,
        s.tableName,
        ColAccountID,
        ColStatus,
        ColBalanceTicks,
        ColMetaData,
        ColCreatedAt,
        ColUpdatedAt,
        ColAccountID,
        ColStatus,
    )

    // Execute query.
    if _, err := executor.ExecContext(ctx, query); err != nil {
        return fmt.Errorf("%w: %w", operationErr, err)
    }

    return nil
}

//
// Delete usage balance by ID.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageBalanceStore) DeleteByID(ctx context.Context, executor k4k3ruAPI.Executor, id uint64) error {
    operationErr := errors.New("failed to delete usage balance by id")

    // Guard.
    if s == nil {
        return fmt.Errorf("%w: invalid parameter: usage_balance_store=null", operationErr)
    }
    if s.tableName == "" {
        return fmt.Errorf("%w: invalid parameter: table_name=empty", operationErr)
    }
    if ctx == nil {
        return fmt.Errorf("%w: invalid parameter: context=null", operationErr)
    }
    if executor == nil {
        return fmt.Errorf("%w: invalid parameter: executor=null", operationErr)
    }
    if id == 0 {
        return fmt.Errorf("%w: invalid parameter: id=0", operationErr)
    }

    // Generate DELETE query.
    query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", s.tableName, ColID)

    // Execute query.
    if _, err := executor.ExecContext(ctx, query, id); err != nil {
        return fmt.Errorf("%w: %w", operationErr, err)
    }

    return nil
}

//
// Insert usage balance.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageBalanceStore) Insert(ctx context.Context, executor k4k3ruAPI.Executor, params *UsageBalanceInsertParams) error {
    operationErr := errors.New("failed to insert usage balance")

    // Guard.
    if s == nil {
        return fmt.Errorf("%w: invalid parameter: usage_balance_store=null", operationErr)
    }
    if s.tableName == "" {
        return fmt.Errorf("%w: invalid parameter: table_name=empty", operationErr)
    }
    if ctx == nil {
        return fmt.Errorf("%w: invalid parameter: context=null", operationErr)
    }
    if executor == nil {
        return fmt.Errorf("%w: invalid parameter: executor=null", operationErr)
    }

    // Apply default.
    now := time.Now().UTC()
    if params.CreatedAt.IsZero() {
        params.CreatedAt = now
    }

    // Validate params.
    if err := params.Validate(); err != nil {
        return fmt.Errorf("%w: %w", operationErr, err)
    }

    // Generate INSERT query.
    queryPrefix := "INSERT"
    if params.Ignore {
        queryPrefix = "INSERT IGNORE"
    }

    query := fmt.Sprintf(
        "%s INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?);",
        queryPrefix,
        s.tableName,
        ColAccountID,
        ColStatus,
        ColBalanceTicks,
        ColMetaData,
        ColCreatedAt,
    )

    // Execute query.
    if _, err := executor.ExecContext(
        ctx,
        query,
        params.AccountID,
        params.Status,
        params.BalanceTicks,
        params.MetaData,
        params.CreatedAt,
    ); err != nil {
        var mysqlErr *mysql.MySQLError
        if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
            return fmt.Errorf("%w: %w: %w", operationErr, k4k3ruAPI.ErrDuplicateKey, err)
        }
        return fmt.Errorf("%w: %w", operationErr, err)
    }

    return nil
}

//
// Select usage balance by account ID.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageBalanceStore) SelectByName(ctx context.Context, executor k4k3ruAPI.Executor, accountID uint64) (*UsageBalance, error) {
    operationErr := errors.New("failed to select usage balance by account id")

    // Guard.
    if s == nil {
        return nil, fmt.Errorf("%w: invalid parameter: usage_balance_store=null", operationErr)
    }
    if s.tableName == "" {
        return nil, fmt.Errorf("%w: invalid parameter: table_name=empty", operationErr)
    }
    if ctx == nil {
        return nil, fmt.Errorf("%w: invalid parameter: context=null", operationErr)
    }
    if executor == nil {
        return nil, fmt.Errorf("%w: invalid parameter: executor=null", operationErr)
    }

    // Validate account ID.
    if err := ValidateUsageBalanceAccountID(accountID); err != nil {
        return nil, fmt.Errorf("%w: %w", operationErr, err)
    }

    // Generate SELECT query.
    query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1;", s.tableName, ColAccountID)

    // Execute query.
    row := executor.QueryRowContext(ctx, query, accountID)

    // Scan.
    result := &UsageBalance{}
    if err := row.Scan(
        &result.AccountID,
        &result.Status,
        &result.BalanceTicks,
        &result.MetaData,
        &result.CreatedAt,
        &result.UpdatedAt,
    ); err != nil {
        if err == sql.ErrNoRows {
            return nil, nil
        }
        return nil, fmt.Errorf("%w: %w", operationErr, err)
    }

    return result, nil
}

//
// Update usage balance by ID.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageBalanceStore) UpdateByID(ctx context.Context, executor k4k3ruAPI.Executor, params UsageBalanceUpdateParams, id uint64) error {
    operationErr := errors.New("failed to update usage balance by id")

    // Guard.
    if s == nil {
        return fmt.Errorf("%w: invalid parameter: usage_balance_store=null", operationErr)
    }
    if s.tableName == "" {
        return fmt.Errorf("%w: invalid parameter: table_name=empty", operationErr)
    }
    if ctx == nil {
        return fmt.Errorf("%w: invalid parameter: context=null", operationErr)
    }
    if executor == nil {
        return fmt.Errorf("%w: invalid parameter: executor=null", operationErr)
    }
    if id == 0 {
        return fmt.Errorf("%w: invalid parameter: id=0", operationErr)
    }

    // Validate params.
    if err := params.Validate(); err != nil {
        return fmt.Errorf("%w: %w", operationErr, err)
    }

    // Build assignments.
    assignments, args := params.BuildAssignments()
    if len(assignments) == 0 {
        return fmt.Errorf("%w: invalid parameter: assignments=empty", operationErr)
    }

    args = append(args, id)

    // Generate UPDATE query.
    query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?;", s.tableName, strings.Join(assignments, ", "), ColID)

    // Execute query.
    if _, err := executor.ExecContext(ctx, query, args...); err != nil {
        return fmt.Errorf("%w: %w", operationErr, err)
    }

    return nil
}

//
// Check whether usage balance status is valid.
//
// Version:
//   - 2026-07-26: Added.
//
func (s UsageBalanceStatus) IsValid() bool {
    switch s {
    case UsageBalanceStatusActive,
         UsageBalanceStatusBlocked:
        return true
    default:
        return false
    }
}

//
// Validate usage balance status.
//
// Version:
//   - 2026-07-26: Added.
//
func (s UsageBalanceStatus) Validate() error {
    if !s.IsValid() {
        return fmt.Errorf("invalid parameter: usage_balance_status=%d", s)
    }
    return nil
}

//
// Get usage balance status as driver.Valuer.
//
// Version:
//   - 2026-07-26: Added.
//
func (s UsageBalanceStatus) Value() (driver.Value, error) {
    if err := s.Validate(); err != nil {
        return nil, err
    }

    return int64(s), nil
}

//
// Scan usage balance status.
//
func (s *UsageBalanceStatus) Scan(value any) error {
    if s == nil {
        return fmt.Errorf("failed to scan usage balance status: invalid parameter: usage_balance_status=null")
    }

    v, err := k4k3ruInternalSQLScan.Uint8(value)
    if err != nil {
        return fmt.Errorf("failed to scan usage balance status: %w", err)
    }

    result := UsageBalanceStatus(v)
    if err := result.Validate(); err != nil {
        return fmt.Errorf("failed to scan usage balance status: %w", err)
    }

    *s = result

    return nil
}

//
// Validate usage balance insert params.
//
// Version:
//   - 2026-07-27: Added.
//
func (p *UsageBalanceInsertParams) Validate() error {
    if p == nil {
        return fmt.Errorf("invalid parameter: usage_balance_insert_params=null")
    }
    if err := ValidateUsageBalanceAccountID(p.AccountID); err != nil {
        return err
    }
    if err := ValidateUsageBalanceStatus(p.Status); err != nil {
        return err
    }
    if err := ValidateUsageBalanceMetaData(p.MetaData); err != nil {
        return err
    }
    return nil
}

//
// Build SELECT query.
//
// Version:
//   - 2025-07-27: Added.
//
func (p UsageBalanceSelectParams) BuildQuery(selectFromClause string) (string, []any) {
    var query strings.Builder
    query.WriteString(selectFromClause)

    conditions := make([]string, 0, 4)
    args := make([]any, 0, 6)

    if p.AccountID != nil {
        conditions = append(conditions, ColAccountID+"=?")
        args = append(args, *p.AccountID)
    }
    if p.Status != nil {
        conditions = append(conditions, ColStatus+"=?")
        args = append(args, *p.Status)
    }
    if p.BalanceTicksGTE != nil {
        conditions = append(conditions, ColBalanceTicks+">=?")
        args = append(args, *p.BalanceTicksGTE)
    }
    if p.BalanceTicksLTE != nil {
        conditions = append(conditions, ColBalanceTicks+"<=?")
        args = append(args, *p.BalanceTicksLTE)
    }

    if len(conditions) == 0 {
        return selectFromClause, nil
    }

    query.WriteString(" WHERE ")
    query.WriteString(strings.Join(conditions, " AND "))

    if p.OrderBy != "" {
        query.WriteString(" ORDER BY ")
        query.WriteString(p.OrderBy)
        if p.OrderByDesc {
            query.WriteString(" DESC")
        }
    }

    if p.Limit > 0 {
        query.WriteString(" LIMIT ? OFFSET ?")
        args = append(args, p.Limit, p.Offset)
    }

    return query.String(), args
}

//
// Build UPDATE assignments and args.
//
// Version:
//   - 2025-07-27: Added.
//
func (p UsageBalanceUpdateParams) BuildAssignments() ([]string, []any) {
    assignments := make([]string, 0, 3)
    args := make([]any, 0, 6)

    if p.Status != nil {
        assignments = append(assignments, ColStatus+"=?")
        args = append(args, *p.Status)
    }
    if p.BalanceTicks != nil {
        assignments = append(assignments, ColBalanceTicks+"=?")
        args = append(args, *p.BalanceTicks)
    }
    if p.SetNullMetaData {
        assignments = append(assignments, ColMetaData+"=NULL")
    } else if p.MetaData != nil {
        assignments = append(assignments, ColMetaData+"=?")
        args = append(args, *p.MetaData)
    }

    return assignments, args
}

//
// Validate usage balance update params.
//
// Version:
//   - 2026-07-27: Added.
//
func (p *UsageBalanceUpdateParams) Validate() error {
    if p == nil {
        return fmt.Errorf("invalid parameter: usage_balance_update_params=null")
    }
    if p.Status != nil {
        if err := ValidateUsageBalanceStatus(*p.Status); err != nil {
            return err
        }
    }
    if p.MetaData != nil {
        if err := ValidateUsageBalanceMetaData(p.MetaData); err != nil {
            return err
        }
    }
    return nil
}
