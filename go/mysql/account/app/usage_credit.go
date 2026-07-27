//
// usage_credit.go
//
package app

import (
    "context"
    "database/sql/driver"
    "errors"
    "fmt"
    "strings"
    "time"
    "unicode/utf8"

    "github.com/go-sql-driver/mysql"

    k4k3ruAPI               "github.com/k4k3ru-hub/storage/go/api"
    k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
    k4k3ruInternalSQLScan   "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
)


const (
    DefaultUsageCreditTableName = "account_app_usage_credits"
)

var (
    usageCreditIDGenerator = &k4k3ruInternalGenerator.ID{}
)

type UsageCredit struct {
    ID           uint64
    AccountID    uint64
    Type         UsageCreditType
    BalanceTicks uint64
    ExpiresAt    *time.Time
    Description  *string
    MetaData     *string
    CreatedAt    time.Time
    UpdatedAt    time.Time
}

type UsageCreditType uint8

const (
    UsageCreditTypePurchased UsageCreditType = iota + 1
    UsageCreditTypeCampaign
    UsageCreditTypeCompensation
    UsageCreditTypeAdjustment
)

type UsageCreditStore struct {
    tableName string
}

type UsageCreditInsertParams struct {
    ID           uint64
    AccountID    uint64
    Type         UsageCreditType
    BalanceTicks uint64
    ExpiresAt    *time.Time
    Description  *string
    MetaData     *string
    CreatedAt    time.Time
    Ignore       bool
}


//
// Generate usage credit ID.
//
// Version:
//   - 2026-07-26: Added.
//
func GenerateUsageCreditID() uint64 {
    return usageCreditIDGenerator.Generate()
}

//
// Create new usage credit store.
//
// Version:
//   - 2026-07-26: Added.
//
func NewUsageCreditStore(tableName string) (*UsageCreditStore, error) {
    operationErr := errors.New("failed to create account app usage credit store")

    // Guard.
    tableName = strings.TrimSpace(tableName)
    if tableName == "" {
        return nil, fmt.Errorf("%w: invalid parameter: table_name=empty", operationErr)
    }

    return &UsageCreditStore{
        tableName: tableName,
    }, nil
}

//
// Validate usage credit ID.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditID(id uint64) error {
    if id == 0 {
        return fmt.Errorf("invalid parameter: id=0")
    }
    return nil
}

//
// Validate usage credit ID.
//
// Version:
//   - 2026-07-26: Added.
//
func (c *UsageCredit) ValidateID() error {
    if c == nil {
        return fmt.Errorf("invalid parameter: usage_credit=null")
    }
    return ValidateUsageCreditID(c.ID)
}

//
// Validate usage credit account ID.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditAccountID(accountID uint64) error {
    if accountID == 0 {
        return fmt.Errorf("invalid parameter: account_id=0")
    }
    return nil
}

//
// Validate usage credit account ID.
//
// Version:
//   - 2026-07-26: Added.
//
func (c *UsageCredit) ValidateAccountID() error {
    if c == nil {
        return fmt.Errorf("invalid parameter: usage_credit=null")
    }
    return ValidateUsageCreditAccountID(c.AccountID)
}

//
// Validate usage credit type.
// 
// Version:
//   - 2026-07-26: Added.
// 
func ValidateUsageCreditType(t UsageCreditType) error {
    if err := t.Validate(); err != nil {
        return err
    }
    return nil
}  

//      
// Validate usage credit type.
// 
// Version:
//   - 2026-07-26: Added.
// 
func (e *UsageCredit) ValidateType() error {
    if e == nil {
        return fmt.Errorf("invalid parameter: usage_credit=null")
    }
    return ValidateUsageCreditType(e.Type)
}

//
// Validate usage credit expires at.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditExpiresAt(expiresAt *time.Time) error {
    if expiresAt == nil {
        return nil
    }
    if (*expiresAt).IsZero() {
        return fmt.Errorf("invalid parameter: expires_at=empty")
    }
    return nil
}

//
// Validate usage credit expires at.
//
// Version:
//   - 2026-07-26: Added.
//
func (c *UsageCredit) ValidateCreditExpiresAt() error {
    if c == nil {
        return fmt.Errorf("invalid parameter: usage_credit=null")
    }
    return ValidateUsageCreditExpiresAt(c.ExpiresAt)
}

//
// Validate usage credit description.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditDescription(description *string) error {
    if description == nil {
        return nil
    }
    if utf8.RuneCountInString(*description) > 255 {
        return fmt.Errorf("invalid parameter: description=too_long")
    }
    return nil
}

//
// Validate usage credit description.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCredit) ValidateDescription() error {
    if e == nil {
        return fmt.Errorf("invalid parameter: usage_credit=null")
    }
    return ValidateUsageCreditDescription(e.Description)
}

//
// Validate usage credit meta data.
//
// Version:
//   - 2026-07-26: Added.
//
func ValidateUsageCreditMetaData(metaData *string) error {
    if metaData == nil {
        return nil
    }
    if len([]byte(*metaData)) > 4096 {
        return fmt.Errorf("invalid parameter: meta_data=too_long")
    }
    return nil
}

//
// Validate usage credit meta data.
//
// Version:
//   - 2026-07-26: Added.
//
func (e *UsageCredit) ValidateMetaData() error {
    if e == nil {
        return fmt.Errorf("invalid parameter: usage_credit=null")
    }
    return ValidateUsageCreditMetaData(e.MetaData)
}

//
// Create usage credit table.
//
// Version:
//   - 2026-07-26: Added.
//
func (s *UsageCreditStore) CreateTable(ctx context.Context, executor k4k3ruAPI.Executor) error {
    operationErr := errors.New("failed to create usage credit table")

    // Guard.
    if s == nil {
        return fmt.Errorf("%w: invalid parameter: usage_credit_store=null", operationErr)
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
            %s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
            %s BIGINT UNSIGNED NOT NULL COMMENT 'Account ID',
            %s TINYINT UNSIGNED NOT NULL COMMENT 'Type',
            %s BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Remaining balance ticks',
            %s DATETIME(6) NULL COMMENT 'Expires at',
            %s VARCHAR(255) NULL COMMENT 'Description',
            %s TEXT NULL COMMENT 'Meta data',
            %s DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created at',
            %s DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Updated at',
            PRIMARY KEY (%s),
            KEY idx_%s_account_expires (%s, %s),
            KEY idx_%s_expires_account (%s, %s)
        ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`,
        s.tableName,
        ColID,
        ColAccountID,
        ColType,
        ColBalanceTicks,
        ColExpiresAt,
        ColDescription,
        ColMetaData,
        ColCreatedAt,
        ColUpdatedAt,
        ColID,
        s.tableName, ColAccountID, ColExpiresAt,
        s.tableName, ColExpiresAt, ColAccountID,
    )

    // Execute query.
    if _, err := executor.ExecContext(ctx, query); err != nil {
        return fmt.Errorf("%w: %w", operationErr, err)
    }

    return nil
}

//
// Delete usage credit by ID.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageCreditStore) DeleteByID(ctx context.Context, executor k4k3ruAPI.Executor, id uint64) error {
    operationErr := errors.New("failed to delete usage credit by id")

    // Guard.
    if s == nil {
        return fmt.Errorf("%w: invalid parameter: usage_credit_store=null", operationErr)
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
// Insert usage credit.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageCreditStore) Insert(ctx context.Context, executor k4k3ruAPI.Executor, params *UsageCreditInsertParams) error {
    operationErr := errors.New("failed to insert usage credit")

    // Guard.
    if s == nil {
        return fmt.Errorf("%w: invalid parameter: usage_credit_store=null", operationErr)
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

    // Apply defaults.
    if params.ID == 0 {
        params.ID = GenerateUsageCreditID()
    }

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
        "%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
        queryPrefix,
        s.tableName,
        ColID,
        ColAccountID,
        ColType,
        ColBalanceTicks,
        ColExpiresAt,
        ColDescription,
        ColMetaData,
        ColCreatedAt,
    )

    // Execute query.
    if _, err := executor.ExecContext(
        ctx,
        query,
        params.ID,
        params.AccountID,
        params.Type,
        params.BalanceTicks,
        params.ExpiresAt,
        params.Description,
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
// Select usage credit by account ID and expires at.
//
// Version:
//   - 2026-07-27: Added.
//
func (s *UsageCreditStore) SelectByAccountIDAndExpiresAt(ctx context.Context, executor k4k3ruAPI.Executor, accountID uint64, expiresAt time.Time) ([]*UsageCredit, error) {
    operationErr := errors.New("failed to select usage credit by account id and expires at")

    // Guard.
    if s == nil {
        return nil, fmt.Errorf("%w: invalid parameter: usage_credit_store=null", operationErr)
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

    // Validate.
    if err := ValidateUsageCreditAccountID(accountID); err != nil {
        return nil, fmt.Errorf("%w: %w", operationErr, err)
    }
    if err := ValidateUsageCreditExpiresAt(&expiresAt); err != nil {
        return nil, fmt.Errorf("%w: %w", operationErr, err)
    }

    // Generate SELECT query.
    query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? AND %s = ?;", s.tableName, ColAccountID, ColExpiresAt)

    // Execute query.
    rows, err := executor.QueryContext(ctx, query, accountID, expiresAt)
    if err != nil {
        return nil, fmt.Errorf("%w: %w", operationErr, err)
    }

    defer rows.Close()

    // Scan.
    var result []*UsageCredit
    for rows.Next() {
        row := &UsageCredit{}
        if err := rows.Scan(
            &row.ID,
            &row.AccountID,
            &row.Type,
            &row.BalanceTicks,
            &row.ExpiresAt,
            &row.Description,
            &row.MetaData,
            &row.CreatedAt,
            &row.UpdatedAt,
        ); err != nil {
            return nil, fmt.Errorf("%w: %w", operationErr, err)
        }

        result = append(result, row)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("%w: %w", operationErr, err)
    }

    return result, nil
}

//
// Check whether usage credit type is valid.
//
// Version:
//   - 2026-07-26: Added.
//
func (t UsageCreditType) IsValid() bool {
    switch t {
    case UsageCreditTypePurchased,
         UsageCreditTypeCampaign,
         UsageCreditTypeCompensation,
         UsageCreditTypeAdjustment:
        return true
    default:
        return false
    }
}

//
// Validate usage credit type.
//
// Version:
//   - 2026-07-26: Added.
//
func (t UsageCreditType) Validate() error {
    if !t.IsValid() {
        return fmt.Errorf("invalid parameter: usage_credit_type=%d", t)
    }
    return nil
}

//
// Get usage credit type as driver.Valuer.
//
// Version:
//   - 2026-07-26: Added.
//
func (t UsageCreditType) Value() (driver.Value, error) {
    if err := t.Validate(); err != nil {
        return nil, err
    }

    return int64(t), nil
}

//
// Scan usage credit type.
//
// Version:
//   - 2026-07-26: Added.
//
func (t *UsageCreditType) Scan(value any) error {
    if t == nil {
        return fmt.Errorf("failed to scan usage credit type: invalid parameter: usage_credit_type=null")
    }

    v, err := k4k3ruInternalSQLScan.Uint8(value)
    if err != nil {
        return fmt.Errorf("failed to scan usage credit type: %w", err)
    }

    result := UsageCreditType(v)
    if err := result.Validate(); err != nil {
        return fmt.Errorf("failed to scan usage credit type: %w", err)
    }

    *t = result

    return nil
}

//
// Validate usage credit insert params.
//
// Version:
//   - 2026-07-27: Added.
//
func (p *UsageCreditInsertParams) Validate() error {
    if p == nil {
        return fmt.Errorf("invalid parameter: usage_credit_insert_params=null")
    }
    if err := ValidateUsageCreditID(p.ID); err != nil {
        return err
    }
    if err := ValidateUsageCreditAccountID(p.AccountID); err != nil {
        return err
    }
    if err := ValidateUsageCreditType(p.Type); err != nil {
        return err
    }
    if err := ValidateUsageCreditExpiresAt(p.ExpiresAt); err != nil {
        return err
    }
    if err := ValidateUsageCreditDescription(p.Description); err != nil {
        return err
    }
    if err := ValidateUsageCreditMetaData(p.MetaData); err != nil {
        return err
    }
    return nil
}
