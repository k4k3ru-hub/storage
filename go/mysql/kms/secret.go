//
// secret.go
//
package kms

import (
    "database/sql"
    "errors"
    "fmt"
    "strings"

    "github.com/go-sql-driver/mysql"

    k4k3ruAPI               "github.com/k4k3ru-hub/storage/go/api"
    k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
)


const (
    DefaultSecretTableName = "kms_secrets"
)


var (
    secretIDGenerator = &k4k3ruInternalGenerator.ID{}
)


type Secret struct {
    ID         uint64
    Name       string
    Ciphertext string
    ProviderID string
    AAD        string
}

type SecretStore struct {
    tableName string
}

type SecretInsertParams struct {
    ID         uint64
    Name       string
    Ciphertext string
    ProviderID string
    AAD        string
    Ignore     bool
}

type SecretSelectParams struct {
    ID           *uint64
    Name         *string
    NameLike     *string
    ProviderID   *string
    OrderBy      string
    OrderByDesc  bool
    Limit        int
    Offset       int
}

//
// Create new secret store.
//
// Version:
//   - 2026-07-18: Added.
//
func NewSecretStore(tableName string) (*SecretStore, error) {
    // Guard.
    tableName = strings.TrimSpace(tableName)
    if tableName == "" {
        return nil, fmt.Errorf("failed to create new secret store: missing required parameter: table_name=empty")
    }

    return &SecretStore{
        tableName: tableName,
    }, nil
}

//
// Generate an ID.
//
// Version:
//   - 2026-07-18: Added.
//
func GenerateSecretID() uint64 {
    return secretIDGenerator.Generate()
}

//
// Validate secret ID.
//
// Version:
//   - 2026-05-08: Added.
//
func ValidateSecretID(id uint64) error {
    if id == 0 {
        return fmt.Errorf("missing required parameter: id=0")
    }
    return nil
}

//
// Validate secret ID.
//
// Version:
//   - 2026-05-08: Added.
//
func (s *Secret) ValidateID() error {
    if s == nil {
        return fmt.Errorf("missing required parameter: secret=null")
    }
    return ValidateSecretID(s.ID)
}

//
// Validate secret name.
//
func ValidateSecretName(name string) error {
    if name == "" {
        return fmt.Errorf("missing required parameter: name=empty")
    }
    if len(name) > 64 {
        return fmt.Errorf("invalid parameter: name=too_long max_length=64")
    }
    return nil
}

//
// Validate secret name.
//
func (s *Secret) ValidateName() error {
    if s == nil {
        return fmt.Errorf("missing required parameter: secret=null")
    }
    return ValidateSecretName(s.Name)
}

//
// Validate secret ciphertext.
//
func ValidateSecretCiphertext(ciphertext string) error {
    if ciphertext == "" {
        return fmt.Errorf("missing required parameter: ciphertext=empty")
    }
    if len(ciphertext) > 1024 {
        return fmt.Errorf("invalid parameter: ciphertext=too_long max_length=1024")
    }
    return nil
}

//
// Validate secret ciphertext.
//
func (s *Secret) ValidateCiphertext() error {
    if s == nil {
        return fmt.Errorf("missing required parameter: secret=null")
    }
    return ValidateSecretCiphertext(s.Ciphertext)
}

//
// Validate secret provider ID.
//
func ValidateSecretProviderID(providerID string) error {
    if providerID == "" {
        return fmt.Errorf("missing required parameter: provider_id=empty")
    }
    if len(providerID) > 128 {
        return fmt.Errorf("invalid parameter: provider_id=too_long max_length=128")
    }
    return nil
}

//
// Validate secret provider ID.
//
func (s *Secret) ValidateProviderID() error {
    if s == nil {
        return fmt.Errorf("missing required parameter: secret=null")
    }
    return ValidateSecretProviderID(s.ProviderID)
}

//
// Validate secret AAD.
//
func ValidateSecretAAD(aad string) error {
    if aad == "" {
        return fmt.Errorf("missing required parameter: aad=empty")
    }
    if len(aad) > 128 {
        return fmt.Errorf("invalid parameter: aad=too_long max_length=128")
    }
    return nil
}

//
// Validate secret AAD.
//
func (s *Secret) ValidateAAD() error {
    if s == nil {
        return fmt.Errorf("missing required parameter: secret=null")
    }
    return ValidateSecretAAD(s.AAD)
}

//
// Create secrets table.
//
// Version:
//   - 2026-07-18: Added.
//
func (s *SecretStore) CreateTable(executor k4k3ruAPI.Executor) error {
    // Guard.
    if s == nil {
        return fmt.Errorf("failed to create secrets table: missing required parameter: secret_store=null")
    }
    if s.tableName == "" {
        return fmt.Errorf("failed to create secrets table: missing required parameter: table_name=empty")
    }
    if executor == nil {
        return fmt.Errorf("failed to create secrets table: missing required parameter: executor=null")
    }

    // Generate CREATE TABLE query.
    query := fmt.Sprintf(
        `CREATE TABLE IF NOT EXISTS %s (
            %s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
            %s VARCHAR(64) NOT NULL COMMENT 'Name',
            %s TEXT NOT NULL COMMENT 'Ciphertext',
            %s VARCHAR(128) NULL COMMENT 'Provider ID',
            %s VARCHAR(128) NULL COMMENT 'AAD',
            PRIMARY KEY (%s),
            UNIQUE KEY uk_name (%s),
            KEY idx_provider_id (%s);
        `,
        s.tableName,
        ColID,
        ColName,
        ColCiphertext,
        ColProviderID,
        ColAAD,
        ColID,
        ColName,
        ColProviderID,
    )

    // Execute query.
    if _, err := executor.Exec(query); err != nil {
        return fmt.Errorf("failed to create secrets table: %w", err)
    }

    return nil
}

//
// Count secrets.
//
// Version:
//   - 2026-07-18: Added.
//
func (s *SecretStore) Count(executor k4k3ruAPI.Executor, params *SecretSelectParams) (int64, error) {
    // Guard.
    if s == nil {
        return 0, fmt.Errorf("failed to count secrets: missing required parameter: secret_store=null")
    }
    if s.tableName == "" {
        return 0, fmt.Errorf("failed to count secrets: missing required parameter: table_name=%q", "empty")
    }
    if executor == nil {
        return 0, fmt.Errorf("failed to count secrets: missing required parameter: executor=null")
    }
    if err := params.Validate(); err != nil {
        return 0, fmt.Errorf("failed to count secrets: %w", err)
    }

    query, args := params.BuildQuery("SELECT COUNT(*) FROM " + s.tableName)

    // Execute query.
    var result int64
    err := executor.QueryRow(query, args...).Scan(&result)
    if err != nil {
        return 0, fmt.Errorf("failed to count secrets: %w", err)
    }

    return result, nil
}

//
// Delete secret by ID.
//
// Version:
//   - 2026-07-18: Added.
//
func (s *SecretStore) DeleteByID(executor k4k3ruAPI.Executor, id uint64) error {
    // Guard.
    if s == nil {
        return fmt.Errorf("failed to delete secret by id: missing required parameter: secret_store=null")
    }
    if s.tableName == "" {
        return fmt.Errorf("failed to delete secret by id: missing required parameter: table_name=empty")
    }
    if executor == nil {
        return fmt.Errorf("failed to delete secret by id: missing required parameter: executor=null")
    }
    if id == 0 {
        return fmt.Errorf("failed to delete secret by id:  missing required parameter: id=0")
    }

    // Generate DELETE query.
    query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", s.tableName, ColID)

    // Execute query.
    if _, err := executor.Exec(query, id); err != nil {
        return fmt.Errorf("failed to delete secret by id: %w", err)
    }

    return nil
}

//
// Insert secret.
//
// Version:
//   - 2026-07-18: Added.
//
func (s *SecretStore) Insert(executor k4k3ruAPI.Executor, params *SecretInsertParams) error {
    // Guard.
    if s == nil {
        return fmt.Errorf("failed to insert secret: missing required parameter: secret_store=null")
    }
    if s.tableName == "" {
        return fmt.Errorf("failed to insert secret: missing required parameter: table_name=empty")
    }
    if executor == nil {
        return fmt.Errorf("failed to insert secret: missing required parameter: executor=null")
    }

    // Validate params.
    if err := params.Validate(); err != nil {
        return fmt.Errorf("failed to insert secret: %w", err)
    }

    // Generate INSERT query.
    query := fmt.Sprintf(
        "INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?);",
        s.tableName,
        ColID,
        ColName,
        ColCiphertext,
        ColProviderID,
        ColAAD,
    )

    if params.ID == 0 {
        params.ID = GenerateSecretID()
    }

    // Execute query.
    if _, err := executor.Exec(
        query,
        params.ID,
        params.Name,
        params.Ciphertext,
        params.ProviderID,
        params.AAD,
    ); err != nil {
        var mysqlErr *mysql.MySQLError
        if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
            return fmt.Errorf("failed to insert account: %w", k4k3ruAPI.ErrDuplicateKey)
        }
        return fmt.Errorf("failed to insert secret: %w", err)
    }

    return nil
}

//
// Select secrets.
//
// Version:
//   - 2026-07-18: Added.
//
func (s *SecretStore) Select(executor k4k3ruAPI.Executor, params *SecretSelectParams) ([]*Secret, error) {
    // Guard.
    if s == nil {
        return nil, fmt.Errorf("failed to select secrets: missing required parameter: secret_store=null")
    }
    if s.tableName == "" {
        return nil, fmt.Errorf("failed to select secrets: missing required parameter: table_name=empty")
    }
    if executor == nil {
        return nil, fmt.Errorf("failed to select secrets: missing required parameter: executor=null")
    }

    // Validate params.
    if err := params.Validate(); err != nil {
        return nil, fmt.Errorf("failed to select secrets: %w", err)
    }

    query, args := params.BuildQuery("SELECT * FROM " + s.tableName)

    // Execute.
    rows, err := executor.Query(query, args...)
    if err != nil {
        return nil, fmt.Errorf("failed to select secrets: %w", err)
    }
    defer rows.Close()

    // Scan.
    var result []*Secret
    for rows.Next() {
        row := &Secret{}
        err := rows.Scan(
            &row.ID,
            &row.Name,
            &row.Ciphertext,
            &row.ProviderID,
            &row.AAD,
        )
        if err != nil {
            return nil, fmt.Errorf("failed to select secrets: %w", err)
        }

        result = append(result, row)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("failed to select secrets: %w", err)
    }

    return result, nil
}

//
// Select secret by name.
//
// Version:
//   - 2026-07-18: Added.
//
func (s *SecretStore) SelectByName(executor k4k3ruAPI.Executor, name string) (*Secret, error) {
    // Guard.
    if s == nil {
        return nil, fmt.Errorf("failed to select secret by name: missing required parameter: secret_store=null")
    }
    if s.tableName == "" {
        return nil, fmt.Errorf("failed to select secret by name: missing required parameter: table_name=empty")
    }
    if executor == nil {
        return nil, fmt.Errorf("failed to select secret by name: missing required parameter: executor=null")
    }
    if err := ValidateSecretName(name); err != nil {
        return nil, fmt.Errorf("failed to select secret by name: %w", err)
    }

    // Generate SELECT query.
    query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1;", s.tableName, ColName)

    // Execute query.
    row := executor.QueryRow(query, name)

    // Scan.
    result := &Secret{}
    err := row.Scan(
        &result.ID,
        &result.Name,
        &result.Ciphertext,
        &result.ProviderID,
        &result.AAD,
    )
    if err != nil {
        if err == sql.ErrNoRows {
            return nil, nil
        }
        return nil, fmt.Errorf("failed to select secret by name: %w", err)
    }

    return result, nil
}

//
// Validate secret insert params.
//
// Version:
//   - 2026-07-18: Added.
//
func (p *SecretInsertParams) Validate() error {
    if p == nil {
        return fmt.Errorf("missing required parameter: secret_insert_params=null")
    }
    if err := ValidateSecretName(p.Name); err != nil {
        return err
    }
    if err := ValidateSecretCiphertext(p.Ciphertext); err != nil {
        return err
    }
    if err := ValidateSecretProviderID(p.ProviderID); err != nil {
        return err
    }
    if err := ValidateSecretAAD(p.AAD); err != nil {
        return err
    }
    return nil
}

//
// Build query.
//
// Version:
//   - 2025-07-08: Added.
//
func (p *SecretSelectParams) BuildQuery(selectFromClause string) (string, []any) {
    if p == nil {
        return selectFromClause, nil
    }

    var query strings.Builder
    query.WriteString(selectFromClause)

    conditions := make([]string, 0, 4)
    args := make([]any, 0, 6)

    if p.ID != nil {
        conditions = append(conditions, ColID+"=?")
        args = append(args, *p.ID)
    }
    if p.Name != nil {
        conditions = append(conditions, ColName+"=?")
        args = append(args, *p.Name)
    }
    if p.NameLike != nil {
        conditions = append(conditions, ColName+"=?")
        args = append(args, "%"+*p.NameLike+"%")
    }
    if p.ProviderID != nil {
        conditions = append(conditions, ColProviderID+"=?")
        args = append(args, *p.ProviderID)
    }

    if len(conditions) > 0 {
        query.WriteString(" WHERE ")
        query.WriteString(strings.Join(conditions, " AND "))
    }

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
// Validate secret select params.
//
// Version:
//   - 2026-07-18: Added.
//
func (p *SecretSelectParams) Validate() error {
    if p == nil {
        return nil
    }

    if p.ID != nil {
        if err := ValidateSecretID(*p.ID); err != nil {
            return err
        }
    }
    if p.Name != nil {
        if err := ValidateSecretName(*p.Name); err != nil {
            return err
        }
    }
    if p.NameLike != nil {
        if err := ValidateSecretName(*p.NameLike); err != nil {
            return err
        }
    }
    if p.ProviderID != nil {
        if err := ValidateSecretProviderID(*p.ProviderID); err != nil {
            return err
        }
    }

    if p.OrderBy != "" {
        if len(p.OrderBy) > 64 {
            return fmt.Errorf("invalid parameter: order_by=too_long")
        }

        switch p.OrderBy {
        case ColID, ColName, ColCiphertext, ColProviderID, ColAAD:
        default:
            return fmt.Errorf("invalid parameter: order_by=%q", p.OrderBy)
        }
    }

    if p.Limit < 0 {
        return fmt.Errorf("invalid parameter: limit=%d", p.Limit)
    }
    if p.Offset < 0 {
        return fmt.Errorf("invalid parameter: offset=%d", p.Offset)
    }

    return nil
}

