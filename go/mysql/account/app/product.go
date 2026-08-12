//
// product.go
//
package app

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	k4k3ruAPI "github.com/k4k3ru-hub/storage/go/api"
	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruInternalSQLScan "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const DefaultProductTableName = "account_app_products"

var productIDGenerator = &k4k3ruInternalGenerator.ID{}

type Product struct {
	ID            uint64
	Name          string
	Status        ProductStatus
	Type          ProductType
	CreditTicks   uint64
	BonusTicks    uint64
	PriceAmount   uint64
	PriceCurrency PriceCurrency
	ExpiresInDays uint32
	PurchaseLimit uint32
	Description   *string
	MetaData      *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type ProductStatus uint8

const (
	ProductStatusInactive ProductStatus = iota
	ProductStatusActive
)

type ProductType uint8

const (
	ProductTypeSystem ProductType = iota
	ProductTypeGeneral
	ProductTypeCampaign
	ProductTypeTrial
)

type PriceCurrency string

const (
	PriceCurrencyUSD PriceCurrency = "usd"
	PriceCurrencyEUR PriceCurrency = "eur"
	PriceCurrencyJPY PriceCurrency = "jpy"
)

type ProductStore struct {
	tableName string
}

type ProductInsertParams struct {
	ID            uint64
	Name          string
	Status        ProductStatus
	Type          ProductType
	CreditTicks   uint64
	BonusTicks    uint64
	PriceAmount   uint64
	PriceCurrency PriceCurrency
	ExpiresInDays uint32
	PurchaseLimit uint32
	Description   *string
	MetaData      *string
	CreatedAt     time.Time
	Ignore        bool
}

type ProductSelectParams struct {
	ID             *uint64
	Name           *string
	Status         *ProductStatus
	Type           *ProductType
	TypeNE         *ProductType
	CreditTicksGTE *uint64
	CreditTicksLTE *uint64
	BonusTicksGTE  *uint64
	BonusTicksLTE  *uint64
	PriceAmountGTE *uint64
	PriceAmountLTE *uint64
	PriceCurrency  *PriceCurrency
	ExpiresInDays  *uint32
	PurchaseLimit  *uint32
	CreatedAtGTE   *time.Time
	CreatedAtLTE   *time.Time
	OrderBy        string
	OrderByDesc    bool
	Limit          int
	Offset         int
}

type ProductUpdateParams struct {
	Name               *string
	Status             *ProductStatus
	Type               *ProductType
	CreditTicks        *uint64
	BonusTicks         *uint64
	PriceAmount        *uint64
	PriceCurrency      *PriceCurrency
	ExpiresInDays      *uint32
	PurchaseLimit      *uint32
	Description        *string
	MetaData           *string
	SetNullDescription bool
	SetNullMetaData    bool
}

// GenerateProductID generates a product ID.
//
// Version:
//   - 2026-08-12: Added.
func GenerateProductID() uint64 {
	return productIDGenerator.Generate()
}

// NewProductStore creates a product store.
//
// Parameters:
//   - tableName: product table name.
//
// Returns:
//   - Product store.
//   - Creation error.
//
// Version:
//   - 2026-08-12: Added.
func NewProductStore(tableName string) (*ProductStore, error) {
	operationErr := "failed to create account app product store"

	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}

	return &ProductStore{tableName: tableName}, nil
}

// ValidateProductID validates a product ID.
//
// Parameters:
//   - id: product ID.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-12: Added.
func ValidateProductID(id uint64) error {
	if id == 0 {
		return fmt.Errorf("invalid parameter: id=empty")
	}
	return nil
}

// ValidateProductName validates a product name.
//
// Parameters:
//   - name: product name.
//
// Returns:
//   - Validation error.
//
// Version:
//   - 2026-08-12: Added with a maximum length of 128 characters.
func ValidateProductName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("invalid parameter: name=empty")
	}
	if utf8.RuneCountInString(name) > 128 {
		return fmt.Errorf("invalid parameter: name=too_long max_length=128")
	}
	return nil
}

// ValidateProductStatus validates a product status.
//
// Version:
//   - 2026-08-12: Added.
func ValidateProductStatus(status ProductStatus) error {
	return status.Validate()
}

// ValidateProductType validates a product type.
//
// Version:
//   - 2026-08-12: Added.
func ValidateProductType(productType ProductType) error {
	return productType.Validate()
}

// ValidateProductPriceCurrency validates a product price currency.
//
// Version:
//   - 2026-08-12: Added.
func ValidateProductPriceCurrency(currency PriceCurrency) error {
	return currency.Validate()
}

// ValidateProductDescription validates a product description.
//
// Version:
//   - 2026-08-12: Added.
func ValidateProductDescription(description *string) error {
	if description != nil && utf8.RuneCountInString(*description) > 255 {
		return fmt.Errorf("invalid parameter: description=too_long max_length=255")
	}
	return nil
}

// ValidateProductMetaData validates product metadata.
//
// Version:
//   - 2026-08-12: Added.
func ValidateProductMetaData(metaData *string) error {
	if metaData == nil {
		return nil
	}
	if len([]byte(*metaData)) > 4096 {
		return fmt.Errorf("invalid parameter: meta_data=too_long max_size=4096")
	}
	if !json.Valid([]byte(*metaData)) {
		return fmt.Errorf("invalid parameter: meta_data=invalid")
	}
	return nil
}

// ValidateID validates the product ID.
//
// Version:
//   - 2026-08-12: Added.
func (p *Product) ValidateID() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product=null")
	}
	return ValidateProductID(p.ID)
}

// ValidateName validates the product name.
//
// Version:
//   - 2026-08-12: Added.
func (p *Product) ValidateName() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product=null")
	}
	return ValidateProductName(p.Name)
}

// ValidateStatus validates the product status.
//
// Version:
//   - 2026-08-12: Added.
func (p *Product) ValidateStatus() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product=null")
	}
	return ValidateProductStatus(p.Status)
}

// ValidateType validates the product type.
//
// Version:
//   - 2026-08-12: Added.
func (p *Product) ValidateType() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product=null")
	}
	return ValidateProductType(p.Type)
}

// ValidatePriceCurrency validates the product price currency.
//
// Version:
//   - 2026-08-12: Added.
func (p *Product) ValidatePriceCurrency() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product=null")
	}
	return ValidateProductPriceCurrency(p.PriceCurrency)
}

// ValidateDescription validates the product description.
//
// Version:
//   - 2026-08-12: Added.
func (p *Product) ValidateDescription() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product=null")
	}
	return ValidateProductDescription(p.Description)
}

// ValidateMetaData validates the product metadata.
//
// Version:
//   - 2026-08-12: Added.
func (p *Product) ValidateMetaData() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product=null")
	}
	return ValidateProductMetaData(p.MetaData)
}

// CreateTable creates the product table.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) CreateTable(ctx context.Context, executor k4k3ruAPI.Executor) error {
	operationErr := "failed to create account app product table"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return err
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
            %s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
            %s VARCHAR(128) NOT NULL COMMENT 'Name',
            %s TINYINT UNSIGNED NOT NULL COMMENT 'Status',
            %s TINYINT UNSIGNED NOT NULL COMMENT 'Type',
            %s BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Credit ticks',
            %s BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Bonus ticks',
            %s BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Price amount',
            %s VARCHAR(16) NOT NULL COMMENT 'Price currency',
            %s INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Expires in days',
            %s INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Purchase limit',
            %s VARCHAR(255) NULL COMMENT 'Description',
            %s JSON NULL COMMENT 'Meta data',
            %s DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Created at',
            %s DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Updated at',
            PRIMARY KEY (%s),
            UNIQUE KEY uk_account_app_products_name (%s),
            KEY idx_account_app_products_status (%s),
            KEY idx_account_app_products_type (%s),
            KEY idx_account_app_products_price_currency (%s)
        ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`,
		s.tableName, ColID, ColName, ColStatus, ColType, ColCreditTicks, ColBonusTicks,
		ColPriceAmount, ColPriceCurrency, ColExpiresInDays, ColPurchaseLimit,
		ColDescription, ColMetaData, ColCreatedAt, ColUpdatedAt, ColID, ColName,
		ColStatus, ColType, ColPriceCurrency,
	)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Insert inserts a product.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) Insert(ctx context.Context, executor k4k3ruAPI.Executor, params *ProductInsertParams) error {
	operationErr := "failed to insert account app product"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: invalid parameter: product_insert_params=null", operationErr)
	}
	if params.ID == 0 {
		params.ID = GenerateProductID()
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}

	queryPrefix := "INSERT"
	if params.Ignore {
		queryPrefix = "INSERT IGNORE"
	}
	query := fmt.Sprintf(
		"%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
		queryPrefix, s.tableName, ColID, ColName, ColStatus, ColType, ColCreditTicks,
		ColBonusTicks, ColPriceAmount, ColPriceCurrency, ColExpiresInDays,
		ColPurchaseLimit, ColDescription, ColMetaData, ColCreatedAt,
	)
	if _, err := executor.ExecContext(ctx, query, params.ID, params.Name, params.Status,
		params.Type, params.CreditTicks, params.BonusTicks, params.PriceAmount,
		params.PriceCurrency, params.ExpiresInDays, params.PurchaseLimit,
		params.Description, params.MetaData, params.CreatedAt); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, k4k3ruAPI.ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// SelectByID selects a product by ID.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) SelectByID(ctx context.Context, executor k4k3ruAPI.Executor, id uint64) (*Product, error) {
	operationErr := "failed to select account app product by id"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := ValidateProductID(id); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return s.selectOne(ctx, executor, operationErr, ColID+"=?", id)
}

// SelectByName selects a product by name.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) SelectByName(ctx context.Context, executor k4k3ruAPI.Executor, name string) (*Product, error) {
	operationErr := "failed to select account app product by name"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := ValidateProductName(name); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return s.selectOne(ctx, executor, operationErr, ColName+"=?", name)
}

// Select selects products.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) Select(ctx context.Context, executor k4k3ruAPI.Executor, params ProductSelectParams) ([]*Product, error) {
	operationErr := "failed to select account app products"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if err := params.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query, args := params.BuildQuery("SELECT * FROM " + s.tableName)
	rows, err := executor.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	defer rows.Close()

	var products []*Product
	for rows.Next() {
		product := &Product{}
		if err := scanProduct(rows, product); err != nil {
			return nil, fmt.Errorf("%s: %w", operationErr, err)
		}
		products = append(products, product)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return products, nil
}

// Count counts products.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) Count(ctx context.Context, executor k4k3ruAPI.Executor, params ProductSelectParams) (uint64, error) {
	operationErr := "failed to count account app products"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return 0, err
	}
	if err := params.Validate(); err != nil {
		return 0, fmt.Errorf("%s: %w", operationErr, err)
	}
	params.OrderBy = ""
	params.OrderByDesc = false
	params.Limit = 0
	params.Offset = 0
	query, args := params.BuildQuery("SELECT COUNT(*) FROM " + s.tableName)
	var count uint64
	if err := executor.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("%s: %w", operationErr, err)
	}
	return count, nil
}

// UpdateByID updates a product by ID.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) UpdateByID(ctx context.Context, executor k4k3ruAPI.Executor, params ProductUpdateParams, id uint64) error {
	operationErr := "failed to update account app product by id"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return err
	}
	if err := ValidateProductID(id); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	assignments, args := params.BuildAssignments()
	if len(assignments) == 0 {
		return fmt.Errorf("%s: invalid parameter: assignments=empty", operationErr)
	}
	args = append(args, id)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s=?;", s.tableName, strings.Join(assignments, ", "), ColID)
	if _, err := executor.ExecContext(ctx, query, args...); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, k4k3ruAPI.ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// DeleteByID deletes a product by ID.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStore) DeleteByID(ctx context.Context, executor k4k3ruAPI.Executor, id uint64) error {
	operationErr := "failed to delete account app product by id"
	if err := s.validate(ctx, executor, operationErr); err != nil {
		return err
	}
	if err := ValidateProductID(id); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s=?;", s.tableName, ColID)
	if _, err := executor.ExecContext(ctx, query, id); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Validate validates product insert parameters.
//
// Version:
//   - 2026-08-12: Added.
func (p *ProductInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: product_insert_params=null")
	}
	if err := ValidateProductID(p.ID); err != nil {
		return err
	}
	if err := ValidateProductName(p.Name); err != nil {
		return err
	}
	if err := ValidateProductStatus(p.Status); err != nil {
		return err
	}
	if err := ValidateProductType(p.Type); err != nil {
		return err
	}
	if err := ValidateProductPriceCurrency(p.PriceCurrency); err != nil {
		return err
	}
	if err := ValidateProductDescription(p.Description); err != nil {
		return err
	}
	return ValidateProductMetaData(p.MetaData)
}

// BuildQuery builds a product SELECT query.
//
// Version:
//   - 2026-08-12: Added.
func (p ProductSelectParams) BuildQuery(selectFromClause string) (string, []any) {
	var query strings.Builder
	query.WriteString(selectFromClause)
	conditions := make([]string, 0, 14)
	args := make([]any, 0, 16)
	appendCondition := func(column, operator string, value any) {
		conditions = append(conditions, column+operator+"?")
		args = append(args, value)
	}
	if p.ID != nil {
		appendCondition(ColID, "=", *p.ID)
	}
	if p.Name != nil {
		appendCondition(ColName, "=", *p.Name)
	}
	if p.Status != nil {
		appendCondition(ColStatus, "=", *p.Status)
	}
	if p.Type != nil {
		appendCondition(ColType, "=", *p.Type)
	}
	if p.TypeNE != nil {
		appendCondition(ColType, "!=", *p.TypeNE)
	}
	if p.CreditTicksGTE != nil {
		appendCondition(ColCreditTicks, ">=", *p.CreditTicksGTE)
	}
	if p.CreditTicksLTE != nil {
		appendCondition(ColCreditTicks, "<=", *p.CreditTicksLTE)
	}
	if p.BonusTicksGTE != nil {
		appendCondition(ColBonusTicks, ">=", *p.BonusTicksGTE)
	}
	if p.BonusTicksLTE != nil {
		appendCondition(ColBonusTicks, "<=", *p.BonusTicksLTE)
	}
	if p.PriceAmountGTE != nil {
		appendCondition(ColPriceAmount, ">=", *p.PriceAmountGTE)
	}
	if p.PriceAmountLTE != nil {
		appendCondition(ColPriceAmount, "<=", *p.PriceAmountLTE)
	}
	if p.PriceCurrency != nil {
		appendCondition(ColPriceCurrency, "=", *p.PriceCurrency)
	}
	if p.ExpiresInDays != nil {
		appendCondition(ColExpiresInDays, "=", *p.ExpiresInDays)
	}
	if p.PurchaseLimit != nil {
		appendCondition(ColPurchaseLimit, "=", *p.PurchaseLimit)
	}
	if p.CreatedAtGTE != nil {
		appendCondition(ColCreatedAt, ">=", *p.CreatedAtGTE)
	}
	if p.CreatedAtLTE != nil {
		appendCondition(ColCreatedAt, "<=", *p.CreatedAtLTE)
	}
	if len(conditions) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(conditions, " AND "))
	}
	if isProductOrderByColumn(p.OrderBy) {
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

// Validate validates product select parameters.
//
// Version:
//   - 2026-08-12: Added.
func (p ProductSelectParams) Validate() error {
	if p.ID != nil {
		if err := ValidateProductID(*p.ID); err != nil {
			return err
		}
	}
	if p.Name != nil {
		if err := ValidateProductName(*p.Name); err != nil {
			return err
		}
	}
	if p.Status != nil {
		if err := ValidateProductStatus(*p.Status); err != nil {
			return err
		}
	}
	if p.Type != nil {
		if err := ValidateProductType(*p.Type); err != nil {
			return err
		}
	}
	if p.TypeNE != nil {
		if err := ValidateProductType(*p.TypeNE); err != nil {
			return err
		}
	}
	if p.Type != nil && p.TypeNE != nil && *p.Type == *p.TypeNE {
		return fmt.Errorf("invalid parameter: product_type_filter=invalid")
	}
	if p.PriceCurrency != nil {
		if err := ValidateProductPriceCurrency(*p.PriceCurrency); err != nil {
			return err
		}
	}
	if p.CreditTicksGTE != nil && p.CreditTicksLTE != nil && *p.CreditTicksGTE > *p.CreditTicksLTE {
		return fmt.Errorf("invalid parameter: credit_ticks_range=invalid")
	}
	if p.BonusTicksGTE != nil && p.BonusTicksLTE != nil && *p.BonusTicksGTE > *p.BonusTicksLTE {
		return fmt.Errorf("invalid parameter: bonus_ticks_range=invalid")
	}
	if p.PriceAmountGTE != nil && p.PriceAmountLTE != nil && *p.PriceAmountGTE > *p.PriceAmountLTE {
		return fmt.Errorf("invalid parameter: price_amount_range=invalid")
	}
	if p.CreatedAtGTE != nil && p.CreatedAtLTE != nil && p.CreatedAtGTE.After(*p.CreatedAtLTE) {
		return fmt.Errorf("invalid parameter: created_at_range=invalid")
	}
	if p.OrderBy != "" && !isProductOrderByColumn(p.OrderBy) {
		return fmt.Errorf("invalid parameter: order_by=invalid")
	}
	if p.Limit < 0 {
		return fmt.Errorf("invalid parameter: limit=out_of_range")
	}
	if p.Offset < 0 {
		return fmt.Errorf("invalid parameter: offset=out_of_range")
	}
	return nil
}

// BuildAssignments builds product UPDATE assignments.
//
// Version:
//   - 2026-08-12: Added.
func (p ProductUpdateParams) BuildAssignments() ([]string, []any) {
	assignments := make([]string, 0, 11)
	args := make([]any, 0, 11)
	appendAssignment := func(column string, value any) {
		assignments = append(assignments, column+"=?")
		args = append(args, value)
	}
	if p.Name != nil {
		appendAssignment(ColName, *p.Name)
	}
	if p.Status != nil {
		appendAssignment(ColStatus, *p.Status)
	}
	if p.Type != nil {
		appendAssignment(ColType, *p.Type)
	}
	if p.CreditTicks != nil {
		appendAssignment(ColCreditTicks, *p.CreditTicks)
	}
	if p.BonusTicks != nil {
		appendAssignment(ColBonusTicks, *p.BonusTicks)
	}
	if p.PriceAmount != nil {
		appendAssignment(ColPriceAmount, *p.PriceAmount)
	}
	if p.PriceCurrency != nil {
		appendAssignment(ColPriceCurrency, *p.PriceCurrency)
	}
	if p.ExpiresInDays != nil {
		appendAssignment(ColExpiresInDays, *p.ExpiresInDays)
	}
	if p.PurchaseLimit != nil {
		appendAssignment(ColPurchaseLimit, *p.PurchaseLimit)
	}
	if p.SetNullDescription {
		assignments = append(assignments, ColDescription+"=NULL")
	} else if p.Description != nil {
		appendAssignment(ColDescription, *p.Description)
	}
	if p.SetNullMetaData {
		assignments = append(assignments, ColMetaData+"=NULL")
	} else if p.MetaData != nil {
		appendAssignment(ColMetaData, *p.MetaData)
	}
	return assignments, args
}

// Validate validates product update parameters.
//
// Version:
//   - 2026-08-12: Added.
func (p ProductUpdateParams) Validate() error {
	if p.Name != nil {
		if err := ValidateProductName(*p.Name); err != nil {
			return err
		}
	}
	if p.Status != nil {
		if err := ValidateProductStatus(*p.Status); err != nil {
			return err
		}
	}
	if p.Type != nil {
		if err := ValidateProductType(*p.Type); err != nil {
			return err
		}
	}
	if p.PriceCurrency != nil {
		if err := ValidateProductPriceCurrency(*p.PriceCurrency); err != nil {
			return err
		}
	}
	if p.SetNullDescription && p.Description != nil {
		return fmt.Errorf("invalid parameter: description=invalid")
	}
	if p.SetNullMetaData && p.MetaData != nil {
		return fmt.Errorf("invalid parameter: meta_data=invalid")
	}
	if err := ValidateProductDescription(p.Description); err != nil {
		return err
	}
	return ValidateProductMetaData(p.MetaData)
}

// IsValid reports whether the product status is valid.
//
// Version:
//   - 2026-08-12: Added.
func (s ProductStatus) IsValid() bool { return s == ProductStatusInactive || s == ProductStatusActive }

// Validate validates the product status.
//
// Version:
//   - 2026-08-12: Added.
func (s ProductStatus) Validate() error {
	if !s.IsValid() {
		return fmt.Errorf("invalid parameter: product_status=%d", s)
	}
	return nil
}

// Value returns the product status as a driver.Value.
//
// Version:
//   - 2026-08-12: Added.
func (s ProductStatus) Value() (driver.Value, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return int64(s), nil
}

// Scan scans a product status.
//
// Version:
//   - 2026-08-12: Added.
func (s *ProductStatus) Scan(value any) error {
	if s == nil {
		return fmt.Errorf("failed to scan product status: invalid parameter: product_status=null")
	}
	v, err := k4k3ruInternalSQLScan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan product status: %w", err)
	}
	result := ProductStatus(v)
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan product status: %w", err)
	}
	*s = result
	return nil
}

// IsValid reports whether the product type is valid.
//
// Version:
//   - 2026-08-12: Added.
func (t ProductType) IsValid() bool { return t >= ProductTypeSystem && t <= ProductTypeTrial }

// Validate validates the product type.
//
// Version:
//   - 2026-08-12: Added.
func (t ProductType) Validate() error {
	if !t.IsValid() {
		return fmt.Errorf("invalid parameter: product_type=%d", t)
	}
	return nil
}

// Value returns the product type as a driver.Value.
//
// Version:
//   - 2026-08-12: Added.
func (t ProductType) Value() (driver.Value, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return int64(t), nil
}

// Scan scans a product type.
//
// Version:
//   - 2026-08-12: Added.
func (t *ProductType) Scan(value any) error {
	if t == nil {
		return fmt.Errorf("failed to scan product type: invalid parameter: product_type=null")
	}
	v, err := k4k3ruInternalSQLScan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan product type: %w", err)
	}
	result := ProductType(v)
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan product type: %w", err)
	}
	*t = result
	return nil
}

// IsValid reports whether the price currency is valid.
//
// Version:
//   - 2026-08-12: Added.
func (c PriceCurrency) IsValid() bool {
	return c == PriceCurrencyUSD || c == PriceCurrencyEUR || c == PriceCurrencyJPY
}

// Validate validates the price currency.
//
// Version:
//   - 2026-08-12: Added.
func (c PriceCurrency) Validate() error {
	if !c.IsValid() {
		return fmt.Errorf("invalid parameter: price_currency=invalid")
	}
	return nil
}

// Value returns the price currency as a driver.Value.
//
// Version:
//   - 2026-08-12: Added.
func (c PriceCurrency) Value() (driver.Value, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return string(c), nil
}

// Scan scans a price currency.
//
// Version:
//   - 2026-08-12: Added.
func (c *PriceCurrency) Scan(value any) error {
	if c == nil {
		return fmt.Errorf("failed to scan price currency: invalid parameter: price_currency=null")
	}
	var result PriceCurrency
	switch v := value.(type) {
	case string:
		result = PriceCurrency(v)
	case []byte:
		result = PriceCurrency(string(v))
	default:
		return fmt.Errorf("failed to scan price currency: unsupported scan type=%T", value)
	}
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan price currency: %w", err)
	}
	*c = result
	return nil
}

type productScanner interface{ Scan(dest ...any) error }

func (s *ProductStore) validate(ctx context.Context, executor k4k3ruAPI.Executor, operationErr string) error {
	if s == nil {
		return fmt.Errorf("%s: invalid parameter: product_store=null", operationErr)
	}
	if s.tableName == "" {
		return fmt.Errorf("%s: invalid parameter: table_name=empty", operationErr)
	}
	if ctx == nil {
		return fmt.Errorf("%s: invalid parameter: context=null", operationErr)
	}
	if executor == nil {
		return fmt.Errorf("%s: invalid parameter: executor=null", operationErr)
	}
	return nil
}

func (s *ProductStore) selectOne(ctx context.Context, executor k4k3ruAPI.Executor, operationErr, condition string, arg any) (*Product, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1;", s.tableName, condition)
	product := &Product{}
	if err := scanProduct(executor.QueryRowContext(ctx, query, arg), product); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return product, nil
}

func scanProduct(scanner productScanner, product *Product) error {
	return scanner.Scan(&product.ID, &product.Name, &product.Status, &product.Type,
		&product.CreditTicks, &product.BonusTicks, &product.PriceAmount,
		&product.PriceCurrency, &product.ExpiresInDays, &product.PurchaseLimit,
		&product.Description, &product.MetaData, &product.CreatedAt, &product.UpdatedAt)
}

func isProductOrderByColumn(column string) bool {
	switch column {
	case ColID, ColName, ColStatus, ColType, ColCreditTicks, ColBonusTicks,
		ColPriceAmount, ColPriceCurrency, ColExpiresInDays, ColPurchaseLimit,
		ColCreatedAt, ColUpdatedAt:
		return true
	default:
		return false
	}
}
