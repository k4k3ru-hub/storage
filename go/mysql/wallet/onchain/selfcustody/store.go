package selfcustody

import (
	"context"
	_ "embed"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	api "github.com/k4k3ru-hub/storage/go/api"
	validator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultAddressTableName       = "wallet_onchain_selfcustody_addresses"
	DefaultLinkTableName          = "wallet_onchain_selfcustody_links"
	DefaultLinkChallengeTableName = "wallet_onchain_selfcustody_link_challenges"
)

//go:embed schema.sql
var schema string

type Store struct{ addressTable, linkTable, challengeTable string }

// NewStore creates a self-custody wallet store with application-owned table names.
// No database connection or schema mutation occurs during construction.
//
// Version:
//   - 2026-09-22: Added.
func NewStore(addressTable, linkTable, challengeTable string) (*Store, error) {
	seen := make(map[string]bool)
	for _, name := range []string{addressTable, linkTable, challengeTable} {
		if err := validator.ValidateSQLIdentifier(name, "table_name"); err != nil {
			return nil, fmt.Errorf("failed to create self-custody wallet store: %w", err)
		}
		if len(name) > 64 {
			return nil, fmt.Errorf("failed to create self-custody wallet store: table_name=too_long max_length=64")
		}
		if seen[strings.ToLower(name)] {
			return nil, fmt.Errorf("failed to create self-custody wallet store: duplicate table names: table_name=invalid")
		}
		seen[strings.ToLower(name)] = true
	}
	return &Store{addressTable, linkTable, challengeTable}, nil
}

// NewDefaultStore creates a store using the generic table names.
//
// Version:
//   - 2026-09-22: Added.
func NewDefaultStore() (*Store, error) {
	return NewStore(DefaultAddressTableName, DefaultLinkTableName, DefaultLinkChallengeTableName)
}

// Schema returns initial DDL with configured table names for application migrations.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) Schema() (string, error) {
	if s == nil || s.addressTable == "" || s.linkTable == "" || s.challengeTable == "" {
		return "", fmt.Errorf("failed to read self-custody wallet schema: store=null")
	}
	names := map[string]string{DefaultAddressTableName: s.addressTable, DefaultLinkTableName: s.linkTable, DefaultLinkChallengeTableName: s.challengeTable}
	pattern := regexp.MustCompile(`\b(?:wallet_onchain_selfcustody_addresses|wallet_onchain_selfcustody_links|wallet_onchain_selfcustody_link_challenges)\b`)
	return pattern.ReplaceAllStringFunc(schema, func(name string) string { return "`" + names[name] + "`" }), nil
}

// CreateTables applies initial DDL through a caller-owned executor.
// MySQL DDL is not transactional. Call this only from explicit migration code;
// it does not upgrade or validate tables that already exist.
//
// Version:
//   - 2026-09-22: Added.
func (s *Store) CreateTables(ctx context.Context, executor api.Executor) error {
	if ctx == nil || executor == nil {
		return fmt.Errorf("failed to create self-custody wallet tables: dependency=null")
	}
	v := reflect.ValueOf(executor)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return fmt.Errorf("failed to create self-custody wallet tables: executor=null")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failed to create self-custody wallet tables: %w", err)
	}
	ddl, err := s.Schema()
	if err != nil {
		return fmt.Errorf("failed to create self-custody wallet tables: %w", err)
	}
	for _, statement := range strings.Split(ddl, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := executor.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("failed to create self-custody wallet tables: %w", err)
		}
	}
	return nil
}
