package news

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	api "github.com/k4k3ru-hub/storage/go/api"
	validator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

//go:embed schema.sql
var schema string

type Store struct {
	prefix string
	valid  bool
}

// NewStore creates a store whose tables are prefix + news_* without performing I/O.
// An empty prefix selects generic names. Prefixes are limited to 25 ASCII bytes so
// MySQL's generated foreign-key names fit its 64-character identifier limit.
//
// Version:
//   - 2026-10-31: Added.
func NewStore(prefix string) (*Store, error) {
	if len(prefix) > 25 {
		return nil, invalid("create news store", "prefix=too_long max_length=25")
	}
	if err := validator.ValidateSQLIdentifier(prefix+"news_publishers", "prefix"); err != nil {
		return nil, fmt.Errorf("failed to create news store: %w: %w", ErrInvalid, err)
	}
	return &Store{prefix: prefix, valid: true}, nil
}

// Schema returns initial DDL for explicit application-owned migration execution.
// It neither connects to a database nor alters an existing schema.
//
// Version:
//   - 2026-10-31: Added.
func (s *Store) Schema() (string, error) {
	if s == nil || !s.valid {
		return "", invalid("read news schema", "store=null")
	}
	re := regexp.MustCompile(`\bnews_(?:publishers|sources|fetch_runs|articles|article_revisions|fetch_run_items|categories|annotation_sets|article_categories|article_entities)\b`)
	return re.ReplaceAllStringFunc(schema, func(name string) string { return "`" + s.prefix + name + "`" }), nil
}

func (s *Store) table(suffix string) string { return "`" + s.prefix + "news_" + suffix + "`" }
func invalid(operation, state string) error {
	return fmt.Errorf("failed to %s: %w: %s", operation, ErrInvalid, state)
}
func conflict(operation string) error { return fmt.Errorf("failed to %s: %w", operation, ErrConflict) }
func integrity(operation string) error {
	return fmt.Errorf("failed to %s: %w", operation, ErrIntegrity)
}
func dependency(s *Store, ctx context.Context, executor api.Executor, operation string) error {
	if s == nil || !s.valid {
		return invalid(operation, "store=null")
	}
	if ctx == nil {
		return invalid(operation, "context=null")
	}
	if executor == nil {
		return invalid(operation, "executor=null")
	}
	v := reflect.ValueOf(executor)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return invalid(operation, "executor=null")
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failed to %s: %w", operation, err)
	}
	return nil
}
func writeDependency(s *Store, ctx context.Context, tx *sql.Tx, operation string) error {
	return dependency(s, ctx, tx, operation)
}
func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func equalJSON(a, b []byte) bool { return string(a) == string(b) }
func rowsError(rows *sql.Rows, operation string) error {
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to %s: %w", operation, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("failed to %s: %w", operation, err)
	}
	return nil
}
func rowError(err error, operation string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to %s: %w: %w", operation, ErrNotFound, err)
	}
	return fmt.Errorf("failed to %s: %w", operation, err)
}
func keyOrder(a, b []byte) int { return strings.Compare(string(a), string(b)) }
