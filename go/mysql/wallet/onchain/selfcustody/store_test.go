package selfcustody

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type ddlExecutor struct {
	*sql.DB
	queries []string
	err     error
}

func (e *ddlExecutor) ExecContext(_ context.Context, query string, _ ...any) (sql.Result, error) {
	e.queries = append(e.queries, query)
	return nil, e.err
}

// TestComposition verifies configured tables and references.
//
// Version:
//   - 2026-09-22: Added.
func TestComposition(t *testing.T) {
	s, err := NewStore("console_"+DefaultAddressTableName, "console_"+DefaultLinkTableName, "console_"+DefaultLinkChallengeTableName)
	if err != nil {
		t.Fatal(err)
	}
	e := &ddlExecutor{}
	if err := s.CreateTables(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if len(e.queries) != 3 {
		t.Fatalf("expected three tables, got %d", len(e.queries))
	}
	if !strings.Contains(e.queries[1], "REFERENCES `console_"+DefaultAddressTableName+"`") {
		t.Fatal("foreign key did not use configured name")
	}
	// Replacement must be simultaneous even when a supplied name is another default.
	s, err = NewStore(DefaultLinkTableName, DefaultAddressTableName, "challenges")
	if err != nil {
		t.Fatal(err)
	}
	ddl, err := s.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ddl, "CREATE TABLE IF NOT EXISTS `"+DefaultLinkTableName+"`") {
		t.Fatal("table identifiers were replaced recursively")
	}
}

// TestInvalidComposition rejects unsafe or colliding table names.
//
// Version:
//   - 2026-09-22: Added.
func TestInvalidComposition(t *testing.T) {
	for _, names := range [][3]string{{"", "links", "challenges"}, {"a.b", "links", "challenges"}, {"a`; DROP TABLE x", "links", "challenges"}, {strings.Repeat("x", 65), "links", "challenges"}, {"wallet", "WALLET", "challenges"}} {
		if _, err := NewStore(names[0], names[1], names[2]); err == nil {
			t.Fatalf("accepted invalid table names: %q", names)
		}
	}
}

// TestDDLErrors verifies dependency checks and underlying error propagation.
//
// Version:
//   - 2026-09-22: Added.
func TestDDLErrors(t *testing.T) {
	s, err := NewDefaultStore()
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("test database failure")
	e := &ddlExecutor{err: failure}
	if err := s.CreateTables(t.Context(), e); !errors.Is(err, failure) {
		t.Fatalf("lost underlying error: %v", err)
	}
	if len(e.queries) != 1 {
		t.Fatal("continued after DDL failure")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.CreateTables(ctx, &ddlExecutor{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var nilExecutor *ddlExecutor
	if err := s.CreateTables(t.Context(), nilExecutor); err == nil {
		t.Fatal("accepted nil executor")
	}
	if err := s.CreateTables(nil, &ddlExecutor{}); err == nil {
		t.Fatal("accepted nil context")
	}
	var empty Store
	if _, err := empty.Schema(); err == nil {
		t.Fatal("accepted uninitialized store")
	}
}
