package selfcustody

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	api "github.com/k4k3ru-hub/storage/go/api"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalidParameter = errors.New("failed to validate wallet parameters")
	ErrDuplicate        = errors.New("failed to write wallet record: duplicate")
	ErrConflict         = errors.New("failed to change wallet state: conflict")
)

func invalid(field, state string) error {
	return fmt.Errorf("%w: %s=%s", ErrInvalidParameter, field, state)
}
func (s *Store) guard(ctx context.Context, executor api.Executor) error {
	if s == nil || s.addressTable == "" || s.linkTable == "" || s.challengeTable == "" {
		return invalid("store", "null")
	}
	if ctx == nil || executor == nil {
		return invalid("dependency", "null")
	}
	v := reflect.ValueOf(executor)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return invalid("executor", "null")
	}
	return ctx.Err()
}
func textField(v, field string, max int, empty bool) error {
	if !utf8.ValidString(v) {
		return invalid(field, "invalid")
	}
	if utf8.RuneCountInString(v) > max {
		return invalid(field, "too_long")
	}
	if !empty && strings.TrimSpace(v) == "" {
		return invalid(field, "empty")
	}
	if strings.TrimSpace(v) != v || strings.ContainsRune(v, 0) {
		return invalid(field, "invalid")
	}
	return nil
}
func identity(family, address string) error {
	for _, f := range []struct {
		v, n string
		max  int
	}{{family, "chain_family", 32}, {address, "address", 255}} {
		if err := textField(f.v, f.n, f.max, false); err != nil {
			return err
		}
		for _, r := range f.v {
			if r < 33 || r > 126 {
				return invalid(f.n, "invalid")
			}
		}
	}
	if family != strings.ToLower(family) {
		return invalid("chain_family", "invalid")
	}
	return nil
}
func timestamp(t time.Time, field string) error {
	if t.IsZero() {
		return invalid(field, "empty")
	}
	if t.UTC().Year() < 1000 || t.UTC().Year() > 9999 || t.Nanosecond()%1000 != 0 {
		return invalid(field, "out_of_range")
	}
	return nil
}
func positive(id uint64, field string) error {
	if id == 0 {
		return invalid(field, "empty")
	}
	return nil
}
func quote(s string) string { return "`" + s + "`" }

// Database details can include challenge messages or subjects. Keep the cause inspectable.
type databaseError struct{ cause error }

// Error returns a fixed message without database payloads.
//
// Version:
//   - 2026-09-22: Added.
func (e *databaseError) Error() string { return "failed to access wallet storage" }

// Unwrap preserves inspection of the underlying database error.
//
// Version:
//   - 2026-09-22: Added.
func (e *databaseError) Unwrap() error { return e.cause }
func dbError(op string, err error) error {
	if err == nil {
		return nil
	}
	safe := error(&databaseError{err})
	var duplicate *mysql.MySQLError
	if errors.As(err, &duplicate) && duplicate.Number == 1062 {
		safe = errors.Join(ErrDuplicate, safe)
	}
	return fmt.Errorf("%s: %w", op, safe)
}

type scanner interface{ Scan(...any) error }

func scanAddress(row scanner) (*Address, error) {
	var v Address
	err := row.Scan(&v.ID, &v.ChainFamily, &v.Address, &v.CreatedAt)
	return &v, err
}
func scanLink(row scanner) (*Link, error) {
	var v Link
	err := row.Scan(&v.ID, &v.AddressID, &v.Subject, &v.DisplayName, &v.LinkedAt, &v.UnlinkedAt)
	return &v, err
}
func scanChallenge(row scanner) (*LinkChallenge, error) {
	var v LinkChallenge
	var binding, nonce []byte
	err := row.Scan(&v.ID, &v.Subject, &binding, &v.ChainFamily, &v.Address, &nonce, &v.Message, &v.CreatedAt, &v.ExpiresAt, &v.ConsumedAt)
	if err != nil {
		return nil, err
	}
	if len(binding) != 32 || len(nonce) != 32 {
		return nil, ErrInvalidParameter
	}
	copy(v.BindingHash[:], binding)
	copy(v.Nonce[:], nonce)
	return &v, nil
}
