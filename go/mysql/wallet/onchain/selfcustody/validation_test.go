package selfcustody

import (
	"database/sql"
	"errors"
	"github.com/go-sql-driver/mysql"
	"strings"
	"testing"
	"time"
)

// TestCRUDValidation verifies invalid data is rejected before persistence.
//
// Version:
//   - 2026-09-22: Added.
func TestCRUDValidation(t *testing.T) {
	s, err := NewDefaultStore()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, change := range []func(*Address){func(v *Address) { v.ID = 0 }, func(v *Address) { v.Address = " " }, func(v *Address) { v.ChainFamily = "EVM" }, func(v *Address) { v.Address = strings.Repeat("a", 256) }, func(v *Address) { v.CreatedAt = now.Add(time.Nanosecond) }} {
		v := Address{ID: 1, ChainFamily: "evm", Address: "0x01", CreatedAt: now}
		change(&v)
		e := &ddlExecutor{}
		if err := s.InsertAddress(t.Context(), e, v); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal(err)
		}
		if len(e.queries) != 0 {
			t.Fatal("invalid address reached database")
		}
	}
	for _, change := range []func(*LinkChallenge){func(v *LinkChallenge) { v.Subject = "" }, func(v *LinkChallenge) { v.Nonce = [32]byte{} }, func(v *LinkChallenge) { v.BindingHash = [32]byte{} }, func(v *LinkChallenge) { v.ExpiresAt = now }, func(v *LinkChallenge) { v.ConsumedAt = &now }, func(v *LinkChallenge) { v.Message = nil }, func(v *LinkChallenge) { v.Message = make([]byte, 65536) }} {
		c := challenge(1, "alice", "0x01", now)
		change(&c)
		e := &ddlExecutor{}
		if err := s.InsertLinkChallenge(t.Context(), e, c); !errors.Is(err, ErrInvalidParameter) {
			t.Fatal(err)
		}
		if len(e.queries) != 0 {
			t.Fatal("invalid challenge reached database")
		}
	}
	var tx *sql.Tx
	if _, err := s.CompleteLink(t.Context(), tx, CompleteLinkParams{}); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal(err)
	}
	if err := s.Unlink(t.Context(), tx, 1, "alice", now); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal(err)
	}
	if _, err := s.ListLinks(t.Context(), &ddlExecutor{}, "alice", 0, 101, false); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal(err)
	}
}

// TestDatabaseErrorPrivacy preserves inspectability without printing confidential database text.
//
// Version:
//   - 2026-09-22: Added.
func TestDatabaseErrorPrivacy(t *testing.T) {
	underlying := &mysql.MySQLError{Number: 1062, Message: "private subject and signed payload"}
	err := dbError("failed to insert wallet record", underlying)
	if !errors.Is(err, ErrDuplicate) || !errors.Is(err, underlying) {
		t.Fatal("lost error identity")
	}
	var got *mysql.MySQLError
	if !errors.As(err, &got) || got != underlying {
		t.Fatal("lost database cause")
	}
	if strings.Contains(err.Error(), underlying.Message) {
		t.Fatal("database payload exposed")
	}
}
