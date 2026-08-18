package auth

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type otpExecutorStub struct {
	query string
	args  []any
	err   error
}

func (e *otpExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}
func (e *otpExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query, e.args = query, args
	return nil, e.err
}
func (*otpExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}
func (*otpExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}
func (*otpExecutorStub) QueryRow(string, ...any) *sql.Row                         { return &sql.Row{} }
func (*otpExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row { return &sql.Row{} }

func validOTPInsertParams() *OTPInsertParams {
	now := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	return &OTPInsertParams{ID: 1, Channel: OTPChannelEmail, Purpose: "login",
		DestinationHash: strings.Repeat("a", 64), CodeHash: strings.Repeat("b", 64),
		SendCount: 1, ExpiresAt: now.Add(10 * time.Minute), LastSentAt: now, CreatedAt: now}
}

func TestOTPStoreCreateTableContract(t *testing.T) {
	store, err := NewOTPStore(DefaultOTPTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &otpExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}
	for _, check := range []string{"UNIQUE KEY uq_auth_otp_destination (channel, purpose, destination_hash)", "verification_attempt_count SMALLINT UNSIGNED NOT NULL", "DATETIME(6)", "destination_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin"} {
		if !strings.Contains(executor.query, check) {
			t.Fatalf("CreateTable() query does not contain %q", check)
		}
	}
}

func TestOTPInsertNormalizesDuplicateKey(t *testing.T) {
	store, err := NewOTPStore(DefaultOTPTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &otpExecutorStub{err: &mysql.MySQLError{Number: 1062, Message: "duplicate"}}
	err = store.Insert(context.Background(), executor, validOTPInsertParams())
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Insert() error = %v, want ErrDuplicateKey", err)
	}
}

func TestHashCodeUsesSecret(t *testing.T) {
	first, err := HashCode([]byte("first-secret"), "123456", DefaultCodeLength)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashCode([]byte("second-secret"), "123456", DefaultCodeLength)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("HashCode() returned the same hash for different secrets")
	}
	if strings.Contains(first, "123456") {
		t.Fatal("HashCode() exposed the OTP code")
	}
}

func TestNormalizeDestination(t *testing.T) {
	email, err := NormalizeDestination(OTPChannelEmail, " User@Example.COM ")
	if err != nil || email != "user@example.com" {
		t.Fatalf("email = %q, error = %v", email, err)
	}
	phone, err := NormalizeDestination(OTPChannelSMS, "+81 (90) １２３４-５６７８")
	if err != nil || phone != "+819012345678" {
		t.Fatalf("phone = %q, error = %v", phone, err)
	}
}

func TestOTPInsertParamsRejectsEmptyPurpose(t *testing.T) {
	params := validOTPInsertParams()
	params.Purpose = ""
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "purpose=empty") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestOTPInsertParamsRejectsNonASCIIPurpose(t *testing.T) {
	params := validOTPInsertParams()
	params.Purpose = "ログイン"
	err := params.Validate()
	if err == nil || !strings.Contains(err.Error(), "purpose=invalid") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestOTPChannelRejectsUnknownDatabaseValue(t *testing.T) {
	var channel OTPChannel
	err := channel.Scan(uint8(255))
	if err == nil || !strings.Contains(err.Error(), "otp_channel=invalid") {
		t.Fatalf("Scan() error = %v", err)
	}
}

func TestOTPStoreRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewOTPStore("auth_otps; DROP TABLE auth_otps"); err == nil {
		t.Fatal("unsafe table name accepted")
	}
}
