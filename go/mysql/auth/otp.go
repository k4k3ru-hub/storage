package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	k4k3ruInternalGenerator "github.com/k4k3ru-hub/storage/go/internal/generator"
	k4k3ruMySQLInternalValidator "github.com/k4k3ru-hub/storage/go/mysql/internal/validator"
)

const (
	DefaultOTPTableName         = "auth_otps"
	DefaultCodeLength           = 6
	DefaultMaxVerificationCount = 3
	DefaultExpiresIn            = 10 * time.Minute
	DefaultLockedUntilIn        = 15 * time.Minute
	maxCodeLength               = 10
	maxPurposeLength            = 64
	maxHashLength               = 128
)

var otpIDGenerator = &k4k3ruInternalGenerator.ID{}

type OTP struct {
	ID                       uint64
	Channel                  OTPChannel
	Purpose                  string
	DestinationHash          string
	CodeHash                 string
	SendCount                uint16
	VerificationAttemptCount uint16
	ConsumedAt               *time.Time
	ExpiresAt                time.Time
	LastSentAt               time.Time
	LockedUntil              *time.Time
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

type OTPStore struct {
	tableName string
}

type OTPInsertParams struct {
	ID                       uint64
	Channel                  OTPChannel
	Purpose                  string
	DestinationHash          string
	CodeHash                 string
	SendCount                uint16
	VerificationAttemptCount uint16
	ConsumedAt               *time.Time
	ExpiresAt                time.Time
	LastSentAt               time.Time
	LockedUntil              *time.Time
	CreatedAt                time.Time
	Ignore                   bool
}

type OTPKey struct {
	Channel         OTPChannel
	Purpose         string
	DestinationHash string
}

type OTPIssuanceUpdateParams struct {
	CodeHash                 string
	SendCount                uint16
	VerificationAttemptCount uint16
	ExpiresAt                time.Time
	LastSentAt               time.Time
	LockedUntil              *time.Time
}

type OTPVerifyParams struct {
	Key                    OTPKey
	CodeHash               string
	Now                    time.Time
	MaxVerificationAttempt uint16
	LockDuration           time.Duration
}

type OTPVerifyStatus uint8

const (
	OTPVerifyStatusSucceeded OTPVerifyStatus = iota + 1
	OTPVerifyStatusNotFound
	OTPVerifyStatusAlreadyConsumed
	OTPVerifyStatusExpired
	OTPVerifyStatusLocked
	OTPVerifyStatusFailed
)

type OTPVerifyResult struct {
	Status OTPVerifyStatus
	OTP    *OTP
}

// DefaultExpiresAt returns the default OTP expiration time.
//
// Version:
//   - 2026-08-18: Added.
func DefaultExpiresAt() time.Time {
	return time.Now().UTC().Add(DefaultExpiresIn)
}

// GenerateOTPID generates an OTP record ID.
//
// Version:
//   - 2026-08-18: Added.
func GenerateOTPID() uint64 {
	return otpIDGenerator.Generate()
}

// GenerateCode generates a cryptographically random numeric OTP code.
//
// Parameters:
//   - length: Number of digits.
//
// Returns:
//   - Zero-padded OTP code.
//   - Generation error.
//
// Version:
//   - 2026-08-18: Added.
func GenerateCode(length int) (string, error) {
	if length <= 0 || length > maxCodeLength {
		return "", fmt.Errorf("failed to generate otp code: length=out_of_range min_value=1 max_value=%d", maxCodeLength)
	}
	maximum := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(length)), nil)
	value, err := rand.Int(rand.Reader, maximum)
	if err != nil {
		return "", fmt.Errorf("failed to generate otp code: %w", err)
	}
	return fmt.Sprintf("%0*d", length, value.Uint64()), nil
}

// HashCode hashes an OTP code using HMAC-SHA-256.
//
// Parameters:
//   - secret: Server-held HMAC secret.
//   - code: OTP code.
//   - maximumLength: Maximum accepted code length.
//
// Returns:
//   - Hex-encoded code hash.
//   - Hashing error.
//
// Version:
//   - 2026-08-18: Added.
func HashCode(secret []byte, code string, maximumLength int) (string, error) {
	if len(secret) == 0 {
		return "", fmt.Errorf("failed to hash otp code: secret=empty")
	}
	if maximumLength <= 0 || maximumLength > maxCodeLength {
		return "", fmt.Errorf("failed to hash otp code: maximum_length=out_of_range min_value=1 max_value=%d", maxCodeLength)
	}
	code = strings.TrimSpace(code)
	if code == "" {
		return "", fmt.Errorf("failed to hash otp code: code=empty")
	}
	if len(code) > maximumLength {
		return "", fmt.Errorf("failed to hash otp code: code=too_long max_length=%d", maximumLength)
	}
	mac := hmac.New(sha256.New, secret)
	if _, err := mac.Write([]byte(code)); err != nil {
		return "", fmt.Errorf("failed to hash otp code: %w", err)
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// NormalizeDestination normalizes an OTP destination.
//
// Parameters:
//   - channel: Delivery channel.
//   - destination: Raw email address or phone number.
//
// Returns:
//   - Normalized destination.
//   - Normalization error.
//
// Version:
//   - 2026-08-18: Added.
func NormalizeDestination(channel OTPChannel, destination string) (string, error) {
	if err := channel.Validate(); err != nil {
		return "", fmt.Errorf("failed to normalize otp destination: %w", err)
	}
	switch channel {
	case OTPChannelEmail:
		return normalizeEmail(destination)
	case OTPChannelSMS:
		return normalizePhone(destination)
	default:
		return "", fmt.Errorf("failed to normalize otp destination: otp_channel=invalid")
	}
}

// HashDestination hashes a normalized OTP destination using HMAC-SHA-256.
//
// Parameters:
//   - secret: Server-held HMAC secret.
//   - destination: Normalized destination.
//
// Returns:
//   - Hex-encoded destination hash.
//   - Hashing error.
//
// Version:
//   - 2026-08-18: Added.
func HashDestination(secret []byte, destination string) (string, error) {
	if len(secret) == 0 {
		return "", fmt.Errorf("failed to hash otp destination: secret=empty")
	}
	if destination == "" {
		return "", fmt.Errorf("failed to hash otp destination: destination=empty")
	}
	mac := hmac.New(sha256.New, secret)
	if _, err := mac.Write([]byte(destination)); err != nil {
		return "", fmt.Errorf("failed to hash otp destination: %w", err)
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// NewOTPStore creates an OTP store.
//
// Version:
//   - 2026-08-18: Added.
func NewOTPStore(tableName string) (*OTPStore, error) {
	operationErr := "failed to create otp store"
	tableName = strings.TrimSpace(tableName)
	if err := k4k3ruMySQLInternalValidator.ValidateSQLIdentifier(tableName, "table_name"); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	return &OTPStore{tableName: tableName}, nil
}

// CreateTable creates the OTP table.
//
// Version:
//   - 2026-08-18: Added.
func (s *OTPStore) CreateTable(ctx context.Context, executor Executor) error {
	operationErr := "failed to create otp table"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s BIGINT UNSIGNED NOT NULL COMMENT 'ID',
		%s TINYINT UNSIGNED NOT NULL COMMENT 'Channel',
		%s VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Purpose',
		%s VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Destination hash',
		%s VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'Code hash',
		%s SMALLINT UNSIGNED NOT NULL COMMENT 'Send count',
		%s SMALLINT UNSIGNED NOT NULL COMMENT 'Verification attempt count',
		%s DATETIME(6) NULL COMMENT 'Consumed at',
		%s DATETIME(6) NOT NULL COMMENT 'Expires at',
		%s DATETIME(6) NOT NULL COMMENT 'Last sent at',
		%s DATETIME(6) NULL COMMENT 'Locked until',
		%s DATETIME(6) NOT NULL COMMENT 'Created at',
		%s DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'Updated at',
		PRIMARY KEY (%s),
		UNIQUE KEY uq_auth_otp_destination (%s, %s, %s),
		KEY idx_auth_otp_expires_at (%s),
		KEY idx_auth_otp_consumed_at (%s)
	) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8mb4;`, s.tableName,
		ColID, ColChannel, ColPurpose, ColDestinationHash, ColCodeHash, ColSendCount,
		ColVerificationAttemptCount, ColConsumedAt, ColExpiresAt, ColLastSentAt,
		ColLockedUntil, ColCreatedAt, ColUpdatedAt, ColID, ColChannel, ColPurpose,
		ColDestinationHash, ColExpiresAt, ColConsumedAt)
	if _, err := executor.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// Insert inserts an OTP challenge.
//
// Version:
//   - 2026-08-18: Added.
func (s *OTPStore) Insert(ctx context.Context, executor Executor, params *OTPInsertParams) error {
	operationErr := "failed to insert otp"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if params == nil {
		return fmt.Errorf("%s: otp_insert_params=null", operationErr)
	}
	if params.ID == 0 {
		params.ID = GenerateOTPID()
	}
	if params.CreatedAt.IsZero() {
		params.CreatedAt = time.Now().UTC()
	} else {
		params.CreatedAt = params.CreatedAt.UTC()
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	prefix := "INSERT"
	if params.Ignore {
		prefix = "INSERT IGNORE"
	}
	query := fmt.Sprintf("%s INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
		prefix, s.tableName, ColID, ColChannel, ColPurpose, ColDestinationHash, ColCodeHash,
		ColSendCount, ColVerificationAttemptCount, ColConsumedAt, ColExpiresAt, ColLastSentAt,
		ColLockedUntil, ColCreatedAt)
	_, err := executor.ExecContext(ctx, query, params.ID, params.Channel, params.Purpose,
		params.DestinationHash, params.CodeHash, params.SendCount, params.VerificationAttemptCount,
		utcPointer(params.ConsumedAt), params.ExpiresAt.UTC(), params.LastSentAt.UTC(), utcPointer(params.LockedUntil), params.CreatedAt)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("%s: %w: %w", operationErr, ErrDuplicateKey, err)
		}
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return nil
}

// SelectByID selects an OTP challenge by ID.
//
// Version:
//   - 2026-08-18: Added.
func (s *OTPStore) SelectByID(ctx context.Context, executor Executor, id uint64) (*OTP, error) {
	operationErr := "failed to select otp by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, fmt.Errorf("%s: id=empty", operationErr)
	}
	return scanOTP(executor.QueryRowContext(ctx, s.selectClause()+fmt.Sprintf(" WHERE %s = ? LIMIT 1;", ColID), id), operationErr)
}

// SelectForUpdate selects and locks an OTP challenge by its stable key.
//
// Version:
//   - 2026-08-18: Added.
func (s *OTPStore) SelectForUpdate(ctx context.Context, tx *sql.Tx, key OTPKey) (*OTP, error) {
	operationErr := "failed to select otp for update"
	if s == nil || s.tableName == "" {
		return nil, fmt.Errorf("%s: otp_store=null_or_empty", operationErr)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%s: context=null", operationErr)
	}
	if tx == nil {
		return nil, fmt.Errorf("%s: sql_tx=null", operationErr)
	}
	if err := key.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	query := s.selectClause() + fmt.Sprintf(" WHERE %s = ? AND %s = ? AND %s = ? LIMIT 1 FOR UPDATE;", ColChannel, ColPurpose, ColDestinationHash)
	return scanOTP(tx.QueryRowContext(ctx, query, key.Channel, key.Purpose, key.DestinationHash), operationErr)
}

// UpdateIssuanceByID replaces the active OTP issuance values.
//
// Version:
//   - 2026-08-18: Added.
func (s *OTPStore) UpdateIssuanceByID(ctx context.Context, executor Executor, id uint64, params OTPIssuanceUpdateParams) error {
	operationErr := "failed to update otp issuance by id"
	if err := s.validateOperation(ctx, executor, operationErr); err != nil {
		return err
	}
	if id == 0 {
		return fmt.Errorf("%s: id=empty", operationErr)
	}
	if err := params.Validate(); err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	query := fmt.Sprintf("UPDATE %s SET %s=?, %s=?, %s=?, %s=?, %s=?, %s=?, %s=NULL WHERE %s=?;",
		s.tableName, ColCodeHash, ColSendCount, ColVerificationAttemptCount, ColExpiresAt,
		ColLastSentAt, ColLockedUntil, ColConsumedAt, ColID)
	result, err := executor.ExecContext(ctx, query, params.CodeHash, params.SendCount,
		params.VerificationAttemptCount, params.ExpiresAt.UTC(), params.LastSentAt.UTC(),
		utcPointer(params.LockedUntil), id)
	if err != nil {
		return fmt.Errorf("%s: %w", operationErr, err)
	}
	return requireOneAffectedRow(result, operationErr)
}

// Verify verifies and consumes an OTP challenge within a caller-owned transaction.
//
// The caller must commit the transaction for both successful consumption and
// failed-attempt accounting. Verification outcomes are returned as data; only
// database and parameter failures are returned as errors.
//
// Version:
//   - 2026-08-18: Added.
func (s *OTPStore) Verify(ctx context.Context, tx *sql.Tx, params OTPVerifyParams) (OTPVerifyResult, error) {
	operationErr := "failed to verify otp"
	if err := params.Validate(); err != nil {
		return OTPVerifyResult{}, fmt.Errorf("%s: %w", operationErr, err)
	}
	otp, err := s.SelectForUpdate(ctx, tx, params.Key)
	if err != nil {
		return OTPVerifyResult{}, fmt.Errorf("%s: %w", operationErr, err)
	}
	if otp == nil {
		return OTPVerifyResult{Status: OTPVerifyStatusNotFound}, nil
	}
	result := OTPVerifyResult{OTP: otp}
	if otp.ConsumedAt != nil {
		result.Status = OTPVerifyStatusAlreadyConsumed
		return result, nil
	}
	if !otp.ExpiresAt.After(params.Now) {
		result.Status = OTPVerifyStatusExpired
		return result, nil
	}
	if otp.LockedUntil != nil && otp.LockedUntil.After(params.Now) {
		result.Status = OTPVerifyStatusLocked
		return result, nil
	}
	if !hmac.Equal([]byte(otp.CodeHash), []byte(params.CodeHash)) {
		if otp.VerificationAttemptCount == ^uint16(0) {
			return OTPVerifyResult{}, fmt.Errorf("%s: stored verification_attempt_count=out_of_range", operationErr)
		}
		next := otp.VerificationAttemptCount + 1
		var lockedUntil *time.Time
		if next >= params.MaxVerificationAttempt {
			value := params.Now.Add(params.LockDuration)
			lockedUntil = &value
		}
		query := fmt.Sprintf("UPDATE %s SET %s=?, %s=? WHERE %s=? AND %s IS NULL;",
			s.tableName, ColVerificationAttemptCount, ColLockedUntil, ColID, ColConsumedAt)
		dbResult, execErr := tx.ExecContext(ctx, query, next, utcPointer(lockedUntil), otp.ID)
		if execErr != nil {
			return OTPVerifyResult{}, fmt.Errorf("%s: failed to record verification attempt: %w", operationErr, execErr)
		}
		if err := requireOneAffectedRow(dbResult, operationErr); err != nil {
			return OTPVerifyResult{}, err
		}
		otp.VerificationAttemptCount = next
		otp.LockedUntil = lockedUntil
		result.Status = OTPVerifyStatusFailed
		return result, nil
	}
	query := fmt.Sprintf("UPDATE %s SET %s=? WHERE %s=? AND %s IS NULL AND %s>? AND (%s IS NULL OR %s<=?);",
		s.tableName, ColConsumedAt, ColID, ColConsumedAt, ColExpiresAt, ColLockedUntil, ColLockedUntil)
	dbResult, err := tx.ExecContext(ctx, query, params.Now, otp.ID, params.Now, params.Now)
	if err != nil {
		return OTPVerifyResult{}, fmt.Errorf("%s: failed to consume otp: %w", operationErr, err)
	}
	if err := requireOneAffectedRow(dbResult, operationErr); err != nil {
		return OTPVerifyResult{}, err
	}
	consumedAt := params.Now
	otp.ConsumedAt = &consumedAt
	result.Status = OTPVerifyStatusSucceeded
	return result, nil
}

// Validate validates OTP insert parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p *OTPInsertParams) Validate() error {
	if p == nil {
		return fmt.Errorf("invalid parameter: otp_insert_params=null")
	}
	if p.ID == 0 {
		return fmt.Errorf("invalid parameter: id=empty")
	}
	if err := (OTPKey{Channel: p.Channel, Purpose: p.Purpose, DestinationHash: p.DestinationHash}).Validate(); err != nil {
		return err
	}
	if err := validateHash("code_hash", p.CodeHash); err != nil {
		return err
	}
	if p.SendCount == 0 {
		return fmt.Errorf("invalid parameter: send_count=empty")
	}
	if p.ExpiresAt.IsZero() {
		return fmt.Errorf("invalid parameter: expires_at=empty")
	}
	if p.LastSentAt.IsZero() {
		return fmt.Errorf("invalid parameter: last_sent_at=empty")
	}
	if p.CreatedAt.IsZero() {
		return fmt.Errorf("invalid parameter: created_at=empty")
	}
	return validateOptionalTimes(p.ConsumedAt, p.LockedUntil)
}

// Validate validates an OTP stable key.
//
// Version:
//   - 2026-08-18: Added.
func (k OTPKey) Validate() error {
	if err := k.Channel.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(k.Purpose) == "" {
		return fmt.Errorf("invalid parameter: purpose=empty")
	}
	if utf8.RuneCountInString(k.Purpose) > maxPurposeLength {
		return fmt.Errorf("invalid parameter: purpose=too_long max_length=%d", maxPurposeLength)
	}
	for _, character := range k.Purpose {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("._:-", character) {
			continue
		}
		return fmt.Errorf("invalid parameter: purpose=invalid")
	}
	return validateHash("destination_hash", k.DestinationHash)
}

// Validate validates OTP issuance update parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p OTPIssuanceUpdateParams) Validate() error {
	if err := validateHash("code_hash", p.CodeHash); err != nil {
		return err
	}
	if p.SendCount == 0 {
		return fmt.Errorf("invalid parameter: send_count=empty")
	}
	if p.ExpiresAt.IsZero() {
		return fmt.Errorf("invalid parameter: expires_at=empty")
	}
	if p.LastSentAt.IsZero() {
		return fmt.Errorf("invalid parameter: last_sent_at=empty")
	}
	return validateOptionalTimes(nil, p.LockedUntil)
}

// Validate validates OTP verification parameters.
//
// Version:
//   - 2026-08-18: Added.
func (p OTPVerifyParams) Validate() error {
	if err := p.Key.Validate(); err != nil {
		return err
	}
	if err := validateHash("code_hash", p.CodeHash); err != nil {
		return err
	}
	if p.Now.IsZero() {
		return fmt.Errorf("invalid parameter: now=empty")
	}
	if p.MaxVerificationAttempt == 0 {
		return fmt.Errorf("invalid parameter: max_verification_attempt=empty")
	}
	if p.LockDuration <= 0 {
		return fmt.Errorf("invalid parameter: lock_duration=out_of_range")
	}
	return nil
}

func (s *OTPStore) validateOperation(ctx context.Context, executor Executor, operationErr string) error {
	if s == nil {
		return fmt.Errorf("%s: otp_store=null", operationErr)
	}
	if s.tableName == "" {
		return fmt.Errorf("%s: table_name=empty", operationErr)
	}
	if ctx == nil {
		return fmt.Errorf("%s: context=null", operationErr)
	}
	if executor == nil {
		return fmt.Errorf("%s: executor=null", operationErr)
	}
	return nil
}

func (s *OTPStore) selectClause() string {
	return fmt.Sprintf("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s",
		ColID, ColChannel, ColPurpose, ColDestinationHash, ColCodeHash, ColSendCount,
		ColVerificationAttemptCount, ColConsumedAt, ColExpiresAt, ColLastSentAt,
		ColLockedUntil, ColCreatedAt, ColUpdatedAt, s.tableName)
}

func scanOTP(row *sql.Row, operationErr string) (*OTP, error) {
	result := &OTP{}
	if err := row.Scan(&result.ID, &result.Channel, &result.Purpose, &result.DestinationHash,
		&result.CodeHash, &result.SendCount, &result.VerificationAttemptCount, &result.ConsumedAt,
		&result.ExpiresAt, &result.LastSentAt, &result.LockedUntil, &result.CreatedAt,
		&result.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("%s: %w", operationErr, err)
	}
	result.ExpiresAt = result.ExpiresAt.UTC()
	result.LastSentAt = result.LastSentAt.UTC()
	result.CreatedAt = result.CreatedAt.UTC()
	result.UpdatedAt = result.UpdatedAt.UTC()
	result.ConsumedAt = utcPointer(result.ConsumedAt)
	result.LockedUntil = utcPointer(result.LockedUntil)
	return result, nil
}

func requireOneAffectedRow(result sql.Result, operationErr string) error {
	if result == nil {
		return fmt.Errorf("%s: sql_result=null", operationErr)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: failed to read affected rows: %w", operationErr, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: affected_rows=invalid actual_count=%d expected_count=1", operationErr, count)
	}
	return nil
}

func validateHash(parameter, value string) error {
	if value == "" {
		return fmt.Errorf("invalid parameter: %s=empty", parameter)
	}
	if len(value) > maxHashLength {
		return fmt.Errorf("invalid parameter: %s=too_long actual_length=%d max_length=%d", parameter, len(value), maxHashLength)
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return fmt.Errorf("invalid parameter: %s=invalid", parameter)
		}
	}
	return nil
}

func validateOptionalTimes(values ...*time.Time) error {
	for _, value := range values {
		if value != nil && value.IsZero() {
			return fmt.Errorf("invalid parameter: optional_time=empty")
		}
	}
	return nil
}

func utcPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.UTC()
	return &result
}

func normalizeEmail(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "", fmt.Errorf("failed to normalize otp destination: email=empty")
	}
	parts := strings.Split(value, "@")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("failed to normalize otp destination: email=invalid")
	}
	return value, nil
}

func normalizePhone(value string) (string, error) {
	var builder strings.Builder
	for _, character := range strings.TrimSpace(value) {
		switch {
		case character >= '0' && character <= '9':
			builder.WriteRune(character)
		case character >= '０' && character <= '９':
			builder.WriteRune('0' + character - '０')
		case character == '+' && builder.Len() == 0:
			builder.WriteRune(character)
		case unicode.IsSpace(character), character == '-', character == 'ー', character == '−', character == '(', character == ')':
			continue
		default:
			return "", fmt.Errorf("failed to normalize otp destination: phone=invalid")
		}
	}
	if builder.Len() == 0 {
		return "", fmt.Errorf("failed to normalize otp destination: phone=empty")
	}
	return builder.String(), nil
}
