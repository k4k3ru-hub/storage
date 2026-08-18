package auth

import (
	"database/sql/driver"
	"fmt"

	k4k3ruInternalSQLScan "github.com/k4k3ru-hub/storage/go/internal/sqlscan"
)

type OTPChannel uint8

const (
	OTPChannelEmail OTPChannel = iota + 1
	OTPChannelSMS
)

// IsValid reports whether the OTP channel is valid.
//
// Version:
//   - 2026-08-18: Added.
func (c OTPChannel) IsValid() bool {
	return c == OTPChannelEmail || c == OTPChannelSMS
}

// String returns the OTP channel name.
//
// Version:
//   - 2026-08-18: Added.
func (c OTPChannel) String() string {
	switch c {
	case OTPChannelEmail:
		return "email"
	case OTPChannelSMS:
		return "sms"
	default:
		return ""
	}
}

// Validate validates the OTP channel.
//
// Version:
//   - 2026-08-18: Added.
func (c OTPChannel) Validate() error {
	if !c.IsValid() {
		return fmt.Errorf("invalid parameter: otp_channel=invalid")
	}
	return nil
}

// Value returns the OTP channel as a driver.Value.
//
// Version:
//   - 2026-08-18: Added.
func (c OTPChannel) Value() (driver.Value, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return int64(c), nil
}

// Scan scans and validates an OTP channel.
//
// Version:
//   - 2026-08-18: Added.
func (c *OTPChannel) Scan(value any) error {
	if c == nil {
		return fmt.Errorf("failed to scan otp channel: otp_channel=null")
	}
	parsed, err := k4k3ruInternalSQLScan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan otp channel: %w", err)
	}
	result := OTPChannel(parsed)
	if err := result.Validate(); err != nil {
		return fmt.Errorf("failed to scan otp channel: %w", err)
	}
	*c = result
	return nil
}
