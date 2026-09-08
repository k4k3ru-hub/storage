// types.go
package api

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/k4k3ru-hub/storage/go/internal/sqlscan"
)

type CredentialStatus uint8

const (
	CredentialStatusPending CredentialStatus = iota
	CredentialStatusActive
	CredentialStatusExpired
	CredentialStatusSuspended
	CredentialStatusRevoked
)

// String convert credential status to string.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialStatus) String() string {
	switch s {
	case CredentialStatusPending:
		return "pending"
	case CredentialStatusActive:
		return "active"
	case CredentialStatusExpired:
		return "expired"
	case CredentialStatusRevoked:
		return "revoked"
	case CredentialStatusSuspended:
		return "suspended"
	default:
		return ""
	}
}

// IsValid check whether credential status is valid.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialStatus) IsValid() bool {
	return s <= CredentialStatusRevoked
}

// Validate credential status.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialStatus) Validate() error {
	if !s.IsValid() {
		return fmt.Errorf("failed to validate credential status: status=out_of_range")
	}
	return nil
}

// Value get credential status as driver.Valuer.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialStatus) Value() (driver.Value, error) {
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("failed to process api credential: %w", err)
	}

	return int64(s), nil
}

// Scan credential status as sql.Scanner.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s *CredentialStatus) Scan(value any) error {
	if s == nil {
		return fmt.Errorf("failed to scan: missing required parameter: credential_status=null")
	}

	v, err := sqlscan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	}

	scanned := CredentialStatus(v)
	if err := scanned.Validate(); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	}

	*s = scanned

	return nil
}

type CredentialSignatureAlgorithm uint8

const (
	CredentialSignatureAlgorithmHMACSHA256 CredentialSignatureAlgorithm = iota + 1
	CredentialSignatureAlgorithmEd25519
)

// String convert credential signature algorithm to string.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (a CredentialSignatureAlgorithm) String() string {
	switch a {
	case CredentialSignatureAlgorithmHMACSHA256:
		return "hmac-sha256"
	case CredentialSignatureAlgorithmEd25519:
		return "ed25519"
	default:
		return ""
	}
}

// IsValid check whether credential algorithm is valid.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (a CredentialSignatureAlgorithm) IsValid() bool {
	switch a {
	case CredentialSignatureAlgorithmHMACSHA256, CredentialSignatureAlgorithmEd25519:
		return true
	default:
		return false
	}
}

// Validate credential algorithm.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialSignatureAlgorithm) Validate() error {
	if !s.IsValid() {
		return fmt.Errorf("failed to validate credential algorithm: algorithm=out_of_range")
	}
	return nil
}

// Value get credential algorithm as driver.Valuer.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialSignatureAlgorithm) Value() (driver.Value, error) {
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("failed to process api credential: %w", err)
	}

	return int64(s), nil
}

// Scan credential signature algorithm as sql.Scanner.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (a *CredentialSignatureAlgorithm) Scan(value any) error {
	if a == nil {
		return fmt.Errorf("failed to scan: missing required parameter: credential_signature_algorithm=null")
	}

	v, err := sqlscan.Uint8(value)
	if err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	}

	scanned := CredentialSignatureAlgorithm(v)
	if err := scanned.Validate(); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	}

	*a = scanned

	return nil
}

type CredentialScopes []string

// Validate credential scopes.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialScopes) Validate() error {
	if s == nil {
		return nil
	}

	seen := make(map[string]struct{}, len(s))

	for _, scope := range s {
		if scope == "" {
			return fmt.Errorf("failed to validate api credential: invalid parameter: scope=empty")
		}
		if len(scope) > 128 {
			return fmt.Errorf("failed to validate api credential: invalid parameter: scope=too_long max_length=128")
		}
		if _, ok := seen[scope]; ok {
			return fmt.Errorf("failed to validate api credential: scope=invalid")
		}

		seen[scope] = struct{}{}
	}

	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("failed to validate api credential: invalid parameter: %w", err)
	}
	if len(b) > 4096 {
		return fmt.Errorf("failed to validate api credential: invalid parameter: scopes=too_long max_size=4096")
	}

	return nil
}

// Value get credential scopes as driver.Valuer.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialScopes) Value() (driver.Value, error) {
	if s == nil {
		return []byte("[]"), nil
	}
	return json.Marshal([]string(s))
}

// Scan credential scopes as sql.Scanner.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s *CredentialScopes) Scan(src any) error {
	if s == nil {
		return fmt.Errorf("failed to validate api credential: invalid parameter: scopes=null")
	}

	if src == nil {
		*s = CredentialScopes{}
		return nil
	}

	var b []byte
	switch v := src.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("failed to validate api credential: invalid parameter: scopes: data_type=%T", src)
	}

	if len(b) == 0 || string(b) == "null" {
		*s = CredentialScopes{}
		return nil
	}

	var scopes []string
	if err := json.Unmarshal(b, &scopes); err != nil {
		return fmt.Errorf("failed to unmarshal scopes: %w", err)
	}

	if scopes == nil {
		scopes = []string{}
	}

	*s = CredentialScopes(scopes)
	return nil
}

// String get credential scopes as JSON string.
//
// Version:
//   - 2026-09-09: Migrate to storage and support revoked credentials.
func (s CredentialScopes) String() string {
	if s == nil {
		return "[]"
	}

	// A slice of strings has no unsupported values or custom marshalers.
	b, err := json.Marshal([]string(s))
	if err != nil {
		return "[]"
	}
	return string(b)
}
