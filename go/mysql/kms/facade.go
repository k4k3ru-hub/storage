//
// facade.go
//
package kms

import (
    "github.com/k4k3ru-hub/storage/go/mysql/kms/payload"
)


type PayloadSMTP = payload.SMTP


//
// Parse SMTP payload.
//
// Version:
//   - 2026-07-19: Added.
//
func ParseSMTPPayload(plaintext []byte) (*PayloadSMTP, error) {
    return payload.ParseSMTP(plaintext)
}
