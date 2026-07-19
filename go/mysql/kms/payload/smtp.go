//
// smtp.go
//
package payload

import (
    "encoding/json"
    "fmt"
)


//
// SMTP payload.
//
type SMTP struct {
    Host       string `json:"host,omitempty"`
    Port       int    `json:"port,omitempty"`
    Username   string `json:"username,omitempty"`
    Password   string `json:"password,omitempty"`
    AuthMethod string `json:"authMethod,omitempty"`
    Security   string `json:"security,omitempty"`
    FromName   string `json:"fromName,omitempty"`
    FromEmail  string `json:"fromEmail,omitempty"`
}


//
// Build plaintext.
//
// Version:
//   - 2026-07-19: Added.
//
func (s SMTP) BuildPlaintext() ([]byte, error) {
    plaintext, err := json.Marshal(s)
    if err != nil {
        return nil, fmt.Errorf("failed to build  payload plaintext: %w", err)
    }

    return plaintext, nil
}


//
// Parse SMTP payload.
//
// Version:
//   - 2026-07-19: Added.
//
func ParseSMTP(plaintext []byte) (*SMTP, error) {
    if len(plaintext) == 0 {
        return nil, fmt.Errorf("failed to parse SMTP payload: plaintext=empty")
    }

    var payload SMTP
    if err := json.Unmarshal(plaintext, &payload); err != nil {
        return nil, fmt.Errorf(
            "failed to parse SMTP payload: %w",
            err,
        )
    }

    return &payload, nil
}
