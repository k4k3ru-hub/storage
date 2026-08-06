//
// sql_identifier.go
//
package validator

import "fmt"

//
// ValidateSQLIdentifier validates an unqualified SQL identifier.
//
// Parameters:
//   - name: SQL identifier.
//   - parameterName: Parameter name used in validation errors.
//
// Returns:
//   - Validation error when the identifier is invalid.
//
// Version:
//   - 2026-08-06: Added.
//
func ValidateSQLIdentifier(name, parameterName string) error {
	if name == "" {
		return fmt.Errorf("invalid parameter: %s=empty", parameterName)
	}
	for i, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return fmt.Errorf("invalid parameter: %s=invalid_identifier", parameterName)
	}
	return nil
}
