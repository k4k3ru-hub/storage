//
// sql_identifier_test.go
//
package validator

import "testing"

func TestValidateSQLIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		wantErr    bool
	}{
		{name: "letters", identifier: "usage_credits"},
		{name: "digits after first character", identifier: "usage_credits_2"},
		{name: "empty", identifier: "", wantErr: true},
		{name: "starts with digit", identifier: "2_usage_credits", wantErr: true},
		{name: "qualified", identifier: "account.usage_credits", wantErr: true},
		{name: "SQL fragment", identifier: "usage_credits; DROP TABLE users", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSQLIdentifier(tt.identifier, "table_name")
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateSQLIdentifier(%q) error = %v, wantErr %v", tt.identifier, err, tt.wantErr)
			}
		})
	}
}
