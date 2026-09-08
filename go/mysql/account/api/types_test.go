package api

import (
	"encoding/json"
	"testing"
)

func TestCredentialStatusCompatibility(t *testing.T) {
	for i, name := range []string{"pending", "active", "expired", "suspended", "revoked"} {
		status := CredentialStatus(i)
		if status.String() != name || !status.IsValid() {
			t.Fatalf("changed state %d", i)
		}
		value, err := status.Value()
		if err != nil {
			t.Fatal(err)
		}
		var scanned CredentialStatus
		if err := scanned.Scan(value); err != nil {
			t.Fatal(err)
		}
		if scanned != status {
			t.Fatal("status changed during SQL roundtrip")
		}
	}
	for _, value := range []any{int64(5), int64(-1), "256"} {
		var status CredentialStatus
		if err := status.Scan(value); err == nil {
			t.Fatal("invalid SQL status accepted")
		}
	}
	if err := CredentialSignatureAlgorithm(0).Validate(); err == nil {
		t.Fatal("invalid algorithm accepted")
	}
}

func TestCredentialStoreTableNames(t *testing.T) {
	for _, table := range []string{"credentials; DROP TABLE accounts", "x.y", "x`y"} {
		if _, err := NewCredentialStore(table, "accounts"); err == nil {
			t.Fatal("unsafe table accepted")
		}
	}
	if _, err := NewCredentialStore("crm_account_api_credentials", "crm_accounts"); err != nil {
		t.Fatal(err)
	}
}

func TestScopesJSONCompatibility(t *testing.T) {
	scopes := CredentialScopes{"a\"b", "日本語", "<read>"}
	var decoded []string
	if err := json.Unmarshal([]byte(scopes.String()), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(scopes) {
		t.Fatal("scope count changed")
	}
	for i := range decoded {
		if decoded[i] != scopes[i] {
			t.Fatal("scope changed")
		}
	}
	if (CredentialScopes(nil)).String() != "[]" {
		t.Fatal("nil representation changed")
	}
}
