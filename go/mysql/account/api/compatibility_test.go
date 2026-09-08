package api

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestReadExistingCredentialLayout(t *testing.T) {
	for _, algorithm := range []CredentialSignatureAlgorithm{CredentialSignatureAlgorithmHMACSHA256, CredentialSignatureAlgorithmEd25519} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		store, err := NewCredentialStore("crm_account_api_credentials", "crm_accounts")
		if err != nil {
			t.Fatal(err)
		}
		stamp := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
		rows := sqlmock.NewRows([]string{"id", "account_id", "status", "name", "api_key", "signature_algorithm", "public_key", "secret_provider_ref", "secret_ref", "scopes", "expires_at", "created_at", "updated_at"}).AddRow(9, 123, CredentialStatusActive, "existing", "example-key", algorithm, "example-public-key", "existing-provider", "existing-encrypted-reference", []byte(`["example:read"]`), stamp, stamp, stamp)
		mock.ExpectQuery("SELECT \\* FROM crm_account_api_credentials WHERE api_key = \\? LIMIT 1;").WithArgs("example-key").WillReturnRows(rows)
		credential, err := store.SelectByAPIKey(db, "example-key")
		if err != nil {
			t.Fatal(err)
		}
		if credential == nil || credential.ID != 9 || credential.AccountID != 123 || credential.SignatureAlgorithm == nil || *credential.SignatureAlgorithm != algorithm || credential.SecretRef == nil || *credential.SecretRef != "existing-encrypted-reference" || credential.SecretProviderRef == nil || *credential.SecretProviderRef != "existing-provider" || credential.PublicKey == nil || *credential.PublicKey != "example-public-key" || len(credential.Scopes) != 1 || credential.Scopes[0] != "example:read" || credential.ExpiresAt == nil || !credential.ExpiresAt.Equal(stamp) {
			t.Fatal("persisted credential layout or KMS reference changed")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		mock.ExpectClose()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
