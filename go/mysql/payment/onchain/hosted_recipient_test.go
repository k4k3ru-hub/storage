//
// hosted_recipient_test.go
//
package onchain

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

type hostedRecipientExecutorStub struct {
	query string
	args  []any
}

func (e *hostedRecipientExecutorStub) Exec(query string, args ...any) (sql.Result, error) {
	return e.ExecContext(context.Background(), query, args...)
}

func (e *hostedRecipientExecutorStub) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.query = query
	e.args = args
	return nil, nil
}

func (*hostedRecipientExecutorStub) Query(string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected Query call")
}

func (*hostedRecipientExecutorStub) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}

func (*hostedRecipientExecutorStub) QueryRow(string, ...any) *sql.Row { return &sql.Row{} }

func (*hostedRecipientExecutorStub) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return &sql.Row{}
}

func TestHostedRecipientCreateTableConstraints(t *testing.T) {
	store, err := NewHostedRecipientStore(DefaultHostedRecipientTableName)
	if err != nil {
		t.Fatal(err)
	}
	executor := &hostedRecipientExecutorStub{}
	if err := store.CreateTable(context.Background(), executor); err != nil {
		t.Fatal(err)
	}

	checks := []string{
		"UNIQUE KEY uq_hosted_recipient_account_chain (account_id, chain_family)",
		"UNIQUE KEY uq_hosted_recipient_chain_address (chain_family, address)",
		"encrypted_private_key TEXT NOT NULL",
	}
	for _, check := range checks {
		if !strings.Contains(executor.query, check) {
			t.Fatalf("CREATE TABLE query does not contain %q: %s", check, executor.query)
		}
	}
}

func TestHostedRecipientInsertRejectsNilParams(t *testing.T) {
	store, err := NewHostedRecipientStore(DefaultHostedRecipientTableName)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Insert(context.Background(), &sql.DB{}, nil)
	if err == nil || !strings.Contains(err.Error(), "hosted_recipient_insert_params=null") {
		t.Fatalf("error = %v, want hosted_recipient_insert_params=null error", err)
	}
}

func TestHostedRecipientInsertParamsValidate(t *testing.T) {
	params := &HostedRecipientInsertParams{
		ID:                  1,
		AccountID:           2,
		Status:              HostedRecipientStatusActive,
		ChainFamily:         "evm",
		Address:             "0x1234",
		EncryptedPrivateKey: "ciphertext",
		SecretProviderRef:   "provider",
	}
	if err := params.Validate(); err != nil {
		t.Fatal(err)
	}

	params.AccountID = 0
	if err := params.Validate(); err == nil {
		t.Fatal("Validate accepted account_id=0")
	}
}

func TestHostedRecipientStoreRejectsUnsafeTableName(t *testing.T) {
	if _, err := NewHostedRecipientStore("recipients; DROP TABLE recipients"); err == nil {
		t.Fatal("NewHostedRecipientStore accepted an unsafe table name")
	}
}
