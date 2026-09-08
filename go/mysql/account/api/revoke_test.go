package api

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRevokeOwnedCredential(t *testing.T) {
	underlying := errors.New("database unavailable")
	for _, tc := range []struct {
		name              string
		status            CredentialStatus
		missing           bool
		readErr, writeErr error
	}{
		{name: "active", status: CredentialStatusActive},
		{name: "pending", status: CredentialStatusPending},
		{name: "expired", status: CredentialStatusExpired},
		{name: "suspended", status: CredentialStatusSuspended},
		{name: "already revoked", status: CredentialStatusRevoked},
		{name: "missing or other account", missing: true},
		{name: "read failure", readErr: underlying},
		{name: "write failure", status: CredentialStatusActive, writeErr: underlying},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				mock.ExpectClose()
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			store, err := NewCredentialStore("credentials", "accounts")
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectBegin()
			read := mock.ExpectQuery("SELECT status FROM credentials WHERE id = \\? AND account_id = \\? FOR UPDATE").WithArgs(uint64(9), uint64(123))
			switch {
			case tc.missing:
				read.WillReturnError(sql.ErrNoRows)
			case tc.readErr != nil:
				read.WillReturnError(tc.readErr)
			default:
				read.WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(tc.status))
			}
			if !tc.missing && tc.readErr == nil && tc.status != CredentialStatusRevoked {
				update := mock.ExpectExec("UPDATE credentials SET status = \\?, updated_at = CURRENT_TIMESTAMP WHERE id = \\? AND account_id = \\?").WithArgs(CredentialStatusRevoked, uint64(9), uint64(123))
				if tc.writeErr != nil {
					update.WillReturnError(tc.writeErr)
				} else {
					update.WillReturnResult(sqlmock.NewResult(0, 1))
				}
			}
			mock.ExpectRollback()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			found, err := store.Revoke(context.Background(), tx, 123, 9)
			if tc.readErr != nil || tc.writeErr != nil {
				if !errors.Is(err, underlying) {
					t.Fatal("error chain lost")
				}
			} else if err != nil || found == tc.missing {
				t.Fatalf("found=%v err=%v", found, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
