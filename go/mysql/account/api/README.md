# MySQL API credential storage

Module: `github.com/k4k3ru-hub/storage/go/mysql/account/api`.

Migrated from `github.com/k4k3ru-hub/db/go/mysql/account/api` at
`a59bbf509285`, preserving the table layout, column order, existing status values,
signature algorithms, KMS references and constructor names. Existing tables and
credentials can be used directly; no DDL migration or key regeneration is needed.

Status values: pending=0, active=1, expired=2, suspended=3, revoked=4.
`Revoke(ctx, tx, accountID, credentialID)` locks only an owned row, updates its
status to revoked, and leaves its name, key, expiration and signing material intact.
Already revoked rows succeed. Missing and differently owned rows both return false.
The caller owns the transaction and must commit before reporting success.

This module uses storage's internal ID generator, integer scanner and SQL identifier
validator. It has no dependency on the legacy `db` modules. The compatibility
CRUD methods keep their existing signatures using this package's `Executor`.

Initialize using the existing table names:

```go
store, err := api.NewCredentialStore("crm_account_api_credentials", "crm_accounts")
if err != nil {
    return err
}
```

Deploy readers that recognize revoked=4 before permitting revocation writes.
The low-level compatibility `UpdateByID` remains available to trusted storage
callers; the service exposes no reactivation RPC. Status and expiry enforcement
belongs to the service authentication boundary.
